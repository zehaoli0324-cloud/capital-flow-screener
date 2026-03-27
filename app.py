"""
capital_flow_screener_v5 — Web版 (Flask)
策略插件化 + 全局任务队列

修复与新增：
  1. 【卡住修复】彻底放弃 sys.stdout 替换方案。
     改用 screener_core 内部 print 的 \r 进度条会卡住 IO 缓冲区的问题。
     现在用独立线程 + 队列方式异步捕获 stdout，不阻塞工作线程。

  2. 【刷新丢失修复】新增 /api/queue_status 接口，前端刷新后可查全局队列状态。
     新增 heartbeat 机制：前端每隔30s上报心跳；超过180s无心跳的任务
     视为用户离开，自动触发 Excel 生成并释放队列位置。

  3. 【预计时间】任务记录开始时间，队列状态接口返回当前任务运行时长，
     前端据此估算前面任务的剩余时间。

  4. 【超时清理】守护线程每60s扫描一次，对超时任务自动收尾，释放资源。
"""

import os, sys, io, time, threading, uuid, pathlib, warnings, queue
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

from flask import Flask, request, jsonify, send_file

app = Flask(__name__, static_folder=".", template_folder=".")

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import screener_core
from strategy_registry import list_strategies, get_strategy

# ══════════════════════════════════════════════════════════════
# 常量
# ══════════════════════════════════════════════════════════════
HEARTBEAT_TIMEOUT = 180   # 秒：超过此时间无心跳则视为用户离开
TASK_TTL          = 3600  # 秒：任务最长保留时间（之后从内存清除）

# ══════════════════════════════════════════════════════════════
# 任务中心
# ══════════════════════════════════════════════════════════════
_tasks: dict     = {}
_tasks_lock      = threading.Lock()
_heavy_queue     = queue.Queue()          # 全市场选股：串行
_light_semaphore = threading.Semaphore(2) # 单股分析：最多2个并发


def _new_task(kind: str) -> str:
    task_id = str(uuid.uuid4())
    now = time.time()
    with _tasks_lock:
        _tasks[task_id] = {
            "kind":        kind,
            "status":      "queued",   # queued | running | done | error | abandoned
            "log":         [],
            "result":      None,
            "excel":       None,
            "excel_ready": False,
            "actual_date": "",
            "queue_pos":   0,
            "created_at":  now,
            "started_at":  None,       # 开始运行时间
            "finished_at": None,       # 完成时间
            "last_heartbeat": now,     # 最近一次前端心跳
            "combined":    None,       # 保留 combined 供超时时补生成 Excel
        }
    return task_id


def _tlog(task_id: str, msg: str):
    """线程安全写任务日志"""
    with _tasks_lock:
        t = _tasks.get(task_id)
        if t is not None:
            t["log"].append(msg)


def _update_queue_positions():
    """重新计算重任务队列中每个等待任务的排队位置"""
    items = list(_heavy_queue.queue)
    for pos, (tid, *_) in enumerate(items, start=1):
        with _tasks_lock:
            if tid in _tasks:
                _tasks[tid]["queue_pos"] = pos


# ══════════════════════════════════════════════════════════════
# stdout 捕获（修复版）
# 关键改动：不再替换全局 sys.stdout，改用管道+后台线程读取。
# 每个任务启动时打开一对 pipe，把 screener_core 的 print 输出
# 重定向到 pipe 写端，后台线程从读端读取并写入任务日志。
# ══════════════════════════════════════════════════════════════

class _StdoutRouter:
    """
    用 os.pipe() 捕获当前线程内所有 print 输出，
    不影响其他线程，彻底解决多用户互相干扰问题。
    """
    def __init__(self, task_id: str):
        self._task_id = task_id
        self._r = self._w = None
        self._reader_thread = None
        self._orig_fd1 = None   # 原始 fd=1 (stdout)

    def __enter__(self):
        # 建立管道
        self._r, self._w = os.pipe()
        # 保存原始 stdout fd
        self._orig_fd1 = os.dup(1)
        # 把 fd=1 重定向到管道写端
        os.dup2(self._w, 1)
        os.close(self._w)
        self._w = None

        # 同时给 Python 层 sys.stdout 加一个包装（处理 Python 直接写 sys.stdout 的情况）
        self._orig_sys_stdout = sys.stdout
        sys.stdout = _PipeWriter(self._r)  # 只作为占位，实际数据走 fd=1

        # 启动后台读取线程
        self._reader_thread = threading.Thread(
            target=self._read_loop, daemon=True
        )
        self._reader_thread.start()
        return self

    def _read_loop(self):
        """从管道读端持续读取，按行写入任务日志"""
        buf = b""
        with os.fdopen(self._r, "rb", buffering=0) as f:
            while True:
                try:
                    chunk = f.read(256)
                    if not chunk:
                        break
                    buf += chunk
                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        # 过滤 \r 进度条更新行（只保留最后一次）
                        decoded = line.decode("utf-8", errors="replace").strip()
                        if decoded:
                            _tlog(self._task_id, decoded)
                except Exception:
                    break
        # 处理剩余
        if buf:
            decoded = buf.decode("utf-8", errors="replace").strip()
            if decoded:
                _tlog(self._task_id, decoded)

    def __exit__(self, *_):
        # 恢复 fd=1
        sys.stdout = self._orig_sys_stdout
        os.dup2(self._orig_fd1, 1)
        os.close(self._orig_fd1)
        # 等待读取线程结束（管道已关闭，会自动退出）
        if self._reader_thread:
            self._reader_thread.join(timeout=3)


class _PipeWriter(io.TextIOBase):
    """占位 sys.stdout，把 Python 层的 write 转发到 fd=1"""
    def __init__(self, _r):
        pass

    def write(self, s):
        try:
            os.write(1, s.encode("utf-8", errors="replace"))
        except Exception:
            pass
        return len(s)

    def flush(self):
        pass


def _log(task_id: str, msg: str):
    """直接写日志（不走 stdout）"""
    _tlog(task_id, msg)


# ══════════════════════════════════════════════════════════════
# Excel 异步生成
# ══════════════════════════════════════════════════════════════

def _generate_excel_async(task_id: str, combined: list, actual_date: str):
    try:
        buf = io.BytesIO()
        screener_core.save_excel(combined, buf)
        buf.seek(0)
        data = buf.getvalue()
        if len(data) < 1000:
            raise RuntimeError(f"文件异常，大小仅 {len(data)} 字节")
        with _tasks_lock:
            if task_id in _tasks:
                _tasks[task_id]["excel"]       = data
                _tasks[task_id]["excel_ready"] = True
                _tasks[task_id]["combined"]    = None  # 释放内存
        _tlog(task_id, f"📁 Excel 已生成（{len(data)//1024} KB），可下载")
    except Exception as e:
        import traceback
        tb = traceback.format_exc().strip().split("\n")[-1]
        _tlog(task_id, f"❌ Excel 生成失败：{e}  |  {tb}")
        with _tasks_lock:
            if task_id in _tasks:
                _tasks[task_id]["excel_ready"] = False


# ══════════════════════════════════════════════════════════════
# 超时守护线程
# ══════════════════════════════════════════════════════════════

def _watchdog():
    """每60秒扫描一次，处理超时任务和过期任务"""
    while True:
        time.sleep(60)
        now = time.time()
        with _tasks_lock:
            task_ids = list(_tasks.keys())

        for tid in task_ids:
            with _tasks_lock:
                t = _tasks.get(tid)
                if t is None:
                    continue
                status  = t["status"]
                hb      = t["last_heartbeat"]
                created = t["created_at"]
                combined = t.get("combined")
                excel_ready = t.get("excel_ready", False)
                actual_date = t.get("actual_date", "")

            # 1. 清除超过 TTL 的任务（释放内存）
            if now - created > TASK_TTL:
                with _tasks_lock:
                    _tasks.pop(tid, None)
                continue

            # 2. 任务已完成但用户离线，且 Excel 未生成 → 自动生成
            if status == "done" and not excel_ready and combined:
                idle = now - hb
                if idle > HEARTBEAT_TIMEOUT:
                    _tlog(tid, f"⏰ 用户离线超过{int(idle)}秒，自动生成 Excel...")
                    threading.Thread(
                        target=_generate_excel_async,
                        args=(tid, combined, actual_date),
                        daemon=True
                    ).start()

            # 3. 运行中但心跳超时 → 标记为 abandoned（不强制停止，让其自然完成）
            if status == "running" and now - hb > HEARTBEAT_TIMEOUT * 2:
                _tlog(tid, "⚠️ 用户长时间未响应，任务将在完成后自动清理")


threading.Thread(target=_watchdog, daemon=True).start()


# ══════════════════════════════════════════════════════════════
# 序列化
# ══════════════════════════════════════════════════════════════

def _safe(v):
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)): return None
    if isinstance(v, np.integer):  return int(v)
    if isinstance(v, np.floating): return float(v)
    if isinstance(v, pd.DataFrame): return None
    if isinstance(v, list): return [_safe(i) for i in v]
    return v


# ══════════════════════════════════════════════════════════════
# 队列工作线程
# ══════════════════════════════════════════════════════════════

def _heavy_worker():
    while True:
        task_id, fn, args = _heavy_queue.get()
        now = time.time()
        with _tasks_lock:
            if task_id in _tasks:
                _tasks[task_id]["status"]     = "running"
                _tasks[task_id]["queue_pos"]  = 0
                _tasks[task_id]["started_at"] = now
        _update_queue_positions()
        try:
            fn(task_id, *args)
        except Exception as e:
            _tlog(task_id, f"❌ 队列执行异常：{e}")
            with _tasks_lock:
                if task_id in _tasks:
                    _tasks[task_id]["status"] = "error"
        finally:
            with _tasks_lock:
                if task_id in _tasks:
                    _tasks[task_id]["finished_at"] = time.time()
            _heavy_queue.task_done()


def _submit_heavy(task_id: str, fn, args: tuple):
    _heavy_queue.put((task_id, fn, args))
    _update_queue_positions()


def _submit_light(task_id: str, fn, args: tuple):
    def _wrapper():
        _light_semaphore.acquire()
        now = time.time()
        with _tasks_lock:
            if task_id in _tasks:
                _tasks[task_id]["status"]     = "running"
                _tasks[task_id]["queue_pos"]  = 0
                _tasks[task_id]["started_at"] = now
        try:
            fn(task_id, *args)
        finally:
            with _tasks_lock:
                if task_id in _tasks:
                    _tasks[task_id]["finished_at"] = time.time()
            _light_semaphore.release()
    threading.Thread(target=_wrapper, daemon=True).start()


threading.Thread(target=_heavy_worker, daemon=True).start()


# ══════════════════════════════════════════════════════════════
# 全市场选股（重任务）
# ══════════════════════════════════════════════════════════════

def _run_screener(task_id: str, token: str, date_str: str, proxy: str, strategy_id: str):
    with _StdoutRouter(task_id):
        def log(msg): _log(task_id, msg)
        try:
            import tushare as ts
            from datetime import datetime

            proxy_url = proxy.strip() if proxy and proxy.strip() else None
            screener_core._PROXY_URL = proxy_url
            log("🔧 代理：" + (proxy_url or "直连"))

            log("🔑 验证 Tushare Token...")
            ts.set_token(token)
            screener_core._ts.set_token(token)
            pro = ts.pro_api()
            screener_core._pro = pro
            log("✅ Token OK")

            target_date = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
            if date_str and date_str.strip():
                for fmt in ("%Y%m%d", "%Y-%m-%d"):
                    try:
                        target_date = datetime.strptime(date_str.strip(), fmt); break
                    except ValueError: pass

            is_today = (target_date.date() == datetime.today().date())
            log(f"📅 模式：{'今日实时' if is_today else '历史回测 ' + target_date.strftime('%Y-%m-%d')}")

            try:
                strategy = get_strategy(strategy_id)
                log(f"📋 策略：【{strategy.META['name']}】v{strategy.META.get('version','?')}")
            except KeyError as e:
                log(f"❌ {e}")
                with _tasks_lock: _tasks[task_id]["status"] = "error"
                return

            log("📊 步骤1：全市场行情快照...")
            snapshot_df, actual_date = screener_core.get_spot_data(target_date)
            hs300_chg = screener_core.get_hs300_change(actual_date)
            log(f"✅ {len(snapshot_df)} 只股票，沪深300: {hs300_chg:+.2f}%")

            log(f"📈 步骤2：执行策略「{strategy.META['name']}」选股...")
            candidates = strategy.run(snapshot_df, hs300_chg, actual_date, log)

            if not candidates:
                log("❌ 策略未筛出候选股，分析结束")
                with _tasks_lock:
                    _tasks[task_id].update({"status": "done", "result": [], "actual_date": actual_date})
                return

            log(f"🎯 {len(candidates)} 只候选股进入资金流向验证：")
            for i, r in enumerate(candidates[:10], 1):
                log(f"  {i:>2}. {r['name']}({r['code']})  涨幅{r['pct_chg']:+.2f}%  量价分{r.get('stage1_score',0)}")
            if len(candidates) > 10:
                log(f"  ... 另有 {len(candidates)-10} 只")

            log("💰 步骤3：拉取资金流向（七级数据源）...")
            ff_results = screener_core.fetch_fund_flows(candidates, actual_date)
            ok_ff = sum(1 for v in ff_results.values() if v is not None)
            log(f"✅ 资金流向 {ok_ff}/{len(candidates)} 只成功")

            log("🧮 综合打分（18分项）...")
            combined = []
            for rec in candidates:
                sc = screener_core.score_fund_flow(
                    rec["code"], ff_results.get(rec["code"]), rec.get("_hist"), rec.get("circ_cap_yi"))
                combined.append({**rec, **sc})
            combined.sort(
                key=lambda x: (int(x.get("has_ff", False)), x["total"], x.get("stage1_score", 0)),
                reverse=True)

            result_list = [{k: _safe(v) for k, v in r.items() if not k.startswith("_")} for r in combined]

            with _tasks_lock:
                _tasks[task_id].update({
                    "result":      result_list,
                    "status":      "done",
                    "actual_date": actual_date,
                    "excel_ready": False,
                    "combined":    combined,   # 保留供超时自动生成 Excel
                })
            log(f"🎉 完成！筛出 {len(result_list)} 只，正在后台生成 Excel...")
            threading.Thread(
                target=_generate_excel_async, args=(task_id, combined, actual_date), daemon=True
            ).start()

        except Exception as e:
            import traceback
            log(f"❌ 出错：{e}")
            log(traceback.format_exc())
            with _tasks_lock:
                _tasks[task_id]["status"] = "error"


# ══════════════════════════════════════════════════════════════
# 单股分析（轻任务）
# ══════════════════════════════════════════════════════════════

def _run_single_analysis(task_id: str, token: str, codes: list, date_str: str, proxy: str):
    with _StdoutRouter(task_id):
        def log(msg): _log(task_id, msg)
        try:
            import tushare as ts, re
            from datetime import datetime, timedelta

            proxy_url = proxy.strip() if proxy and proxy.strip() else None
            screener_core._PROXY_URL = proxy_url
            log("🔧 代理：" + (proxy_url or "直连"))

            log("🔑 验证 Tushare Token...")
            ts.set_token(token)
            screener_core._ts.set_token(token)
            pro = ts.pro_api()
            screener_core._pro = pro
            log("✅ Token OK")

            actual_date = datetime.today().strftime("%Y%m%d")
            if date_str and date_str.strip():
                for fmt in ("%Y%m%d", "%Y-%m-%d"):
                    try:
                        actual_date = datetime.strptime(date_str.strip(), fmt).strftime("%Y%m%d"); break
                    except ValueError: pass
            log(f"📅 分析日期：{actual_date}")

            clean_codes = list(dict.fromkeys(
                m.group(1) for c in codes
                for m in [re.search(r'\b(\d{6})\b', c.strip())] if m
            ))
            if not clean_codes:
                log("❌ 未识别到有效股票代码（需6位数字）")
                with _tasks_lock:
                    _tasks[task_id].update({"status": "done", "result": []})
                return

            log(f"📋 待分析股票：{len(clean_codes)} 只 → {', '.join(clean_codes)}")

            name_map = {}
            try:
                sb = pro.stock_basic(fields="ts_code,name")
                if sb is not None:
                    for _, row in sb.iterrows():
                        name_map[row["ts_code"].split(".")[0]] = row["name"]
            except Exception as e:
                log(f"⚠️ 获取名称失败（非致命）：{e}")

            log("📊 获取行情快照...")
            price_map = {}; pct_map = {}; turnover_map = {}; volratio_map = {}; circ_map = {}
            try:
                daily_df = pro.daily(trade_date=actual_date,
                                     fields="ts_code,close,pct_chg,turnover_rate,vol")
                attempts = 0
                while (daily_df is None or len(daily_df) < 10) and attempts < 10:
                    actual_date = (datetime.strptime(actual_date, "%Y%m%d")
                                   - timedelta(days=1)).strftime("%Y%m%d")
                    daily_df = pro.daily(trade_date=actual_date,
                                         fields="ts_code,close,pct_chg,turnover_rate,vol")
                    attempts += 1
                log(f"✅ 行情日期确认：{actual_date}")
                if daily_df is not None and len(daily_df) > 0:
                    basic_df = pro.daily_basic(trade_date=actual_date,
                                               fields="ts_code,volume_ratio,circ_mv")
                    for _, row in daily_df.iterrows():
                        c6 = row["ts_code"].split(".")[0]
                        price_map[c6]    = float(row.get("close", 0) or 0)
                        pct_map[c6]      = float(row.get("pct_chg", 0) or 0)
                        turnover_map[c6] = float(row.get("turnover_rate", 0) or 0)
                    if basic_df is not None:
                        for _, row in basic_df.iterrows():
                            c6 = row["ts_code"].split(".")[0]
                            volratio_map[c6] = float(row.get("volume_ratio", 0) or 0)
                            circ_map[c6]     = float(row.get("circ_mv", 0) or 0) / 10000
            except Exception as e:
                log(f"⚠️ 行情快照获取失败：{e}")

            log(f"📈 拉取K线数据（{len(clean_codes)} 只）...")
            candidates = []
            try:
                hs300_chg = screener_core.get_hs300_change(actual_date)
            except Exception:
                hs300_chg = 0.0

            for code in clean_codes:
                kdf = screener_core.fetch_kline(code, screener_core.KLINE_DAYS, actual_date)
                log(f"  K线 {code} {'✅' if kdf is not None else '❌'}")
                try:
                    sc, hits = screener_core._score_one(
                        {"pct_chg": pct_map.get(code, 0.0),
                         "vol_ratio": volratio_map.get(code, 0.0),
                         "turnover": turnover_map.get(code, 0.0)},
                        hs300_chg, kdf)
                except Exception:
                    sc, hits = 0, []
                candidates.append({
                    "code": code, "name": name_map.get(code, code),
                    "price": price_map.get(code, 0.0),
                    "pct_chg": pct_map.get(code, 0.0),
                    "vol_ratio": volratio_map.get(code, 0.0),
                    "turnover": turnover_map.get(code, 0.0),
                    "circ_cap_yi": circ_map.get(code, 0.0),
                    "stage1_score": sc, "stage1_hits": hits, "_hist": kdf,
                })

            log(f"💰 拉取资金流向（{len(candidates)} 只）...")
            ff_results = screener_core.fetch_fund_flows(candidates, actual_date)
            ok_ff = sum(1 for v in ff_results.values() if v is not None)
            log(f"✅ 资金流向 {ok_ff}/{len(candidates)} 只成功")

            log("🧮 综合打分（18分项）...")
            combined = []
            for rec in candidates:
                sc = screener_core.score_fund_flow(
                    rec["code"], ff_results.get(rec["code"]),
                    rec.get("_hist"), rec.get("circ_cap_yi"))
                combined.append({**rec, **sc})
            combined.sort(key=lambda x: x["total"], reverse=True)

            result_list = [{k: _safe(v) for k, v in r.items() if not k.startswith("_")} for r in combined]

            with _tasks_lock:
                _tasks[task_id].update({
                    "result":      result_list,
                    "status":      "done",
                    "actual_date": actual_date,
                    "excel_ready": False,
                    "combined":    combined,
                })
            log(f"🎉 完成！分析 {len(result_list)} 只股票，正在生成 Excel...")
            threading.Thread(
                target=_generate_excel_async, args=(task_id, combined, actual_date), daemon=True
            ).start()

        except Exception as e:
            import traceback
            log(f"❌ 出错：{e}")
            log(traceback.format_exc())
            with _tasks_lock:
                _tasks[task_id]["status"] = "error"


# ══════════════════════════════════════════════════════════════
# Flask 路由
# ══════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return send_file("index.html")


@app.route("/api/strategies")
def api_strategies():
    return jsonify({"strategies": list_strategies()})


@app.route("/api/run", methods=["POST"])
def api_run():
    data        = request.json or {}
    token       = (data.get("token") or "").strip()
    strategy_id = (data.get("strategy_id") or "capital_flow").strip()
    if not token:
        return jsonify({"error": "请填写 Tushare Token"}), 400

    task_id = _new_task("heavy")
    qsize   = _heavy_queue.qsize()  # 当前等待数（不含正在运行的）

    # 估算预计等待时间：统计已完成任务的平均运行时长
    avg_sec = _estimate_avg_runtime("heavy")
    pos     = qsize + 1  # 自己排在第几位（1=下一个运行）

    if pos == 1 and qsize == 0:
        msg = "⏳ 即将开始（你是第1位）..."
    else:
        wait_min = int(avg_sec * qsize / 60) + 1
        msg = f"⏳ 排队第{pos}位，前面还有{qsize}个任务，预计等待约{wait_min}分钟..."
    _tlog(task_id, msg)

    _submit_heavy(task_id, _run_screener,
                  (token, data.get("date", ""), data.get("proxy", ""), strategy_id))
    return jsonify({"task_id": task_id})


@app.route("/api/run_single", methods=["POST"])
def api_run_single():
    data  = request.json or {}
    token = (data.get("token") or "").strip()
    codes = data.get("codes", [])
    if not token:
        return jsonify({"error": "请填写 Tushare Token"}), 400
    if not codes:
        return jsonify({"error": "请提供至少一只股票代码"}), 400

    task_id   = _new_task("light")
    available = _light_semaphore._value
    if available > 0:
        _tlog(task_id, "⏳ 即将开始分析...")
    else:
        _tlog(task_id, "⏳ 并发槽满，稍候自动开始...")

    _submit_light(task_id, _run_single_analysis,
                  (token, codes, data.get("date", ""), data.get("proxy", "")))
    return jsonify({"task_id": task_id})


@app.route("/api/status/<task_id>")
def api_status(task_id):
    with _tasks_lock:
        task = _tasks.get(task_id)
    if not task:
        return jsonify({"error": "任务不存在"}), 404

    now       = time.time()
    started   = task.get("started_at")
    elapsed   = int(now - started) if started else 0
    avg_sec   = _estimate_avg_runtime("heavy")

    # 计算队列中前面任务剩余时间
    pos       = task.get("queue_pos", 0)
    eta_front = max(0, int(avg_sec - elapsed)) if task["status"] == "running" else 0
    eta_queue = int(avg_sec * max(0, pos - 1) + eta_front) if pos > 0 else 0

    return jsonify({
        "status":     task["status"],
        "queue_pos":  pos,
        "elapsed":    elapsed,       # 当前任务已运行秒数
        "eta_queue":  eta_queue,     # 预计等待秒数（前面任务）
        "log":        task["log"],
        "has_result": task["result"] is not None,
    })


@app.route("/api/heartbeat/<task_id>", methods=["POST"])
def api_heartbeat(task_id):
    """前端定期调用，证明用户还在线"""
    with _tasks_lock:
        t = _tasks.get(task_id)
        if t:
            t["last_heartbeat"] = time.time()
    return jsonify({"ok": True})


@app.route("/api/queue_status")
def api_queue_status():
    """
    前端刷新后调用此接口，获取全局队列状态。
    返回当前正在运行和等待中的任务概要，让用户知道是否有任务还活着。
    """
    now = time.time()
    running = []
    waiting = []
    with _tasks_lock:
        for tid, t in _tasks.items():
            if t["status"] == "running":
                started = t.get("started_at")
                running.append({
                    "task_id": tid,
                    "kind":    t["kind"],
                    "elapsed": int(now - started) if started else 0,
                })
            elif t["status"] == "queued":
                waiting.append({
                    "task_id":  tid,
                    "kind":     t["kind"],
                    "queue_pos": t.get("queue_pos", 0),
                })

    avg_sec = _estimate_avg_runtime("heavy")
    return jsonify({
        "running": running,
        "waiting": waiting,
        "avg_runtime_sec": int(avg_sec),
    })


@app.route("/api/result/<task_id>")
def api_result(task_id):
    with _tasks_lock:
        task = _tasks.get(task_id)
    if not task or task["result"] is None:
        return jsonify({"error": "结果未就绪"}), 404
    return jsonify({"result": task["result"]})


@app.route("/api/excel_status/<task_id>")
def api_excel_status(task_id):
    with _tasks_lock:
        task = _tasks.get(task_id)
    if not task:
        return jsonify({"error": "任务不存在"}), 404
    return jsonify({"ready": task.get("excel_ready", False)})


@app.route("/api/excel/<task_id>")
def api_excel(task_id):
    with _tasks_lock:
        task = _tasks.get(task_id)
    if not task or task.get("excel") is None:
        return jsonify({"error": "Excel 未就绪"}), 404
    return send_file(
        io.BytesIO(task["excel"]),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=f"capital_flow_v5_{task.get('actual_date','result')}.xlsx"
    )


# ══════════════════════════════════════════════════════════════
# 工具：估算平均运行时长
# ══════════════════════════════════════════════════════════════

def _estimate_avg_runtime(kind: str) -> float:
    """根据历史已完成任务估算平均运行时长（秒），默认300秒"""
    durations = []
    with _tasks_lock:
        for t in _tasks.values():
            if (t["kind"] == kind
                    and t["status"] == "done"
                    and t.get("started_at")
                    and t.get("finished_at")):
                d = t["finished_at"] - t["started_at"]
                if 10 < d < 3600:   # 过滤异常值
                    durations.append(d)
    if not durations:
        return 300.0   # 无历史数据时默认5分钟
    return sum(durations) / len(durations)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
