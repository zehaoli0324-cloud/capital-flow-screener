"""
capital_flow_screener_v5 — Web版 (Flask)
策略插件化 + 全局任务队列（先来后到，防止多用户互相干扰）

核心改动：
  1. 去掉 sys.stdout 全局替换（改为线程本地日志捕获，多线程互不干扰）
  2. 全局串行队列：全市场选股同一时刻只有一个在跑
  3. 轻任务（单股分析）最多同时跑2个
  4. 前端轮询时返回排队位置，用户知道自己在等
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
# 任务中心（线程安全）
# ══════════════════════════════════════════════════════════════

_tasks: dict = {}
_tasks_lock  = threading.Lock()

# 重任务队列（全市场选股）：串行，同时只跑1个
_heavy_queue: queue.Queue = queue.Queue()
# 轻任务队列（单股分析）：最多同时跑2个
_light_semaphore = threading.Semaphore(2)


def _new_task(kind: str) -> tuple:
    task_id = str(uuid.uuid4())
    task = {
        "kind":        kind,
        "status":      "queued",
        "log":         [],
        "result":      None,
        "excel":       None,
        "excel_ready": False,
        "actual_date": "",
        "queue_pos":   0,
        "created_at":  time.time(),
    }
    with _tasks_lock:
        _tasks[task_id] = task
    return task_id, task


def _task_log(task_id: str, msg: str):
    with _tasks_lock:
        t = _tasks.get(task_id)
        if t is not None:
            t["log"].append(msg)


def _update_queue_positions():
    items = list(_heavy_queue.queue)
    for pos, (tid, _fn, _args) in enumerate(items, start=1):
        with _tasks_lock:
            if tid in _tasks:
                _tasks[tid]["queue_pos"] = pos


# ══════════════════════════════════════════════════════════════
# 队列工作线程
# ══════════════════════════════════════════════════════════════

def _heavy_worker():
    """串行消费重任务队列，永久运行"""
    while True:
        task_id, fn, args = _heavy_queue.get()
        with _tasks_lock:
            if task_id in _tasks:
                _tasks[task_id]["status"]    = "running"
                _tasks[task_id]["queue_pos"] = 0
        _update_queue_positions()
        try:
            fn(task_id, *args)
        except Exception as e:
            _task_log(task_id, f"❌ 队列执行异常：{e}")
            with _tasks_lock:
                if task_id in _tasks:
                    _tasks[task_id]["status"] = "error"
        finally:
            _heavy_queue.task_done()


def _submit_heavy(task_id: str, fn, args: tuple):
    _heavy_queue.put((task_id, fn, args))
    _update_queue_positions()


def _submit_light(task_id: str, fn, args: tuple):
    def _wrapper():
        _light_semaphore.acquire()
        with _tasks_lock:
            if task_id in _tasks:
                _tasks[task_id]["status"]    = "running"
                _tasks[task_id]["queue_pos"] = 0
        try:
            fn(task_id, *args)
        finally:
            _light_semaphore.release()
    threading.Thread(target=_wrapper, daemon=True).start()


# 启动重任务工作线程
threading.Thread(target=_heavy_worker, daemon=True).start()


# ══════════════════════════════════════════════════════════════
# 日志捕获：线程本地，不污染全局 sys.stdout
# ══════════════════════════════════════════════════════════════

class _PrintCapture:
    """
    上下文管理器：在 with 块内 print() 的输出同时写到任务日志和真实 stdout。
    使用线程本地存储，不同线程互不干扰，彻底解决多用户冲突问题。
    """
    _local = threading.local()

    def __init__(self, task_id: str):
        self._task_id = task_id
        self._prev_stdout = None
        self._buf = ""

    def __enter__(self):
        self._prev_stdout = sys.stdout
        _PrintCapture._local.active = self
        sys.stdout = _PatchedStdout(self)
        return self

    def __exit__(self, *_):
        sys.stdout = self._prev_stdout
        _PrintCapture._local.active = None

    def feed(self, s: str):
        if self._prev_stdout:
            self._prev_stdout.write(s)
            self._prev_stdout.flush()
        self._buf += s
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            line = line.strip()
            if line:
                _task_log(self._task_id, line)


class _PatchedStdout(io.TextIOBase):
    def __init__(self, capture):
        self._cap = capture

    def write(self, s):
        self._cap.feed(s)
        return len(s)

    def flush(self):
        if self._cap._prev_stdout:
            self._cap._prev_stdout.flush()


def _log(task_id: str, msg: str):
    _task_log(task_id, msg)


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
        _task_log(task_id, f"📁 Excel 已生成（{len(data)//1024} KB），可下载")
    except Exception as e:
        import traceback
        tb = traceback.format_exc().strip().split("\n")[-1]
        _task_log(task_id, f"❌ Excel 生成失败：{e}  |  {tb}")
        with _tasks_lock:
            if task_id in _tasks:
                _tasks[task_id]["excel_ready"] = False


# ══════════════════════════════════════════════════════════════
# 通用序列化
# ══════════════════════════════════════════════════════════════

def _safe(v):
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)): return None
    if isinstance(v, np.integer):  return int(v)
    if isinstance(v, np.floating): return float(v)
    if isinstance(v, pd.DataFrame): return None
    if isinstance(v, list): return [_safe(i) for i in v]
    return v


# ══════════════════════════════════════════════════════════════
# 全市场选股（重任务）
# ══════════════════════════════════════════════════════════════

def _run_screener(task_id: str, token: str, date_str: str, proxy: str, strategy_id: str):
    with _PrintCapture(task_id):
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
                    "result": result_list, "status": "done",
                    "actual_date": actual_date, "excel_ready": False
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
    with _PrintCapture(task_id):
        def log(msg): _log(task_id, msg)
        try:
            import tushare as ts
            import re
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

            log("📖 获取股票基本信息...")
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
                    "result": result_list, "status": "done",
                    "actual_date": actual_date, "excel_ready": False
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

    task_id, _ = _new_task("heavy")
    pos = _heavy_queue.qsize() + 1
    if pos == 1:
        _task_log(task_id, "⏳ 即将开始，排队第1位...")
    else:
        _task_log(task_id, f"⏳ 排队等待中（你是第{pos}位，前面还有{pos-1}个任务）...")

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

    task_id, _ = _new_task("light")
    available = _light_semaphore._value
    if available > 0:
        _task_log(task_id, "⏳ 即将开始分析...")
    else:
        _task_log(task_id, "⏳ 排队等待中（当前并发已满，稍候自动开始）...")

    _submit_light(task_id, _run_single_analysis,
                  (token, codes, data.get("date", ""), data.get("proxy", "")))
    return jsonify({"task_id": task_id})


@app.route("/api/status/<task_id>")
def api_status(task_id):
    with _tasks_lock:
        task = _tasks.get(task_id)
    if not task:
        return jsonify({"error": "任务不存在"}), 404
    return jsonify({
        "status":     task["status"],
        "queue_pos":  task.get("queue_pos", 0),
        "log":        task["log"],
        "has_result": task["result"] is not None,
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
