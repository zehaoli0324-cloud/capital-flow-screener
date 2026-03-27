"""
capital_flow_screener_v4 — Web版 (Flask)
策略插件化版本：选股逻辑从 strategies/ 目录自动加载，
新增策略只需在该目录放一个 .py 文件，无需修改此文件。
"""
import os, sys, io, time, threading, uuid, pathlib, warnings, json
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

from flask import Flask, request, jsonify, send_file

app = Flask(__name__, static_folder=".", template_folder=".")

_tasks = {}
_tasks_lock = threading.Lock()

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import screener_core
from strategy_registry import list_strategies, get_strategy


# ── stdout 重定向 ────────────────────────────────────────────────
class _TaskLogger(io.TextIOBase):
    def __init__(self, task_id, orig_stdout):
        self._task_id = task_id
        self._orig    = orig_stdout
        self._buf     = ""

    def write(self, s):
        self._orig.write(s)
        self._orig.flush()
        self._buf += s
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            line = line.strip()
            if line:
                with _tasks_lock:
                    if self._task_id in _tasks:
                        _tasks[self._task_id]["log"].append(line)
        return len(s)

    def flush(self):
        self._orig.flush()


# ── Excel 异步生成 ───────────────────────────────────────────────
def _generate_excel_async(task_id, combined, actual_date):
    try:
        excel_buf = io.BytesIO()
        screener_core.save_excel(combined, excel_buf)
        with _tasks_lock:
            if task_id in _tasks:
                _tasks[task_id]["excel"]       = excel_buf.getvalue()
                _tasks[task_id]["excel_ready"] = True
                _tasks[task_id]["log"].append("📁 Excel 报告已生成，可下载")
    except Exception as e:
        with _tasks_lock:
            if task_id in _tasks:
                _tasks[task_id]["log"].append(f"❌ Excel 生成失败: {e}")
                _tasks[task_id]["excel_ready"] = False


# ── 通用结果序列化 ───────────────────────────────────────────────
def _safe(v):
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)): return None
    if isinstance(v, (np.integer,)): return int(v)
    if isinstance(v, (np.floating,)): return float(v)
    if isinstance(v, pd.DataFrame): return None
    if isinstance(v, list): return [_safe(i) for i in v]
    return v


# ══════════════════════════════════════════════════════════════
# 全市场选股（策略插件化）
# ══════════════════════════════════════════════════════════════

def _run_screener(task_id, token, date_str, proxy, strategy_id):
    orig_stdout = sys.stdout
    logger = _TaskLogger(task_id, orig_stdout)
    sys.stdout = logger

    def log(msg):
        with _tasks_lock:
            _tasks[task_id]["log"].append(msg)

    try:
        # 代理 & Token
        proxy_url = proxy.strip() if proxy and proxy.strip() else None
        screener_core._PROXY_URL = proxy_url
        log("🔧 代理：" + (proxy_url if proxy_url else "直连"))

        import tushare as ts
        log("🔑 验证 Tushare Token...")
        ts.set_token(token)
        screener_core._ts.set_token(token)
        pro = ts.pro_api()
        screener_core._pro = pro
        log("✅ Token OK")

        # 解析日期
        from datetime import datetime
        if not date_str or not date_str.strip():
            target_date = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            target_date = None
            for fmt in ("%Y%m%d", "%Y-%m-%d"):
                try:
                    target_date = datetime.strptime(date_str.strip(), fmt)
                    break
                except ValueError:
                    pass
            if not target_date:
                target_date = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

        is_today = (target_date.date() == datetime.today().date())
        log(f"📅 模式：{'今日实时' if is_today else '历史回测 ' + target_date.strftime('%Y-%m-%d')}")

        # 加载策略
        try:
            strategy = get_strategy(strategy_id)
            log(f"📋 策略：【{strategy.META['name']}】v{strategy.META.get('version','?')}")
        except KeyError as e:
            log(f"❌ {e}")
            with _tasks_lock:
                _tasks[task_id]["status"] = "error"
            return

        # 步骤1：全市场快照
        log("📊 步骤1：全市场行情快照...")
        snapshot_df, actual_date = screener_core.get_spot_data(target_date)
        hs300_chg = screener_core.get_hs300_change(actual_date)
        log(f"✅ {len(snapshot_df)} 只股票，沪深300: {hs300_chg:+.2f}%")

        # 步骤2：策略选股
        log(f"📈 步骤2：执行策略「{strategy.META['name']}」选股...")
        candidates = strategy.run(snapshot_df, hs300_chg, actual_date, log)

        if not candidates:
            log("❌ 策略未筛出候选股，分析结束")
            with _tasks_lock:
                _tasks[task_id]["status"]      = "done"
                _tasks[task_id]["result"]       = []
                _tasks[task_id]["actual_date"]  = actual_date
            return

        log(f"🎯 {len(candidates)} 只候选股进入资金流向验证：")
        for i, r in enumerate(candidates[:10], 1):
            log(f"  {i:>2}. {r['name']}({r['code']})  涨幅{r['pct_chg']:+.2f}%  "
                f"量价分{r.get('stage1_score',0)}")
        if len(candidates) > 10:
            log(f"  ... 另有 {len(candidates)-10} 只")

        # 步骤3：资金流向
        log("💰 步骤3：拉取资金流向（七级数据源）...")
        ff_results = screener_core.fetch_fund_flows(candidates, actual_date)
        ok_ff = sum(1 for v in ff_results.values() if v is not None)
        log(f"✅ 资金流向 {ok_ff}/{len(candidates)} 只成功")

        # 步骤4：综合评分
        log("🧮 综合打分（18分项）...")
        combined = []
        for rec in candidates:
            code = rec["code"]
            sc   = screener_core.score_fund_flow(
                code, ff_results.get(code), rec.get("_hist"), rec.get("circ_cap_yi")
            )
            combined.append({**rec, **sc})
        combined.sort(
            key=lambda x: (int(x.get("has_ff", False)), x["total"], x.get("stage1_score", 0)),
            reverse=True
        )

        result_list = [{k: _safe(v) for k, v in r.items() if not k.startswith("_")} for r in combined]

        with _tasks_lock:
            _tasks[task_id]["result"]      = result_list
            _tasks[task_id]["status"]      = "done"
            _tasks[task_id]["actual_date"] = actual_date
            _tasks[task_id]["excel_ready"] = False

        log(f"🎉 完成！筛出 {len(result_list)} 只，正在后台生成 Excel...")
        threading.Thread(
            target=_generate_excel_async,
            args=(task_id, combined, actual_date),
            daemon=True
        ).start()

    except Exception as e:
        import traceback
        log(f"❌ 出错：{e}")
        log(traceback.format_exc())
        with _tasks_lock:
            _tasks[task_id]["status"] = "error"
    finally:
        sys.stdout = orig_stdout


# ══════════════════════════════════════════════════════════════
# 单股资金流向分析
# ══════════════════════════════════════════════════════════════

def _run_single_analysis(task_id, token, codes, date_str, proxy):
    orig_stdout = sys.stdout
    logger = _TaskLogger(task_id, orig_stdout)
    sys.stdout = logger

    def log(msg):
        with _tasks_lock:
            _tasks[task_id]["log"].append(msg)

    try:
        proxy_url = proxy.strip() if proxy and proxy.strip() else None
        screener_core._PROXY_URL = proxy_url
        log("🔧 代理：" + (proxy_url if proxy_url else "直连"))

        import tushare as ts
        from datetime import datetime, timedelta
        log("🔑 验证 Tushare Token...")
        ts.set_token(token)
        screener_core._ts.set_token(token)
        pro = ts.pro_api()
        screener_core._pro = pro
        log("✅ Token OK")

        if not date_str or not date_str.strip():
            target_date = datetime.today()
        else:
            target_date = None
            for fmt in ("%Y%m%d", "%Y-%m-%d"):
                try:
                    target_date = datetime.strptime(date_str.strip(), fmt)
                    break
                except ValueError:
                    pass
            if not target_date:
                target_date = datetime.today()

        actual_date = target_date.strftime("%Y%m%d")
        log(f"📅 分析日期：{actual_date}")

        import re
        clean_codes = list(dict.fromkeys(
            m.group(1) for c in codes
            for m in [re.search(r'\b(\d{6})\b', c.strip())] if m
        ))

        if not clean_codes:
            log("❌ 未识别到有效股票代码（需6位数字）")
            with _tasks_lock:
                _tasks[task_id]["status"] = "done"
                _tasks[task_id]["result"] = []
            return

        log(f"📋 待分析股票：{len(clean_codes)} 只 → {', '.join(clean_codes)}")

        # 获取名称
        log("📖 获取股票基本信息...")
        name_map = {}
        try:
            sb = pro.stock_basic(fields="ts_code,name")
            if sb is not None and len(sb) > 0:
                for _, row in sb.iterrows():
                    name_map[row["ts_code"].split(".")[0]] = row["name"]
        except Exception as e:
            log(f"⚠️ 获取股票名称失败（非致命）：{e}")

        # 行情快照
        log("📊 获取行情快照...")
        price_map = {}; pct_map = {}; turnover_map = {}; volratio_map = {}; circ_map = {}
        try:
            daily_df = pro.daily(trade_date=actual_date,
                                  fields="ts_code,close,pct_chg,turnover_rate,vol")
            attempts = 0
            while (daily_df is None or len(daily_df) < 10) and attempts < 10:
                actual_date = (datetime.strptime(actual_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
                daily_df = pro.daily(trade_date=actual_date,
                                      fields="ts_code,close,pct_chg,turnover_rate,vol")
                attempts += 1
            log(f"✅ 行情日期确认：{actual_date}")
            if daily_df is not None and len(daily_df) > 0:
                basic_df = pro.daily_basic(trade_date=actual_date, fields="ts_code,volume_ratio,circ_mv")
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

        # 构建候选列表 + K线
        log(f"📈 拉取K线数据（{len(clean_codes)} 只）...")
        candidates = []
        for code in clean_codes:
            kdf = screener_core.fetch_kline(code, screener_core.KLINE_DAYS, actual_date)
            log(f"  K线 {code} {'✅' if kdf is not None else '❌'}")
            row_dict = {
                "pct_chg":   pct_map.get(code, 0.0),
                "vol_ratio": volratio_map.get(code, 0.0),
                "turnover":  turnover_map.get(code, 0.0),
            }
            try:
                hs300_chg = screener_core.get_hs300_change(actual_date)
                sc, hits = screener_core._score_one(row_dict, hs300_chg, kdf)
            except Exception:
                sc, hits = 0, []
            candidates.append({
                "code":        code,
                "name":        name_map.get(code, code),
                "price":       price_map.get(code, 0.0),
                "pct_chg":     pct_map.get(code, 0.0),
                "vol_ratio":   volratio_map.get(code, 0.0),
                "turnover":    turnover_map.get(code, 0.0),
                "circ_cap_yi": circ_map.get(code, 0.0),
                "stage1_score": sc,
                "stage1_hits":  hits,
                "_hist":        kdf,
            })

        # 资金流向
        log(f"💰 拉取资金流向（{len(candidates)} 只）...")
        ff_results = screener_core.fetch_fund_flows(candidates, actual_date)
        ok_ff = sum(1 for v in ff_results.values() if v is not None)
        log(f"✅ 资金流向 {ok_ff}/{len(candidates)} 只成功")

        # 评分
        log("🧮 综合打分（18分项）...")
        combined = []
        for rec in candidates:
            code = rec["code"]
            sc   = screener_core.score_fund_flow(
                code, ff_results.get(code), rec.get("_hist"), rec.get("circ_cap_yi")
            )
            combined.append({**rec, **sc})
        combined.sort(key=lambda x: x["total"], reverse=True)

        result_list = [{k: _safe(v) for k, v in r.items() if not k.startswith("_")} for r in combined]

        with _tasks_lock:
            _tasks[task_id]["result"]      = result_list
            _tasks[task_id]["status"]      = "done"
            _tasks[task_id]["actual_date"] = actual_date
            _tasks[task_id]["excel_ready"] = False

        log(f"🎉 完成！分析 {len(result_list)} 只股票，正在生成 Excel...")
        threading.Thread(
            target=_generate_excel_async,
            args=(task_id, combined, actual_date),
            daemon=True
        ).start()

    except Exception as e:
        import traceback
        log(f"❌ 出错：{e}")
        log(traceback.format_exc())
        with _tasks_lock:
            _tasks[task_id]["status"] = "error"
    finally:
        sys.stdout = orig_stdout


# ══════════════════════════════════════════════════════════════
# 路由
# ══════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return send_file("index.html")

@app.route("/api/strategies")
def api_strategies():
    """返回所有可用策略列表"""
    return jsonify({"strategies": list_strategies()})

@app.route("/api/run", methods=["POST"])
def api_run():
    data        = request.json or {}
    token       = (data.get("token") or "").strip()
    strategy_id = (data.get("strategy_id") or "capital_flow").strip()
    if not token:
        return jsonify({"error": "请填写 Tushare Token"}), 400
    task_id = str(uuid.uuid4())
    with _tasks_lock:
        _tasks[task_id] = {
            "status": "running", "log": [], "result": None,
            "excel": None, "excel_ready": False, "actual_date": ""
        }
    threading.Thread(
        target=_run_screener,
        args=(task_id, token, data.get("date", ""), data.get("proxy", ""), strategy_id),
        daemon=True
    ).start()
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
    task_id = str(uuid.uuid4())
    with _tasks_lock:
        _tasks[task_id] = {
            "status": "running", "log": [], "result": None,
            "excel": None, "excel_ready": False, "actual_date": ""
        }
    threading.Thread(
        target=_run_single_analysis,
        args=(task_id, token, codes, data.get("date", ""), data.get("proxy", "")),
        daemon=True
    ).start()
    return jsonify({"task_id": task_id})

@app.route("/api/status/<task_id>")
def api_status(task_id):
    with _tasks_lock:
        task = _tasks.get(task_id)
    if not task:
        return jsonify({"error": "任务不存在"}), 404
    return jsonify({
        "status":     task["status"],
        "log":        task["log"],
        "has_result": task["result"] is not None
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
        download_name=f"capital_flow_v4_{task.get('actual_date', 'result')}.xlsx"
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
