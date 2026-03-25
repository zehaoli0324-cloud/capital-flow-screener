"""
capital_flow_screener_v4 — Web版 (Flask)
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

def _run_screener(task_id, token, date_str, proxy):
    def log(msg):
        with _tasks_lock:
            _tasks[task_id]["log"].append(msg)
    try:
        proxy_url = proxy.strip() if proxy and proxy.strip() else None
        screener_core._PROXY_URL = proxy_url
        log("🔧 代理：" + (proxy_url if proxy_url else "直连"))

        import tushare as ts
        log("🔑 验证 Tushare Token...")
        ts.set_token(token)
        pro = ts.pro_api()
        screener_core._pro = pro
        log("✅ Token OK")

        from datetime import datetime
        if not date_str or not date_str.strip():
            target_date = datetime.today().replace(hour=0,minute=0,second=0,microsecond=0)
        else:
            target_date = None
            for fmt in ("%Y%m%d", "%Y-%m-%d"):
                try:
                    target_date = datetime.strptime(date_str.strip(), fmt); break
                except ValueError:
                    pass
            if not target_date:
                target_date = datetime.today().replace(hour=0,minute=0,second=0,microsecond=0)

        is_today = (target_date.date() == datetime.today().date())
        log(f"📅 模式：{'今日实时' if is_today else '历史回测 '+target_date.strftime('%Y-%m-%d')}")

        log("📊 步骤1：全市场行情快照...")
        snapshot_df, actual_date = screener_core.get_spot_data(target_date)
        hs300_chg = screener_core.get_hs300_change(actual_date)
        log(f"✅ {len(snapshot_df)} 只股票，沪深300: {hs300_chg:+.2f}%")

        log("📈 步骤2：量价评分（并发拉K线，约需1-3分钟）...")
        all_candidates = screener_core.screen_stage1(snapshot_df, hs300_chg, actual_date, ff_workers=8)
        log(f"✅ 量价筛选 {len(all_candidates)} 只通过")

        if not all_candidates:
            log("❌ 今日无候选股")
            with _tasks_lock:
                _tasks[task_id]["status"] = "done"
                _tasks[task_id]["result"] = []
            return

        candidates = all_candidates[:screener_core.STAGE1_TOPN]
        log(f"🎯 前 {len(candidates)} 只进入资金流向验证：")
        for i, r in enumerate(candidates, 1):
            log(f"  {i:>2}. {r['name']}({r['code']})  涨幅{r['pct_chg']:+.2f}%  量价分{r['stage1_score']}/8")

        log("💰 步骤3：拉取资金流向（七级数据源）...")
        ff_results = screener_core.fetch_fund_flows(candidates, actual_date)
        ok_ff = sum(1 for v in ff_results.values() if v is not None)
        log(f"✅ 资金流向 {ok_ff}/{len(candidates)} 只成功")

        log("🧮 综合打分（18分项）...")
        combined = []
        for rec in candidates:
            code = rec["code"]
            sc = screener_core.score_fund_flow(code, ff_results.get(code), rec.get("_hist"), rec.get("circ_cap_yi"))
            combined.append({**rec, **sc})
        combined.sort(key=lambda x: (int(x.get("has_ff",False)), x["total"], x["stage1_score"]), reverse=True)

        def _safe(v):
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)): return None
            if isinstance(v, (np.integer,)): return int(v)
            if isinstance(v, (np.floating,)): return float(v)
            if isinstance(v, pd.DataFrame): return None
            if isinstance(v, list): return [_safe(i) for i in v]
            return v

        result_list = [{k: _safe(v) for k,v in r.items() if not k.startswith("_")} for r in combined]

        log("📁 生成 Excel 报告...")
        excel_buf = io.BytesIO()
        screener_core.save_excel(combined, excel_buf)

        log(f"🎉 完成！筛出 {len(result_list)} 只")
        with _tasks_lock:
            _tasks[task_id]["status"] = "done"
            _tasks[task_id]["result"] = result_list
            _tasks[task_id]["excel"]  = excel_buf.getvalue()
            _tasks[task_id]["actual_date"] = actual_date

    except Exception as e:
        import traceback
        log(f"❌ 出错：{e}")
        log(traceback.format_exc())
        with _tasks_lock:
            _tasks[task_id]["status"] = "error"

@app.route("/")
def index():
    return send_file("index.html")

@app.route("/api/run", methods=["POST"])
def api_run():
    data = request.json or {}
    token = (data.get("token") or "").strip()
    if not token:
        return jsonify({"error": "请填写 Tushare Token"}), 400
    task_id = str(uuid.uuid4())
    with _tasks_lock:
        _tasks[task_id] = {"status":"running","log":[],"result":None,"excel":None,"actual_date":""}
    threading.Thread(target=_run_screener, args=(task_id, token, data.get("date",""), data.get("proxy","")), daemon=True).start()
    return jsonify({"task_id": task_id})

@app.route("/api/status/<task_id>")
def api_status(task_id):
    with _tasks_lock:
        task = _tasks.get(task_id)
    if not task:
        return jsonify({"error": "任务不存在"}), 404
    return jsonify({"status": task["status"], "log": task["log"], "has_result": task["result"] is not None})

@app.route("/api/result/<task_id>")
def api_result(task_id):
    with _tasks_lock:
        task = _tasks.get(task_id)
    if not task or task["result"] is None:
        return jsonify({"error": "结果未就绪"}), 404
    return jsonify({"result": task["result"]})

@app.route("/api/excel/<task_id>")
def api_excel(task_id):
    with _tasks_lock:
        task = _tasks.get(task_id)
    if not task or task["excel"] is None:
        return jsonify({"error": "Excel 未就绪"}), 404
    return send_file(
        io.BytesIO(task["excel"]),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=f"capital_flow_v4_{task.get('actual_date','result')}.xlsx"
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
