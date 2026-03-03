# tools/build_dashboard.py
import os, json, glob, datetime
from collections import defaultdict

ROOT = "history"  # results-history ブランチ上の履歴ルート
OUT_DIR = "site"  # 生成物
os.makedirs(OUT_DIR, exist_ok=True)

def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def list_latest_signals():
    # history/YYYY-MM-DD/{morning|close}/latest/signals/signals_YYYY-MM-DD_{session}.json
    cand = []
    for p in glob.glob(f"{ROOT}/*/*/latest/signals/signals_*.json"):
        # p: history/2026-03-03/morning/latest/signals/signals_2026-03-03_morning.json
        parts = p.split(os.sep)
        if len(parts) < 6:
            continue
        date = parts[1]
        session = parts[2]
        try:
            data = read_json(p)
            for c in data.get("candidates", []):
                cand.append((date, session, c, p))
        except Exception:
            pass

    # 最新日付→close優先→score降順
    def session_rank(s): return 1 if s == "close" else 0
    cand.sort(key=lambda x: (x[0], session_rank(x[1]), float(x[2].get("score", 0))), reverse=True)

    # 最新日付の morning/close をそれぞれ最大1ファイルずつ（latest）
    latest_by = {}
    for date, session, c, p in cand:
        key = (date, session)
        if key not in latest_by:
            latest_by[key] = {"path": p, "data": read_json(p)}
    return latest_by  # {(date,session): {...}}

def list_latest_results_per_trade():
    # history/YYYY-MM-DD/{session}/latest/results/result_*.json を探して
    # (date,session,ticker)ごとに最新の result を採用
    rows = {}
    for p in glob.glob(f"{ROOT}/*/*/latest/results/result_*.json"):
        parts = p.split(os.sep)
        if len(parts) < 6:
            continue
        date = parts[1]
        session = parts[2]
        try:
            data = read_json(p)
        except Exception:
            continue
        for t in data.get("results", []):
            ticker = t.get("ticker")
            if not ticker:
                continue
            key = (date, session, ticker)
            rows[key] = {
                "date": date,
                "session": session,
                "ticker": ticker,
                "entry": t.get("entry"),
                "sl": t.get("sl"),
                "tp": t.get("tp"),
                "status": t.get("result"),
            }
    return list(rows.values())

def compute_summary(rows):
    total = len(rows)
    win = sum(1 for r in rows if r["status"] == "WIN")
    loss = sum(1 for r in rows if r["status"] == "LOSS")
    open_ = sum(1 for r in rows if r["status"] == "OPEN")
    nf = sum(1 for r in rows if r["status"] == "NOT_FILLED")
    resolved = win + loss
    win_rate = (win / resolved * 100.0) if resolved else 0.0
    return {
        "total": total,
        "resolved": resolved,
        "win": win,
        "loss": loss,
        "open": open_,
        "not_filled": nf,
        "win_rate_resolved": win_rate,
    }

def html_escape(s):
    return (str(s)
            .replace("&","&amp;")
            .replace("<","&lt;")
            .replace(">","&gt;")
            .replace('"',"&quot;")
            .replace("'","&#39;"))

def build():
    latest_signals = list_latest_signals()
    results_rows = list_latest_results_per_trade()
    summary = compute_summary(results_rows)

    # OPENだけ抽出（今見るべき候補：過去に入っていたら保有中）
    open_rows = [r for r in results_rows if r["status"] == "OPEN"]
    # 日付降順
    open_rows.sort(key=lambda r: (r["date"], r["session"], r["ticker"]), reverse=True)

    # 結果一覧（日付降順）
    results_rows.sort(key=lambda r: (r["date"], r["session"], r["status"], r["ticker"]), reverse=True)

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def row_tr(r):
        return (
            "<tr>"
            f"<td>{html_escape(r['date'])}</td>"
            f"<td>{html_escape(r['session'])}</td>"
            f"<td><b>{html_escape(r['ticker'])}</b></td>"
            f"<td>{html_escape(r.get('entry',''))}</td>"
            f"<td>{html_escape(r.get('sl',''))}</td>"
            f"<td>{html_escape(r.get('tp',''))}</td>"
            f"<td>{html_escape(r.get('status',''))}</td>"
            "</tr>"
        )

    # 最新シグナル表（morning/close）
    latest_sig_blocks = []
    for (date, session), obj in sorted(latest_signals.items(), key=lambda x: (x[0][0], x[0][1]), reverse=True):
        data = obj["data"]
        cands = data.get("candidates", [])
        if not cands:
            continue
        rows_html = []
        for c in cands:
            rows_html.append(
                "<tr>"
                f"<td><b>{html_escape(c.get('ticker'))}</b></td>"
                f"<td>{html_escape(c.get('entry'))}</td>"
                f"<td>{html_escape(c.get('sl'))}</td>"
                f"<td>{html_escape(c.get('tp'))}</td>"
                f"<td>{html_escape(c.get('shares'))}</td>"
                f"<td>{html_escape(c.get('notional_yen',''))}</td>"
                f"<td>{html_escape(c.get('score',''))}</td>"
                "</tr>"
            )
        latest_sig_blocks.append(f"""
        <h3>Latest Signals: {html_escape(date)} / {html_escape(session)}</h3>
        <table>
          <thead><tr><th>Ticker</th><th>Entry</th><th>SL</th><th>TP</th><th>Shares</th><th>Notional</th><th>Score</th></tr></thead>
          <tbody>
            {''.join(rows_html)}
          </tbody>
        </table>
        """)

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Stock Dashboard</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, sans-serif; margin: 24px; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit,minmax(220px,1fr)); gap: 12px; }}
    .card {{ border: 1px solid #ddd; border-radius: 10px; padding: 12px; }}
    table {{ border-collapse: collapse; width: 100%; margin: 10px 0 24px; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; font-size: 14px; }}
    th {{ background: #f6f6f6; text-align: left; }}
    .muted {{ color: #666; font-size: 12px; }}
  </style>
</head>
<body>
  <h1>Stock Dashboard</h1>
  <div class="muted">Generated: {now} (from results-history)</div>

  <h2>Summary</h2>
  <div class="cards">
    <div class="card"><div class="muted">Total trades</div><div style="font-size:24px"><b>{summary['total']}</b></div></div>
    <div class="card"><div class="muted">Resolved</div><div style="font-size:24px"><b>{summary['resolved']}</b></div></div>
    <div class="card"><div class="muted">WIN / LOSS</div><div style="font-size:24px"><b>{summary['win']} / {summary['loss']}</b></div></div>
    <div class="card"><div class="muted">OPEN / NOT_FILLED</div><div style="font-size:24px"><b>{summary['open']} / {summary['not_filled']}</b></div></div>
    <div class="card"><div class="muted">WinRate (resolved)</div><div style="font-size:24px"><b>{summary['win_rate_resolved']:.2f}%</b></div></div>
  </div>

  <h2>Latest “Buy Candidates” (today)</h2>
  {''.join(latest_sig_blocks) if latest_sig_blocks else "<div>No latest signals found.</div>"}

  <h2>OPEN positions (if you had entered) — Watchlist</h2>
  <div class="muted">※ これは「過去にエントリーしていたら未決済」を示す一覧。今持ってないなら“買うリスト”ではなく“検証上の未決済”です。</div>
  <table>
    <thead><tr><th>Date</th><th>Session</th><th>Ticker</th><th>Entry</th><th>SL</th><th>TP</th><th>Status</th></tr></thead>
    <tbody>
      {''.join(row_tr(r) for r in open_rows) if open_rows else "<tr><td colspan='7'>(none)</td></tr>"}
    </tbody>
  </table>

  <h2>All results (latest per date/session/ticker)</h2>
  <table>
    <thead><tr><th>Date</th><th>Session</th><th>Ticker</th><th>Entry</th><th>SL</th><th>TP</th><th>Status</th></tr></thead>
    <tbody>
      {''.join(row_tr(r) for r in results_rows) if results_rows else "<tr><td colspan='7'>(none)</td></tr>"}
    </tbody>
  </table>
</body>
</html>
"""
    out = os.path.join(OUT_DIR, "index.html")
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    print("WRITE:", out)

if __name__ == "__main__":
    build()