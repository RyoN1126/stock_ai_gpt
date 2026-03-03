# tools/build_dashboard.py
import os
import re
import json
import glob
import datetime
from typing import Any, Dict, List, Tuple, Optional

# results-history ブランチからコピーしてきた history/ を読む想定
ROOT = "history"
OUT_DIR = "site"

RE_SIGNAL = re.compile(r"^signals_(\d{4}-\d{2}-\d{2})_(morning|close)\.json$")
RE_RESULT  = re.compile(r"^result_(\d{4}-\d{2}-\d{2})_(morning|close)_.+\.json$")

def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def html_escape(s: Any) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )

def parse_yyyy_mm_dd(s: str) -> Optional[datetime.date]:
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def status_class(status: str) -> str:
    s = (status or "").upper()
    if s == "WIN":
        return "st win"
    if s == "LOSS":
        return "st loss"
    if s == "OPEN":
        return "st open"
    if s == "NOT_FILLED":
        return "st nf"
    return "st"

def session_rank(session: str) -> int:
    # closeを先に見せたい
    return 1 if session == "close" else 0

def scan_latest_signals_by_filename() -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    フォルダ名に依存せず、ファイル名 signals_YYYY-MM-DD_session.json から
    (date, session) -> json を作る（latest配下優先）
    """
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}

    # まず latest/ 配下を拾う（なければ run-... 配下でも拾えるように）
    patterns = [
        os.path.join(ROOT, "**", "latest", "signals", "signals_*.json"),
        os.path.join(ROOT, "**", "signals", "signals_*.json"),
    ]
    files: List[str] = []
    for pat in patterns:
        files.extend(glob.glob(pat, recursive=True))

    # 同じ(date, session)はmtimeが新しい方を採用
    best_mtime: Dict[Tuple[str, str], float] = {}

    for p in files:
        base = os.path.basename(p)
        m = RE_SIGNAL.match(base)
        if not m:
            continue
        date, session = m.group(1), m.group(2)
        key = (date, session)
        try:
            mt = os.path.getmtime(p)
            if key in best_mtime and mt <= best_mtime[key]:
                continue
            out[key] = read_json(p)
            best_mtime[key] = mt
        except Exception:
            continue

    return out

def scan_latest_results_rows_by_filename() -> List[Dict[str, Any]]:
    """
    result_YYYY-MM-DD_session_...json のファイル名から「対象日付」を採用して rows を作る
    同一(date, session, ticker)は最新mtimeのファイルを採用
    """
    pattern = os.path.join(ROOT, "**", "latest", "results", "result_*.json")
    files = glob.glob(pattern, recursive=True)

    best: Dict[Tuple[str, str, str], Tuple[float, Dict[str, Any]]] = {}

    for p in files:
        base = os.path.basename(p)
        m = RE_RESULT.match(base)
        if not m:
            continue
        date, session = m.group(1), m.group(2)

        try:
            data = read_json(p)
            mt = os.path.getmtime(p)
        except Exception:
            continue

        for t in data.get("results", []):
            ticker = t.get("ticker")
            if not ticker:
                continue
            key = (date, session, ticker)
            row = {
                "date": date,
                "session": session,
                "ticker": ticker,
                "entry": t.get("entry"),
                "sl": t.get("sl"),
                "tp": t.get("tp"),
                "status": t.get("result"),
                "shares_used": t.get("shares_used", t.get("shares")),
                "notional_yen": t.get("notional_yen"),
            }
            if key not in best or mt > best[key][0]:
                best[key] = (mt, row)

    return [v[1] for v in best.values()]

def compute_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(rows)
    win = sum(1 for r in rows if r.get("status") == "WIN")
    loss = sum(1 for r in rows if r.get("status") == "LOSS")
    open_ = sum(1 for r in rows if r.get("status") == "OPEN")
    nf = sum(1 for r in rows if r.get("status") == "NOT_FILLED")
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

def latest_date_from_keys(keys: List[Tuple[str, str]]) -> Optional[str]:
    ds = []
    for d, _ in keys:
        dd = parse_yyyy_mm_dd(d)
        if dd:
            ds.append((dd, d))
    if not ds:
        return None
    ds.sort()
    return ds[-1][1]

def build():
    os.makedirs(OUT_DIR, exist_ok=True)

    # signals/results を「ファイル名日付」で再構築
    signals_map = scan_latest_signals_by_filename()
    results_rows = scan_latest_results_rows_by_filename()
    summary = compute_summary(results_rows)

    # 最新日（対象日）
    latest_date = latest_date_from_keys(list(signals_map.keys())) or latest_date_from_keys(
        [(r["date"], r["session"]) for r in results_rows]
    )

    # 最新日のsignals（morning/close）だけ表示
    latest_signals = []
    if latest_date:
        for sess in ["close", "morning"]:
            k = (latest_date, sess)
            if k in signals_map:
                latest_signals.append((latest_date, sess, signals_map[k]))

    # OPEN一覧
    open_rows = [r for r in results_rows if r.get("status") == "OPEN"]
    open_rows.sort(key=lambda r: (r["date"], session_rank(r["session"]), r["ticker"]), reverse=True)

    # 全件
    results_rows.sort(key=lambda r: (r["date"], session_rank(r["session"]), r["ticker"]), reverse=True)

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # カード（買い候補）HTML
    cards_html = ""
    if latest_signals:
        blocks = []
        for date, session, data in latest_signals:
            cands = data.get("candidates", [])
            if not cands:
                continue

            # score高い順
            cands = sorted(cands, key=lambda c: float(c.get("score", 0.0)), reverse=True)

            # 上位だけでもいいけど、今は全部表示
            card_items = []
            for c in cands:
                ticker = c.get("ticker", "")
                entry = c.get("entry", "")
                sl = c.get("sl", "")
                tp = c.get("tp", "")
                shares = c.get("shares", "")
                notional = c.get("notional_yen", "")
                score = c.get("score", "")
                rr = c.get("rr", "")
                risk_yen = c.get("risk_yen", "")

                card_items.append(f"""
                  <div class="cand-card">
                    <div class="cand-top">
                      <div class="cand-ticker">{html_escape(ticker)}</div>
                      <div class="cand-score">score {html_escape(score)}</div>
                    </div>
                    <div class="cand-grid">
                      <div><span class="k">Entry</span><span class="v">{html_escape(entry)}</span></div>
                      <div><span class="k">SL</span><span class="v">{html_escape(sl)}</span></div>
                      <div><span class="k">TP</span><span class="v">{html_escape(tp)}</span></div>
                      <div><span class="k">RR</span><span class="v">{html_escape(rr)}</span></div>
                      <div><span class="k">Shares</span><span class="v">{html_escape(shares)}</span></div>
                      <div><span class="k">Notional</span><span class="v">{html_escape(notional)}</span></div>
                      <div><span class="k">Risk(¥)</span><span class="v">{html_escape(risk_yen)}</span></div>
                    </div>
                    <div class="cand-note">注文目安：{html_escape(entry)} 付近（ルール通り）</div>
                  </div>
                """)

            blocks.append(f"""
              <div class="cand-block">
                <div class="cand-header">
                  <div class="cand-title">Latest Buy Candidates</div>
                  <div class="cand-sub">{html_escape(date)} / {html_escape(session)}</div>
                </div>
                <div class="cand-cards">
                  {''.join(card_items)}
                </div>
              </div>
            """)

        cards_html = "".join(blocks) if blocks else "<div class='muted'>No candidates found for latest date.</div>"
    else:
        cards_html = "<div class='muted'>No latest signals found.</div>"

    def row_tr(r: Dict[str, Any]) -> str:
        st = (r.get("status") or "")
        cls = status_class(st)
        return (
            f"<tr class='{cls}' "
            f"data-date='{html_escape(r['date'])}' "
            f"data-session='{html_escape(r['session'])}' "
            f"data-status='{html_escape(st)}' "
            f"data-ticker='{html_escape(r.get('ticker',''))}'>"
            f"<td>{html_escape(r['date'])}</td>"
            f"<td>{html_escape(r['session'])}</td>"
            f"<td><b>{html_escape(r['ticker'])}</b></td>"
            f"<td>{html_escape(r.get('entry',''))}</td>"
            f"<td>{html_escape(r.get('sl',''))}</td>"
            f"<td>{html_escape(r.get('tp',''))}</td>"
            f"<td><span class='pill {status_class(st)}'>{html_escape(st)}</span></td>"
            "</tr>"
        )

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Stock Dashboard</title>
  <style>
    body {{
      font-family: system-ui, -apple-system, Segoe UI, sans-serif;
      margin: 24px;
      background: #0b0f17;
      color: #e8eefc;
    }}
    a {{ color: #9ecbff; }}
    .muted {{ color: #a8b3cf; font-size: 12px; }}
    .section {{ margin-top: 20px; }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit,minmax(220px,1fr));
      gap: 12px;
      margin-bottom: 12px;
    }}
    .card {{
      border: 1px solid #1d2a4a;
      border-radius: 14px;
      padding: 12px;
      background: #0f172a;
      box-shadow: 0 6px 16px rgba(0,0,0,0.25);
    }}
    .big {{ font-size: 26px; font-weight: 800; }}
    .controls {{
      display: flex; flex-wrap: wrap; gap: 8px; align-items: center;
      margin: 10px 0 16px;
    }}
    .controls input, .controls select {{
      padding: 10px;
      border: 1px solid #1d2a4a;
      border-radius: 10px;
      background: #0f172a;
      color: #e8eefc;
      outline: none;
    }}
    .btn {{
      padding: 10px 12px;
      border: 1px solid #1d2a4a;
      border-radius: 10px;
      background: #0f172a;
      color: #e8eefc;
      cursor: pointer;
    }}
    .btn.active {{
      border-color: #9ecbff;
      box-shadow: 0 0 0 3px rgba(158,203,255,0.12);
    }}

    table {{ border-collapse: collapse; width: 100%; margin: 10px 0 24px; }}
    th, td {{
      border: 1px solid #1d2a4a;
      padding: 8px;
      font-size: 14px;
    }}
    th {{
      background: #0f172a;
      text-align: left;
      position: sticky; top: 0;
    }}
    tr:hover td {{ background: rgba(158,203,255,0.06); }}

    /* status colors */
    .pill {{
      display: inline-block;
      padding: 4px 10px;
      border-radius: 999px;
      font-weight: 700;
      font-size: 12px;
      border: 1px solid #1d2a4a;
    }}
    .st.win  {{ }}
    .st.loss {{ }}
    .st.open {{ }}
    .st.nf   {{ }}

    .pill.st.win  {{ background: rgba(34,197,94,0.16); color: #7dffb0; border-color: rgba(34,197,94,0.35); }}
    .pill.st.loss {{ background: rgba(239,68,68,0.16); color: #ff9a9a; border-color: rgba(239,68,68,0.35); }}
    .pill.st.open {{ background: rgba(59,130,246,0.16); color: #a8d0ff; border-color: rgba(59,130,246,0.35); }}
    .pill.st.nf   {{ background: rgba(148,163,184,0.14); color: #d2dae7; border-color: rgba(148,163,184,0.30); }}

    /* candidate spotlight */
    .spotlight {{
      border: 1px solid rgba(158,203,255,0.35);
      background: radial-gradient(1200px 400px at 30% 0%, rgba(158,203,255,0.12), transparent 60%), #0f172a;
      border-radius: 18px;
      padding: 14px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.35);
    }}
    .cand-header {{
      display: flex; align-items: baseline; justify-content: space-between;
      margin-bottom: 10px;
    }}
    .cand-title {{
      font-size: 18px;
      font-weight: 900;
      letter-spacing: 0.2px;
    }}
    .cand-sub {{
      color: #a8b3cf;
      font-size: 12px;
    }}
    .cand-cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit,minmax(260px,1fr));
      gap: 12px;
    }}
    .cand-card {{
      border: 1px solid #1d2a4a;
      border-radius: 16px;
      padding: 12px;
      background: rgba(11,15,23,0.40);
    }}
    .cand-top {{
      display: flex; justify-content: space-between; align-items: center;
      margin-bottom: 8px;
    }}
    .cand-ticker {{
      font-size: 22px;
      font-weight: 900;
    }}
    .cand-score {{
      font-size: 12px;
      padding: 4px 10px;
      border-radius: 999px;
      border: 1px solid rgba(158,203,255,0.35);
      color: #cfe6ff;
      background: rgba(158,203,255,0.10);
      font-weight: 800;
    }}
    .cand-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
      margin-top: 6px;
    }}
    .cand-grid .k {{
      display: inline-block;
      width: 92px;
      color: #a8b3cf;
      font-size: 12px;
      margin-right: 6px;
    }}
    .cand-grid .v {{
      font-weight: 900;
      font-size: 14px;
    }}
    .cand-note {{
      margin-top: 10px;
      font-size: 12px;
      color: #a8b3cf;
    }}
  </style>
</head>
<body>
  <h1>Stock Dashboard</h1>
  <div class="muted">Generated: {html_escape(now)} / Latest target date: {html_escape(latest_date or "n/a")}</div>

  <div class="section">
    <h2>Summary</h2>
    <div class="cards">
      <div class="card"><div class="muted">Total trades</div><div class="big">{summary['total']}</div></div>
      <div class="card"><div class="muted">Resolved</div><div class="big">{summary['resolved']}</div></div>
      <div class="card"><div class="muted">WIN / LOSS</div><div class="big">{summary['win']} / {summary['loss']}</div></div>
      <div class="card"><div class="muted">OPEN / NOT_FILLED</div><div class="big">{summary['open']} / {summary['not_filled']}</div></div>
      <div class="card"><div class="muted">WinRate (resolved)</div><div class="big">{summary['win_rate_resolved']:.2f}%</div></div>
    </div>
  </div>

  <div class="section spotlight">
    {cards_html}
  </div>

  <div class="section">
    <h2>OPEN (if you had entered) — Watchlist</h2>
    <div class="muted">※ これは「過去に入っていたら未決済」。今持ってないなら“買い候補”ではなく“検証の未決済”。</div>
    <table id="openTable">
      <thead><tr><th>Date</th><th>Session</th><th>Ticker</th><th>Entry</th><th>SL</th><th>TP</th><th>Status</th></tr></thead>
      <tbody>
        {''.join(row_tr(r) for r in open_rows) if open_rows else "<tr><td colspan='7'>(none)</td></tr>"}
      </tbody>
    </table>
  </div>

  <div class="section">
    <h2>All Results (latest per date/session/ticker)</h2>
    <div class="controls">
      <input id="q" type="text" placeholder="Search ticker (e.g. 8285.T) ..." />
      <select id="status">
        <option value="">All status</option>
        <option value="OPEN">OPEN</option>
        <option value="WIN">WIN</option>
        <option value="LOSS">LOSS</option>
        <option value="NOT_FILLED">NOT_FILLED</option>
      </select>

      <button class="btn active" data-range="all">All</button>
      <button class="btn" data-range="10">Last 10 days</button>
      <button class="btn" data-range="30">Last 30 days</button>

      <span class="muted" id="count"></span>
    </div>

    <table id="allTable">
      <thead><tr><th>Date</th><th>Session</th><th>Ticker</th><th>Entry</th><th>SL</th><th>TP</th><th>Status</th></tr></thead>
      <tbody>
        {''.join(row_tr(r) for r in results_rows) if results_rows else "<tr><td colspan='7'>(none)</td></tr>"}
      </tbody>
    </table>
  </div>

<script>
(function() {{
  const q = document.getElementById("q");
  const status = document.getElementById("status");
  const table = document.getElementById("allTable");
  const count = document.getElementById("count");
  const btns = Array.from(document.querySelectorAll(".btn[data-range]"));

  let rangeDays = "all";

  function parseDate(s) {{
    const parts = s.split("-");
    if (parts.length !== 3) return null;
    const y = parseInt(parts[0], 10);
    const m = parseInt(parts[1], 10) - 1;
    const d = parseInt(parts[2], 10);
    return new Date(y, m, d);
  }}

  function withinRange(dateStr) {{
    if (rangeDays === "all") return true;
    const d = parseDate(dateStr);
    if (!d) return true;
    const now = new Date();
    const ms = now.getTime() - d.getTime();
    const days = ms / (1000 * 60 * 60 * 24);
    return days <= parseInt(rangeDays, 10);
  }}

  function apply() {{
    const needle = (q.value || "").trim().toUpperCase();
    const st = status.value;

    const rows = Array.from(table.querySelectorAll("tbody tr"));
    let visible = 0;

    for (const tr of rows) {{
      const date = tr.getAttribute("data-date") || "";
      const ticker = (tr.getAttribute("data-ticker") || "").toUpperCase();
      const s = tr.getAttribute("data-status") || "";

      let ok = true;
      if (needle && !ticker.includes(needle)) ok = false;
      if (st && s !== st) ok = false;
      if (!withinRange(date)) ok = false;

      tr.style.display = ok ? "" : "none";
      if (ok) visible++;
    }}
    count.textContent = visible + " rows";
  }}

  q.addEventListener("input", apply);
  status.addEventListener("change", apply);

  btns.forEach(b => {{
    b.addEventListener("click", () => {{
      btns.forEach(x => x.classList.remove("active"));
      b.classList.add("active");
      rangeDays = b.getAttribute("data-range");
      apply();
    }});
  }});

  apply();
}})();
</script>

</body>
</html>
"""

    out_path = os.path.join(OUT_DIR, "index.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print("WRITE:", out_path)

if __name__ == "__main__":
    build()