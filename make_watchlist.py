# make_watchlist.py
import os
import glob
import json
import argparse
from typing import Dict, Tuple, Any, List

def parse_key_from_result_filename(path: str) -> Tuple[str, str]:
    # result_YYYY-MM-DD_session_YYYYmmdd-HHMMSSZ.json
    base = os.path.basename(path).replace(".json", "")
    parts = base.split("_")
    if len(parts) >= 3 and parts[0] == "result":
        return parts[1], parts[2]
    return "unknown-date", "unknown-session"

def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results_dir", default="results")
    ap.add_argument("--out_csv", default="watchlist.csv")
    ap.add_argument("--out_md", default="watchlist.md")
    ap.add_argument("--latest_only", action="store_true",
                    help="同一(日付,session,銘柄)は最新のresultだけ残す（おすすめ）")
    args = ap.parse_args()

    paths = sorted(glob.glob(os.path.join(args.results_dir, "result_*.json")))
    if not paths:
        print("No result_*.json found in", args.results_dir)
        return

    rows: List[dict] = []
    for p in paths:
        date, session = parse_key_from_result_filename(p)
        data = read_json(p)

        for t in data.get("results", []):
            rows.append({
                "date": date,
                "session": session,
                "ticker": t.get("ticker"),
                "entry": t.get("entry"),
                "sl": t.get("sl"),
                "tp": t.get("tp"),
                "status": t.get("result"),
                "shares_used": t.get("shares_used", t.get("shares")),
                "notional_yen": t.get("notional_yen"),
                "reason": t.get("result_reason", ""),
                "source_file": os.path.basename(p),
                "mtime": os.path.getmtime(p),
            })

    # 同一(日付,session,ticker)は最新のみ残す
    if args.latest_only:
        latest: Dict[Tuple[str, str, str], dict] = {}
        for r in rows:
            k = (r["date"], r["session"], r["ticker"])
            if k not in latest or r["mtime"] > latest[k]["mtime"]:
                latest[k] = r
        rows = list(latest.values())

    # 見やすく並べる：日付降順→session→status→ticker
    status_order = {"OPEN": 0, "WIN": 1, "LOSS": 2, "NOT_FILLED": 3}
    rows.sort(key=lambda r: (r["date"], r["session"], status_order.get(r["status"], 9), r["ticker"]), reverse=True)

    # CSV出力
    import csv
    with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["date","session","ticker","entry","sl","tp","status","shares_used","notional_yen","reason","source_file"]
        )
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in w.fieldnames})

    # Markdown出力（そのまま貼れる）
    def fmt(x):
        return "" if x is None else str(x)

    lines = []
    lines.append("# Watchlist（これまでの銘柄一覧）")
    lines.append("")
    cur = None
    for r in rows:
        key = f'{r["date"]} / {r["session"]}'
        if key != cur:
            cur = key
            lines.append(f"## {cur}")
            lines.append("")
        lines.append(f'- **{r["ticker"]}**  status: `{r["status"]}`')
        lines.append(f'  - entry: {fmt(r["entry"])} / SL: {fmt(r["sl"])} / TP: {fmt(r["tp"])}')
        if r.get("shares_used") is not None:
            lines.append(f'  - shares_used: {fmt(r["shares_used"])} / notional_yen: {fmt(r.get("notional_yen"))}')
        if r.get("reason"):
            lines.append(f'  - reason: {r["reason"]}')
        lines.append(f'  - source: {r["source_file"]}')
        lines.append("")

    with open(args.out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print("WRITE:", args.out_csv)
    print("WRITE:", args.out_md)
    print("ROWS:", len(rows))

if __name__ == "__main__":
    main()