import os
import json
import glob
import argparse
from datetime import datetime


DEFAULT_RESULTS_DIR = "results"


def _read_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _parse_key_from_filename(path: str):
    """
    result_YYYY-MM-DD_session_YYYYmmdd-HHMMSSZ.json
    result_YYYY-MM-DD_session_latest.json
    Return (date, session) or (None, None)
    """
    base = os.path.basename(path).replace(".json", "")
    parts = base.split("_")
    if len(parts) >= 3 and parts[0] == "result":
        date = parts[1]
        session = parts[2]
        return date, session
    return None, None


def _choose_latest_per_key(paths):
    """
    Keep 1 file per (date, session): prefer *_latest.json, else newest mtime.
    """
    buckets = {}
    for p in paths:
        date, sess = _parse_key_from_filename(p)
        if not date or not sess:
            continue
        buckets.setdefault((date, sess), []).append(p)

    chosen = []
    for key, files in buckets.items():
        latest_files = [f for f in files if f.endswith("_latest.json")]
        if latest_files:
            # if multiple, take newest mtime
            latest_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            chosen.append(latest_files[0])
        else:
            files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            chosen.append(files[0])

    chosen.sort()
    return chosen


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results_dir", default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--from", dest="date_from", help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--to", dest="date_to", help="YYYY-MM-DD (inclusive)")
    args = parser.parse_args()

    paths = glob.glob(os.path.join(args.results_dir, "result_*.json"))
    if not paths:
        print("No result files found in:", args.results_dir)
        return

    paths = _choose_latest_per_key(paths)

    total_trades = 0
    resolved = 0
    wins = 0
    losses = 0
    total_R = 0.0
    total_pnl = 0.0

    for p in paths:
        date, sess = _parse_key_from_filename(p)
        if not date or not sess:
            continue

        if not args.all:
            if args.date_from and date < args.date_from:
                continue
            if args.date_to and date > args.date_to:
                continue

        data = _read_json(p)
        summ = data.get("summary", {})
        total_R += float(summ.get("total_R", 0.0))
        total_pnl += float(summ.get("total_pnl_yen", 0.0))

        # Count from per-trade list to be robust
        trades = data.get("results", [])
        for t in trades:
            total_trades += 1
            r = t.get("result")
            if r in ("WIN", "LOSS"):
                resolved += 1
                if r == "WIN":
                    wins += 1
                else:
                    losses += 1

    win_rate = (wins / resolved * 100.0) if resolved else 0.0

    print("===== 集計結果 =====")
    print("対象ファイル数:", len(paths))
    print("総トレード:", total_trades)
    print("解決（WIN/LOSS）:", resolved)
    print("勝ち:", wins, "負け:", losses)
    print("勝率(解決のみ):", round(win_rate, 2), "%")
    print("合計R:", round(total_R, 2))
    print("合計PnL(円):", round(total_pnl, 2))


if __name__ == "__main__":
    main()