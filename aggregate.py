import os
import json
import glob
import argparse
from typing import Dict, List, Tuple, Optional


DEFAULT_RESULTS_DIR = "results"


def _read_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _parse_key_from_filename(path: str) -> Tuple[Optional[str], Optional[str]]:
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


def _choose_latest_per_key(paths: List[str]) -> List[str]:
    """
    Keep 1 file per (date, session): prefer *_latest.json, else newest mtime.
    """
    buckets: Dict[Tuple[str, str], List[str]] = {}
    for p in paths:
        date, sess = _parse_key_from_filename(p)
        if not date or not sess:
            continue
        buckets.setdefault((date, sess), []).append(p)

    chosen: List[str] = []
    for _key, files in buckets.items():
        latest_files = [f for f in files if f.endswith("_latest.json")]
        if latest_files:
            latest_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            chosen.append(latest_files[0])
        else:
            files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            chosen.append(files[0])

    chosen.sort()
    return chosen


def _in_date_range(date: str, date_from: Optional[str], date_to: Optional[str]) -> bool:
    if date_from and date < date_from:
        return False
    if date_to and date > date_to:
        return False
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results_dir", default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--all_files", action="store_true",
                        help="集計対象を間引かず、results_dir 配下の result_*.json を全て集計する（通常は日付×sessionごとに最新1件に絞る）")
    parser.add_argument("--from", dest="date_from", help="YYYY-MM-DD (inclusive)")
    parser.add_argument("--to", dest="date_to", help="YYYY-MM-DD (inclusive)")
    args = parser.parse_args()

    paths = glob.glob(os.path.join(args.results_dir, "result_*.json"))
    if not paths:
        print("No result files found in:", args.results_dir)
        return

    # Default: dedupe to latest per (date, session) for stable reporting
    if not args.all_files:
        paths = _choose_latest_per_key(paths)

    total_trades = 0
    wins = 0
    losses = 0
    opens = 0
    not_filled = 0

    total_R = 0.0
    total_pnl = 0.0

    used_files = 0

    for p in paths:
        date, sess = _parse_key_from_filename(p)
        if not date or not sess:
            continue
        if not _in_date_range(date, args.date_from, args.date_to):
            continue

        data = _read_json(p)
        trades = data.get("results", [])

        # Prefer per-trade numbers (more robust), fall back to summary if absent.
        file_R = 0.0
        file_pnl = 0.0
        file_has_trade_values = False

        for t in trades:
            total_trades += 1
            r = t.get("result")

            if r == "WIN":
                wins += 1
            elif r == "LOSS":
                losses += 1
            elif r == "OPEN":
                opens += 1
            elif r == "NOT_FILLED":
                not_filled += 1
            else:
                # unknown label → treat as OPEN-ish
                opens += 1

            if "R" in t:
                try:
                    file_R += float(t.get("R", 0.0))
                    file_has_trade_values = True
                except Exception:
                    pass
            if "pnl_yen" in t:
                try:
                    file_pnl += float(t.get("pnl_yen", 0.0))
                    file_has_trade_values = True
                except Exception:
                    pass

        if file_has_trade_values:
            total_R += file_R
            total_pnl += file_pnl
        else:
            summ = data.get("summary", {})
            total_R += float(summ.get("total_R", 0.0))
            total_pnl += float(summ.get("total_pnl_yen", 0.0))

        used_files += 1

    resolved = wins + losses
    filled = total_trades - not_filled
    win_rate_resolved = (wins / resolved * 100.0) if resolved else 0.0
    fill_rate = (filled / total_trades * 100.0) if total_trades else 0.0
    not_filled_rate = (not_filled / total_trades * 100.0) if total_trades else 0.0
    open_rate = (opens / total_trades * 100.0) if total_trades else 0.0

    print("===== 集計結果 =====")
    print("対象ファイル数:", used_files, "(候補:", len(paths), ")")
    if args.date_from or args.date_to:
        print("期間:", args.date_from or "-", "〜", args.date_to or "-")
    print("総トレード:", total_trades)
    print("解決（WIN/LOSS）:", resolved)
    print("内訳: WIN:", wins, "LOSS:", losses, "OPEN:", opens, "NOT_FILLED:", not_filled)
    print("勝率(解決のみ):", round(win_rate_resolved, 2), "%")
    print("FillRate(約定扱い):", round(fill_rate, 2), "%")
    print("NOT_FILLED率:", round(not_filled_rate, 2), "%  OPEN率:", round(open_rate, 2), "%")
    print("合計R:", round(total_R, 2))
    print("合計PnL(円):", round(total_pnl, 2))


if __name__ == "__main__":
    main()
