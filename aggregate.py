import os
import json
import argparse
from datetime import datetime

RESULT_DIR = "results"


def parse_date_from_filename(fname):
    parts = fname.split("_")
    if len(parts) < 3:
        return None
    return parts[1]


def load_results():
    files = [f for f in os.listdir(RESULT_DIR) if f.startswith("result")]
    data = []

    for f in files:
        with open(os.path.join(RESULT_DIR, f), "r", encoding="utf-8") as file:
            content = json.load(file)
            content["file"] = f
            data.append(content)

    return data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--from")
    parser.add_argument("--to")
    args = parser.parse_args()

    results = load_results()

    total_pnl = 0
    total_R = 0
    win = 0
    loss = 0

    for r in results:
        date = parse_date_from_filename(r["file"])
        if not args.all:
            if args.from and date < args.from:
                continue
            if args.to and date > args.to:
                continue

        total_pnl += r.get("total_pnl_yen", 0)
        total_R += r.get("total_R", 0)

        for trade in r.get("results", []):
            if trade["result"] == "WIN":
                win += 1
            elif trade["result"] == "LOSS":
                loss += 1

    total_trades = win + loss
    win_rate = (win / total_trades * 100) if total_trades > 0 else 0

    print("===== 集計結果 =====")
    print("総トレード:", total_trades)
    print("勝率:", round(win_rate, 2), "%")
    print("合計R:", round(total_R, 2))
    print("合計PnL(円):", round(total_pnl, 2))


if __name__ == "__main__":
    main()