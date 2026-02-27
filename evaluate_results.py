import os
import json
import argparse
from datetime import datetime
import pandas as pd
import yfinance as yf

SIGNALS_DIR = "signals"
OUTPUT_DIR = "results"
LATEST_META = "outputs/today_candidates_latest_meta.json"

os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_latest_signal_path():
    if not os.path.exists(LATEST_META):
        raise FileNotFoundError("latest_meta not found")
    with open(LATEST_META, "r", encoding="utf-8") as f:
        meta = json.load(f)
    return meta["latest_file"]


def find_signal_file(date_str, session):
    fname = f"signals_{date_str}_{session}.json"
    path = os.path.join(SIGNALS_DIR, fname)
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found")
    return path


def evaluate_trade(ticker, entry, sl, tp, shares, start_date):
    df = yf.download(ticker, start=start_date, interval="1h", progress=False)
    if df.empty:
        return None

    for _, row in df.iterrows():
        high = row["High"]
        low = row["Low"]

        if low <= sl:
            r = (sl - entry) / (entry - sl)
            pnl = (sl - entry) * shares
            return {"result": "LOSS", "R": -1, "pnl_yen": pnl}

        if high >= tp:
            r = (tp - entry) / (entry - sl)
            pnl = (tp - entry) * shares
            return {"result": "WIN", "R": r, "pnl_yen": pnl}

    return {"result": "OPEN", "R": 0, "pnl_yen": 0}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--latest", action="store_true")
    parser.add_argument("--date")
    parser.add_argument("--session", choices=["morning", "close"])
    args = parser.parse_args()

    if args.latest:
        signal_path = load_latest_signal_path()
    else:
        if not args.date or not args.session:
            raise ValueError("Specify --latest OR --date and --session")
        signal_path = find_signal_file(args.date, args.session)

    with open(signal_path, "r", encoding="utf-8") as f:
        signals = json.load(f)

    results = []
    total_pnl = 0
    total_R = 0

    for s in signals["candidates"]:
        ticker = s["ticker"]
        entry = s["entry"]
        sl = s["sl"]
        tp = s["tp"]
        shares = s["shares"]
        start_date = signals["asof"][:10]

        result = evaluate_trade(ticker, entry, sl, tp, shares, start_date)
        if result:
            result["ticker"] = ticker
            results.append(result)
            total_pnl += result["pnl_yen"]
            total_R += result["R"]

    out = {
        "meta": signals.get("meta", {}),
        "results": results,
        "total_pnl_yen": total_pnl,
        "total_R": total_R,
    }

    out_name = os.path.basename(signal_path).replace("signals", "result")
    out_path = os.path.join(OUTPUT_DIR, out_name)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print("Saved:", out_path)


if __name__ == "__main__":
    main()