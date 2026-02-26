# SCAN v3.0 (session=morning/close, signals timestamped + keep latest + meta)
import yfinance as yf
import pandas as pd
import ta
import os
import json
import argparse
from datetime import datetime
import pytz

print("=== SCAN v3.0 (session=morning/close) ===", flush=True)

UNIVERSE_XLSX = "tse_listed_issues.xlsx"

PERIOD_1H = "30d"
INTERVAL_1H = "60m"
RESAMPLE = "4h"

ATR_MULT = 1.2
RR = 2.0

START_EQUITY = 200_000
RISK_PCT = 0.005
MAX_POSITIONS = 2

LOOKBACK_BARS = 4
MAX_EXTEND = 0.02
NOW_MIN_DIST = 0.00
NOW_MAX_DIST = 0.01

USE_1H_CONFIRM = True
VOL_MULT_1H = 1.2
MIN_1H_CANDLE_PCT = 0

OUTPUT_DIR = "outputs"
SIGNALS_DIR = os.path.join(OUTPUT_DIR, "signals")
TZ_NAME = "Asia/Tokyo"

# Session presets (simple & understandable)
SESSION_PRESETS = {
    "morning": {
        "confirm_bar_mode": "morning_session",
        "cutoff": "11:30",
        "label": "morning",
        "note": "Run after morning session ends. Use last 1H bar up to 11:30 JST.",
    },
    "close": {
        "confirm_bar_mode": "morning_session",  # reuse cutoff-mode logic
        "cutoff": "15:30",
        "label": "close",
        "note": "Run after market close. Use last 1H bar up to 15:30 JST.",
    },
}


def _tz():
    return pytz.timezone(TZ_NAME)


def _now_jst():
    return datetime.now(_tz())


def _today_str_jst():
    return _now_jst().strftime("%Y-%m-%d")


def load_prime_universe_from_jpx_xlsx(path: str) -> list[str]:
    uni = pd.read_excel(path)
    seg = uni["Section/Products"].astype(str)
    df = uni[seg.str.contains("Prime|プライム", na=False)].copy()
    codes = (
        df["Local Code"]
        .astype(str)
        .str.extract(r"(\d{4})")[0]
        .dropna()
        .unique()
        .tolist()
    )
    return sorted({f"{c}.T" for c in codes})


def _to_jst(ts: pd.Timestamp) -> pd.Timestamp:
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert(TZ_NAME)


def resample_4h(df: pd.DataFrame) -> pd.DataFrame:
    """
    1HデータをJST基準で4Hにリサンプル（堅牢版）
    """
    if df is None or len(df) == 0:
        return pd.DataFrame()

    df = df.copy()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    need = ["Open", "High", "Low", "Close", "Volume"]
    if not set(need).issubset(df.columns):
        return pd.DataFrame()

    df = df[need].dropna()
    if len(df) == 0:
        return pd.DataFrame()

    idx = pd.to_datetime(df.index, errors="coerce", utc=True)
    df = df.loc[~idx.isna()].copy()
    df.index = idx[~idx.isna()].tz_convert(TZ_NAME)

    out = (
        df.resample(RESAMPLE, label="right", closed="right")
        .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"})
        .dropna()
    )
    out = out[out["Volume"] > 0]
    return out


def pick_confirm_1h_bar(df1h: pd.DataFrame, confirm_bar_mode: str, cutoff_jst: str):
    """
    confirm_bar_mode:
      - "latest": df1h last row
      - "morning_session": pick last row whose JST timestamp <= cutoff_jst
    """
    if df1h is None or len(df1h) == 0:
        raise ValueError("empty df1h")

    df1h = df1h.copy()

    if isinstance(df1h.columns, pd.MultiIndex):
        df1h.columns = df1h.columns.get_level_values(0)

    if "Volume" not in df1h.columns:
        raise ValueError("Volume column missing")

    vol_ma20 = df1h["Volume"].rolling(20).mean()

    if confirm_bar_mode == "latest":
        return df1h.index[-1], df1h.iloc[-1], float(vol_ma20.iloc[-1])

    # cutoff mode
    cutoff_h, cutoff_m = map(int, cutoff_jst.split(":"))
    jst_index = pd.Index([_to_jst(pd.Timestamp(t)) for t in df1h.index])

    mask = [(t.hour < cutoff_h) or (t.hour == cutoff_h and t.minute <= cutoff_m) for t in jst_index]

    if any(mask):
        pos = max(i for i, ok in enumerate(mask) if ok)
        return df1h.index[pos], df1h.iloc[pos], float(vol_ma20.iloc[pos])

    # fallback
    return df1h.index[-1], df1h.iloc[-1], float(vol_ma20.iloc[-1])


drop_stats_template = {
    "no_signal_4h": 0,
    "dist_filter": 0,
    "confirm_1h": 0,
    "extend_filter": 0,
    "risk_zero": 0,
}


def parse_args():
    p = argparse.ArgumentParser(description="4H pullback scanner (session=morning/close)")
    p.add_argument("--session", type=str, required=True, choices=["morning", "close"],
                   help="Session label: morning (after 11:30) or close (after 15:30)")
    p.add_argument("--max_positions", type=int, default=None, help="Override MAX_POSITIONS")
    return p.parse_args()


def main():
    args = parse_args()
    session = args.session

    preset = SESSION_PRESETS[session]
    confirm_bar_mode = preset["confirm_bar_mode"]
    cutoff = preset["cutoff"]
    label = preset["label"]
    max_positions = args.max_positions if args.max_positions is not None else MAX_POSITIONS

    # Ensure output dirs
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(SIGNALS_DIR, exist_ok=True)

    # signal filename (no overwrite)
    date_str = _today_str_jst()
    signal_filename = f"{date_str}_{label}.json"
    signal_path = os.path.join(SIGNALS_DIR, signal_filename)

    # avoid overwrite (if same session run twice)
    if os.path.exists(signal_path):
        suffix = _now_jst().strftime("%H%M%S")
        signal_filename = f"{date_str}_{label}_{suffix}.json"
        signal_path = os.path.join(SIGNALS_DIR, signal_filename)

    print("=== LOAD UNIVERSE ===", flush=True)
    tickers = load_prime_universe_from_jpx_xlsx(UNIVERSE_XLSX)

    candidates = []
    sig_hits_total = 0
    drop_stats = dict(drop_stats_template)

    for ticker in tickers:
        df = yf.download(ticker, period=PERIOD_1H, interval=INTERVAL_1H, progress=False)
        if df is None or len(df) < 80:
            continue

        df = df.dropna()
        df.index = pd.to_datetime(df.index)

        df4 = resample_4h(df)
        if len(df4) < 40:
            continue

        df4["EMA20"] = df4["Close"].ewm(span=20, adjust=False).mean()
        df4["ATR"] = ta.volatility.AverageTrueRange(
            high=df4["High"], low=df4["Low"], close=df4["Close"], window=14
        ).average_true_range()

        sel_ts, sel_row, vol1h_ma20 = pick_confirm_1h_bar(df, confirm_bar_mode, cutoff)

        entry = float(sel_row["Close"])
        ema_now = float(df4.iloc[-1]["EMA20"])
        dist_now = (entry / ema_now) - 1.0

        if not (NOW_MIN_DIST <= dist_now <= NOW_MAX_DIST):
            drop_stats["dist_filter"] += 1
            continue

        last_1h_open = float(sel_row["Open"])
        last_1h_close = float(sel_row["Close"])
        candle1h_pct = (last_1h_close / last_1h_open - 1.0) * 100.0
        vol1h = float(sel_row["Volume"])

        if USE_1H_CONFIRM:
            if candle1h_pct <= MIN_1H_CANDLE_PCT or vol1h < vol1h_ma20 * VOL_MULT_1H:
                drop_stats["confirm_1h"] += 1
                continue

        best_i = None
        for i in range(len(df4) - 1, max(0, len(df4) - LOOKBACK_BARS) - 1, -1):
            sig = df4.iloc[i]
            if (sig["Low"] <= sig["EMA20"]) and (sig["Close"] > sig["EMA20"]):
                sig_hits_total += 1
                if entry <= float(sig["EMA20"]) * (1.0 + MAX_EXTEND):
                    best_i = i
                    break
                else:
                    drop_stats["extend_filter"] += 1

        if best_i is None:
            drop_stats["no_signal_4h"] += 1
            continue

        atr = float(df4.iloc[best_i]["ATR"])
        sl = entry - ATR_MULT * atr
        risk_per_share = entry - sl

        if risk_per_share <= 0:
            drop_stats["risk_zero"] += 1
            continue

        tp = entry + RR * risk_per_share
        shares = int((START_EQUITY * RISK_PCT) / risk_per_share)

        candidates.append(
            {"ticker": ticker, "entry": round(entry, 2), "sl": round(sl, 2), "tp": round(tp, 2), "shares": shares}
        )

    payload = {
        "asof": _now_jst().isoformat(),
        "session": session,
        "session_note": preset["note"],
        "confirm_bar_mode": confirm_bar_mode,
        "cutoff": cutoff,
        "max_positions": int(max_positions),
        "count": len(candidates),
        "sig_hits_total": sig_hits_total,
        "drop_stats": drop_stats,
        "candidates": candidates[:max_positions],
    }

    # 1) timestamped session signal (no overwrite)
    with open(signal_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    # 2) update latest (compat)
    latest_path = os.path.join(OUTPUT_DIR, "today_candidates_latest.json")
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    # 3) update latest meta (points to signals file)
    latest_meta = {
        "asof": payload["asof"],
        "session": session,
        "latest_file": os.path.relpath(signal_path, start=OUTPUT_DIR).replace("\\", "/"),
        "confirm_bar_mode": confirm_bar_mode,
        "cutoff": cutoff,
        "max_positions": int(max_positions),
    }
    latest_meta_path = os.path.join(OUTPUT_DIR, "today_candidates_latest_meta.json")
    with open(latest_meta_path, "w", encoding="utf-8") as f:
        json.dump(latest_meta, f, indent=2, ensure_ascii=False)

    print(f"SAVED signal: {signal_path}", flush=True)
    print("DONE", flush=True)


if __name__ == "__main__":
    main()