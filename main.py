# SCAN v2.8 (signals timestamped + keep latest + meta)
import yfinance as yf
import pandas as pd
import numpy as np
import ta
import os
import json
import argparse
from datetime import datetime
import pytz

print("=== SCAN v2.8 (signals timestamped + keep latest + meta) ===", flush=True)

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

USE_MARKET_TREND_FILTER = True
MARKET_FALLBACK = "skip"

DAILY_PERIOD = "1y"
MIN_AVG_VALUE_20D = 500_000_000
MIN_PRICE = 200
MAX_PRICE = 20000
DD_FROM_HH_MIN = -0.12
DD_FROM_HH_MAX = -0.01

B_SLEEP_MIN = 0.15
B_SLEEP_MAX = 0.45
B_RETRIES = 3

USE_1H_CONFIRM = True
VOL_MULT_1H = 1.2
MIN_1H_CANDLE_PCT = 0

# env default (kept for compatibility)
CONFIRM_BAR_MODE = os.getenv("CONFIRM_BAR_MODE", "morning_session")
MORNING_END_JST = os.getenv("MORNING_END_JST", "11:30")

OUTPUT_DIR = "outputs"
SIGNALS_DIR = os.path.join(OUTPUT_DIR, "signals")
TZ_NAME = "Asia/Tokyo"


# ===============================
# Utility
# ===============================
def _now_jst():
    return datetime.now(pytz.timezone(TZ_NAME))


def _today_str_jst():
    return _now_jst().strftime("%Y-%m-%d")


def _hhmm_str_jst():
    return _now_jst().strftime("%H%M")


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


def _to_jst(ts):
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("Asia/Tokyo")


# ===============================
# FIX ① JST基準4H
# ===============================
def resample_4h(df: pd.DataFrame) -> pd.DataFrame:
    """
    1HデータをJST基準で4Hにリサンプル（堅牢版）
    - yfinanceの微妙な返り値でも落ちないように、DataFrame resample + agg を使う
    """
    if df is None or len(df) == 0:
        return pd.DataFrame()

    df = df.copy()

    # MultiIndex対策（念のため）
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    need = ["Open", "High", "Low", "Close", "Volume"]
    if not set(need).issubset(df.columns):
        return pd.DataFrame()

    df = df[need].dropna()
    if len(df) == 0:
        return pd.DataFrame()

    # indexをUTCとして解釈→JSTへ
    idx = pd.to_datetime(df.index, errors="coerce", utc=True)
    df = df.loc[~idx.isna()].copy()
    df.index = idx[~idx.isna()].tz_convert("Asia/Tokyo")

    # 4H集計（JST基準）
    out = (
        df.resample(RESAMPLE, label="right", closed="right")
        .agg(
            {
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
            }
        )
        .dropna()
    )

    out = out[out["Volume"] > 0]
    return out


# ===============================
# 1H confirm selection
# ===============================
def pick_confirm_1h_bar(df1h: pd.DataFrame, confirm_bar_mode: str, morning_end_jst: str):
    if df1h is None or len(df1h) == 0:
        raise ValueError("empty df1h")

    df1h = df1h.copy()

    # 🔥 MultiIndex潰し
    if isinstance(df1h.columns, pd.MultiIndex):
        df1h.columns = df1h.columns.get_level_values(0)

    if "Volume" not in df1h.columns:
        raise ValueError("Volume column missing")

    vol_ma20 = df1h["Volume"].rolling(20).mean()

    if confirm_bar_mode == "latest":
        return df1h.index[-1], df1h.iloc[-1], float(vol_ma20.iloc[-1])

    jst_index = pd.Index([_to_jst(pd.Timestamp(t)) for t in df1h.index])
    cutoff_h, cutoff_m = map(int, morning_end_jst.split(":"))
    mask = [(t.hour < cutoff_h) or (t.hour == cutoff_h and t.minute <= cutoff_m) for t in jst_index]

    if any(mask):
        pos = max(i for i, ok in enumerate(mask) if ok)
        return df1h.index[pos], df1h.iloc[pos], float(vol_ma20.iloc[pos])

    return df1h.index[-1], df1h.iloc[-1], float(vol_ma20.iloc[-1])


# ===============================
# Drop counters
# ===============================
drop_stats = {
    "no_signal_4h": 0,
    "dist_filter": 0,
    "confirm_1h": 0,
    "extend_filter": 0,
    "risk_zero": 0,
}


# ===============================
# CLI
# ===============================
def parse_args():
    p = argparse.ArgumentParser(description="4H pullback scanner (signals timestamped + latest)")
    p.add_argument("--label", type=str, default=None, help="Output slot label like 1030 or 1210 (recommended)")
    p.add_argument("--confirm", type=str, default=None, choices=["latest", "morning_session"],
                   help="Override CONFIRM_BAR_MODE (env fallback)")
    p.add_argument("--cutoff", type=str, default=None, help="Override MORNING_END_JST like 11:30 (env fallback)")
    p.add_argument("--max_positions", type=int, default=None, help="Override MAX_POSITIONS")
    return p.parse_args()


# ===============================
# RUN
# ===============================
args = parse_args()

confirm_bar_mode = args.confirm if args.confirm is not None else CONFIRM_BAR_MODE
morning_end_jst = args.cutoff if args.cutoff is not None else MORNING_END_JST
max_positions = args.max_positions if args.max_positions is not None else MAX_POSITIONS

# signal filename (no overwrite)
date_str = _today_str_jst()
label = (args.label or _hhmm_str_jst()).strip()
# normalize label: allow "12:10" -> "1210"
label = label.replace(":", "")
signal_filename = f"{date_str}_{label}.json"
signal_path = os.path.join(SIGNALS_DIR, signal_filename)

# Ensure output dirs
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(SIGNALS_DIR, exist_ok=True)

print("=== LOAD UNIVERSE ===")
TICKERS = load_prime_universe_from_jpx_xlsx(UNIVERSE_XLSX)

candidates = []
sig_hits_total = 0

for ticker in TICKERS:
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

    sel_ts, sel_row, vol1h_ma20 = pick_confirm_1h_bar(df, confirm_bar_mode, morning_end_jst)

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
        {
            "ticker": ticker,
            "entry": round(entry, 2),
            "sl": round(sl, 2),
            "tp": round(tp, 2),
            "shares": shares,
        }
    )

payload = {
    "asof": _now_jst().isoformat(),
    "confirm_bar_mode": confirm_bar_mode,
    "cutoff": morning_end_jst,
    "max_positions": int(max_positions),
    "count": len(candidates),
    "sig_hits_total": sig_hits_total,
    "drop_stats": drop_stats,
    "candidates": candidates[:max_positions],
}

# 1) Write timestamped signal (no overwrite)
if os.path.exists(signal_path):
    # in case user runs same label twice in one day, avoid overwriting
    # fallback to HHMMSS suffix
    suffix = _now_jst().strftime("%H%M%S")
    signal_filename = f"{date_str}_{label}_{suffix}.json"
    signal_path = os.path.join(SIGNALS_DIR, signal_filename)

with open(signal_path, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2, ensure_ascii=False)

# 2) Update latest (compat: tools/humans can keep reading it)
latest_path = os.path.join(OUTPUT_DIR, "today_candidates_latest.json")
with open(latest_path, "w", encoding="utf-8") as f:
    json.dump(payload, f, indent=2, ensure_ascii=False)

# 3) Update latest meta (so evaluate/aggregate can know which signal latest points to)
latest_meta = {
    "asof": payload["asof"],
    "latest_file": os.path.relpath(signal_path, start=OUTPUT_DIR).replace("\\", "/"),
    "confirm_bar_mode": confirm_bar_mode,
    "cutoff": morning_end_jst,
    "max_positions": int(max_positions),
}
latest_meta_path = os.path.join(OUTPUT_DIR, "today_candidates_latest_meta.json")
with open(latest_meta_path, "w", encoding="utf-8") as f:
    json.dump(latest_meta, f, indent=2, ensure_ascii=False)

print(f"SAVED signal: {signal_path}")
print("DONE")