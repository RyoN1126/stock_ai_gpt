# SCAN v3.0 (session=morning/close, signals timestamped + keep latest + meta)
import math
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

# Liquidity / price filters
MIN_PRICE = 300
MAX_PRICE = 50_000
MIN_AVG_VOL_1H = 10_000  # minimum average hourly volume (shares)

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


def normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize yfinance output to single-ticker OHLCV columns.

    yfinance sometimes returns MultiIndex columns like (Field, Ticker) even for a single ticker.
    Naively dropping one level can create duplicated column names, which then turns df["Volume"]
    into a DataFrame (not Series) and breaks downstream float()/rolling() calls.

    This function:
      - Collapses MultiIndex to one set of OHLCV columns by taking the first column per field.
      - If columns are duplicated, keeps the first occurrence per name.
    """
    if df is None or len(df) == 0:
        return df

    df = df.copy()

    # MultiIndex: (Field, Ticker) or similar
    if isinstance(df.columns, pd.MultiIndex):
        lvl0 = df.columns.get_level_values(0)
        fields = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
        new = {}
        for f in fields:
            if f in set(lvl0):
                cols = [c for c in df.columns if c[0] == f]
                if cols:
                    new[f] = df[cols[0]]
        if new:
            df = pd.DataFrame(new, index=df.index)
        else:
            # Fallback: just take the last level
            df.columns = df.columns.get_level_values(-1)

    # Drop duplicated column names safely (keep first)
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()

    return df

def _tz():
    return pytz.timezone(TZ_NAME)


def _now_jst():
    return datetime.now(_tz())


def _today_str_jst():
    return _now_jst().strftime("%Y-%m-%d")



def _parse_date_jst(date_str: str | None):
    """Parse YYYY-MM-DD as a JST date. If None, use today's JST date."""
    if not date_str:
        return _now_jst().date()
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def _asof_jst_for_session(d, cutoff_jst: str) -> pd.Timestamp:
    """Build a tz-aware JST timestamp for a given date and cutoff time (HH:MM)."""
    h, m = map(int, cutoff_jst.split(":"))
    return pd.Timestamp(datetime(d.year, d.month, d.day, h, m, 0, tzinfo=_tz()))


def market_regime_ok(market_ticker: str, asof_date_jst) -> tuple[bool, dict]:
    """Simple regime filter: close > SMA50 and SMA50 > SMA200 on or before asof_date_jst."""
    try:
        dfm = yf.download(market_ticker, period="3y", interval="1d", progress=False)
        dfm = normalize_ohlcv(dfm)
    except Exception:
        return True, {"market_error": "download_failed"}

    if dfm is None or len(dfm) < 260:
        return True, {"market_error": "insufficient_data"}

    if isinstance(dfm.columns, pd.MultiIndex):
        dfm.columns = dfm.columns.get_level_values(0)

    dfm = dfm.dropna()
    idx = pd.to_datetime(dfm.index, errors="coerce", utc=True)
    dfm = dfm.loc[~idx.isna()].copy()
    dfm.index = idx[~idx.isna()].tz_convert(TZ_NAME)

    # keep rows up to end of asof_date_jst (JST)
    end_jst = pd.Timestamp(datetime(asof_date_jst.year, asof_date_jst.month, asof_date_jst.day, 23, 59, 59, tzinfo=_tz()))
    dfm = dfm.loc[dfm.index <= end_jst]
    if len(dfm) < 260:
        return True, {"market_error": "insufficient_data_asof"}

    close = dfm["Close"]
    sma50 = close.rolling(50).mean()
    sma200 = close.rolling(200).mean()

    c = float(close.iloc[-1])
    s50 = float(sma50.iloc[-1])
    s200 = float(sma200.iloc[-1])

    ok = (c > s50) and (s50 > s200)
    info = {"close": c, "sma50": s50, "sma200": s200, "ok": ok, "ticker": market_ticker}
    return ok, info
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
    df = normalize_ohlcv(df)

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


def pick_confirm_1h_bar(df1h: pd.DataFrame, asof_jst: pd.Timestamp):
    """
    Pick the last 1H bar at/before a given JST datetime (asof_jst).

    - df1h index may be tz-naive or tz-aware; we normalize to UTC then convert to JST.
    - Returns: (original_ts, row, vol_ma20_at_bar)
    """
    if df1h is None or len(df1h) == 0:
        raise ValueError("empty df1h")

    df1h = df1h.copy()
    df1h = normalize_ohlcv(df1h)

    if "Volume" not in df1h.columns:
        raise ValueError("Volume column missing")

    # Normalize index to UTC then to JST for comparison
    idx_utc = pd.to_datetime(df1h.index, errors="coerce", utc=True)
    ok = ~idx_utc.isna()
    df1h = df1h.loc[ok].copy()
    idx_utc = idx_utc[ok]
    jst_index = idx_utc.tz_convert(TZ_NAME)

    vol_ma20 = df1h["Volume"].rolling(20).mean()

    # last bar at/before asof_jst
    if asof_jst.tzinfo is None:
        asof_jst = asof_jst.tz_localize(TZ_NAME)

    mask = jst_index <= asof_jst
    if mask.any():
        pos = int(mask.to_numpy().nonzero()[0][-1])
        return df1h.index[pos], df1h.iloc[pos], float(vol_ma20.iloc[pos])

    # fallback: nothing before asof → use earliest bar (safer than latest)
    return df1h.index[0], df1h.iloc[0], float(vol_ma20.iloc[0])


drop_stats_template = {
    "no_signal_4h": 0,
    "dist_filter": 0,
    "confirm_1h": 0,
    "extend_filter": 0,
    "risk_zero": 0,
    "price_filter": 0,
    "liquidity_filter": 0,
}


def parse_args():
    p = argparse.ArgumentParser(description="4H pullback scanner (session=morning/close)")
    p.add_argument(
        "--session",
        type=str,
        required=True,
        choices=["morning", "close"],
        help="Session label: morning (uses last 1H bar up to 11:30 JST) or close (up to 15:30 JST)",
    )
    p.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date in JST (YYYY-MM-DD). Default: today (JST).",
    )
    p.add_argument("--max_positions", type=int, default=None, help="Override MAX_POSITIONS")
    p.add_argument(
        "--market_filter",
        type=str,
        default="on",
        choices=["on", "off"],
        help="Enable/disable market regime filter (default: on).",
    )
    p.add_argument(
        "--market_ticker",
        type=str,
        default="^N225",
        help='Market ticker for regime filter (default: "^N225").',
    )
    return p.parse_args()


def main():
    args = parse_args()
    session = args.session

    preset = SESSION_PRESETS[session]
    cutoff = preset["cutoff"]
    label = preset["label"]
    max_positions = args.max_positions if args.max_positions is not None else MAX_POSITIONS

    # Target "as-of" date/time (JST)
    target_date = _parse_date_jst(args.date)
    date_str = target_date.strftime("%Y-%m-%d")
    asof_jst = _asof_jst_for_session(target_date, cutoff)

    # Ensure output dirs
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(SIGNALS_DIR, exist_ok=True)

    # signal filename (no overwrite)
    signal_filename = f"{date_str}_{label}.json"
    signal_path = os.path.join(SIGNALS_DIR, signal_filename)

    # avoid overwrite (if same session run twice)
    if os.path.exists(signal_path):
        suffix = _now_jst().strftime("%H%M%S")
        signal_filename = f"{date_str}_{label}_{suffix}.json"
        signal_path = os.path.join(SIGNALS_DIR, signal_filename)

    # Optional market regime filter
    market_info = None
    if args.market_filter == "on":
        ok, market_info = market_regime_ok(args.market_ticker, target_date)
        print(f"[MARKET] {market_info}", flush=True)
        if not ok:
            print("Market regime filter: OFFENSE DISABLED (no new entries).", flush=True)
            # still write outputs for traceability
            payload = {
                "asof": str(asof_jst),
                "session": session,
                "session_note": preset.get("note"),
                "target_date_jst": date_str,
                "cutoff": cutoff,
                "market_filter": "on",
                "market_info": market_info,
                "max_positions": max_positions,
                "count": 0,
                "sig_hits_total": 0,
                "drop_stats": dict(drop_stats_template),
                "candidates": [],
            }
            with open(signal_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

            # Update latest files
            latest_path = os.path.join(OUTPUT_DIR, "today_candidates_latest.json")
            with open(latest_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

            meta_path = os.path.join(OUTPUT_DIR, "today_candidates_latest_meta.json")
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump({"asof": payload["asof"], "session": session, "target_date_jst": date_str, "latest_file": os.path.relpath(signal_path, start=OUTPUT_DIR).replace("\\", "/"), "market_filter": args.market_filter, "market_info": market_info, "cutoff": cutoff}, f, ensure_ascii=False, indent=2)
            return
    else:
        print("[MARKET] filter=off", flush=True)
    print("=== LOAD UNIVERSE ===", flush=True)
    tickers = load_prime_universe_from_jpx_xlsx(UNIVERSE_XLSX)

    candidates = []
    sig_hits_total = 0
    drop_stats = dict(drop_stats_template)

    # Download window (so --date can be in the past)
    dl_start = (datetime(target_date.year, target_date.month, target_date.day) - pd.Timedelta(days=90)).date()
    dl_end = (datetime(target_date.year, target_date.month, target_date.day) + pd.Timedelta(days=2)).date()

    for ticker in tickers:
        df = yf.download(ticker, start=str(dl_start), end=str(dl_end), interval=INTERVAL_1H, progress=False)

        df = normalize_ohlcv(df)
        if df is None or len(df) < 80:
            continue

        df = df.dropna()
        df.index = pd.to_datetime(df.index, utc=True)

        df4 = resample_4h(df)
        if len(df4) < 40:
            continue

        df4["EMA20"] = df4["Close"].ewm(span=20, adjust=False).mean()
        df4["ATR"] = ta.volatility.AverageTrueRange(
            high=df4["High"], low=df4["Low"], close=df4["Close"], window=14
        ).average_true_range()

        # --- Price filter ---
        current_close = float(df4.iloc[-1]["Close"])
        if not (MIN_PRICE <= current_close <= MAX_PRICE):
            drop_stats["price_filter"] += 1
            continue

        # --- Liquidity filter: minimum average hourly volume ---
        avg_vol_1h = float(df["Volume"].mean()) if "Volume" in df.columns else 0.0
        if avg_vol_1h < MIN_AVG_VOL_1H:
            drop_stats["liquidity_filter"] += 1
            continue

        try:
            sel_ts, sel_row, vol1h_ma20 = pick_confirm_1h_bar(df, asof_jst)
        except Exception:
            continue

        entry = float(sel_row["Close"])

        # Use EMA20 from the 4H bar at/before confirm bar time (avoids future-bar leak)
        sel_ts_jst = _to_jst(pd.Timestamp(sel_ts))
        idxs = [i for i, t in enumerate(df4.index) if t <= sel_ts_jst]
        if not idxs:
            # no 4H bar at/before confirm timestamp → skip to avoid future-bar leak
            continue
        ema_ref_idx = idxs[-1]
        ema_now = float(df4.iloc[ema_ref_idx]["EMA20"])
        dist_now = (entry / ema_now) - 1.0

        if not (NOW_MIN_DIST <= dist_now <= NOW_MAX_DIST):
            drop_stats["dist_filter"] += 1
            continue

        last_1h_open = float(sel_row["Open"])
        last_1h_close = float(sel_row["Close"])
        candle1h_pct = (last_1h_close / last_1h_open - 1.0) * 100.0
        vol1h = float(sel_row["Volume"])

        if USE_1H_CONFIRM:
            if math.isnan(vol1h_ma20) or vol1h_ma20 <= 0:
                drop_stats["confirm_1h"] += 1
                continue
            if candle1h_pct <= MIN_1H_CANDLE_PCT or vol1h < vol1h_ma20 * VOL_MULT_1H:
                drop_stats["confirm_1h"] += 1
                continue

        best_i = None
        for i in range(len(df4) - 1, max(0, len(df4) - LOOKBACK_BARS) - 1, -1):
            sig = df4.iloc[i]
            # NaN guard: skip bars where indicators are not yet computed
            if math.isnan(sig["EMA20"]) or math.isnan(sig["ATR"]):
                continue
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

        sig_bar = df4.iloc[best_i]
        atr = float(sig_bar["ATR"])
        sl = entry - ATR_MULT * atr
        risk_per_share = entry - sl

        if risk_per_share <= 0:
            drop_stats["risk_zero"] += 1
            continue

        tp = entry + RR * risk_per_share
        shares = int((START_EQUITY * RISK_PCT) / risk_per_share)

        if shares <= 0:
            drop_stats["risk_zero"] += 1
            continue

        # --- Score (higher = better) ---
        bars_ago = (len(df4) - 1) - best_i
        atrp = (atr / entry) * 100.0
        rebound_pct = (float(sig_bar["Close"]) / float(sig_bar["EMA20"]) - 1.0) * 100.0
        touch_depth_pct = (float(sig_bar["EMA20"]) / float(sig_bar["Low"]) - 1.0) * 100.0
        vol_boost = math.log1p(vol1h / vol1h_ma20)
        score = (
            (2.0 - abs(dist_now * 100.0))
            + (candle1h_pct * 1.4)
            + (vol_boost * 1.2)
            + (max(0, 4 - bars_ago) * 0.25)
            + (rebound_pct * 2.0)
            - (abs(atrp - 2.5) * 0.2)
            - (max(0, touch_depth_pct - 1.2) * 0.8)
        )

        candidates.append(
            {
                "ticker": ticker,
                "entry": round(entry, 2),
                "sl": round(sl, 2),
                "tp": round(tp, 2),
                "shares": shares,
                "score": round(score, 3),
                "dist_to_ema20_pct": round(dist_now * 100, 2),
                "candle1h_pct": round(candle1h_pct, 2),
                "vol1h_vs_ma20": round(vol1h / vol1h_ma20, 2),
                "bars_ago": int(bars_ago),
                "rebound_pct": round(rebound_pct, 2),
                "touch_depth_pct": round(touch_depth_pct, 2),
            }
        )

    # Sort by score descending so candidates[:max_positions] picks the best
    candidates.sort(key=lambda x: x["score"], reverse=True)

    payload = {
        "asof": str(asof_jst),
        "session": session,
        "session_note": preset.get("note"),
        "target_date_jst": date_str,
        "cutoff": cutoff,
        "market_filter": args.market_filter,
        "market_info": market_info,
        "max_positions": int(max_positions),
        "count": min(len(candidates), int(max_positions)),
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