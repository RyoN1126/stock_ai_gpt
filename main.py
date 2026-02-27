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

UNIVERSE_XLSX = "data_j.xlsx"  # JPXのプライム一覧（あなたの環境に合わせて）
INTERVAL_1H = "1h"

# Risk / sizing
ACCOUNT_SIZE = 100_000  # placeholder (使ってないなら削ってOK)
RISK_PER_TRADE = 0.01
RR = 1.8
ATR_MULT = 1.0
MAX_POSITIONS = 2

# Filters
USE_1H_CONFIRM = True
NOW_MIN_DIST = -0.005
NOW_MAX_DIST = 0.020

# Liquidity / price filters
MIN_PRICE = 300
MAX_PRICE = 50_000
MIN_AVG_VOL_1H = 10_000  # minimum average hourly volume (shares)

OUTPUT_DIR = "outputs"
SIGNALS_DIR = "signals"
TZ_NAME = "Asia/Tokyo"

# Session presets (simple & understandable)
SESSION_PRESETS = {
    "morning": {
        "label": "morning",
        "cutoff": "11:30",
        "note": "Run after morning session. Use last 1H bar up to 11:30 JST.",
        "confirm_bar_mode": "morning_session",
    },
    "close": {
        "label": "close",
        "cutoff": "15:30",
        "note": "Run after market close. Use last 1H bar up to 15:30 JST.",
        "confirm_bar_mode": "morning_session",
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
    hh, mm = cutoff_jst.split(":")
    ts = pd.Timestamp(
        year=d.year, month=d.month, day=d.day, hour=int(hh), minute=int(mm), tz=TZ_NAME
    )
    return ts


def _to_jst(ts: pd.Timestamp) -> pd.Timestamp:
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert(TZ_NAME)


def load_prime_universe_from_jpx_xlsx(path: str) -> list[str]:
    """Read JPX excel and return tickers list like 5406.T.
    (ここはあなたの既存ロジックに合わせている想定)
    """
    # 例: Excelの中身に合わせて調整してください
    df = pd.read_excel(path)
    # 例: 'コード'列がある想定
    codes = df.iloc[:, 0].astype(str).str.zfill(4).tolist()
    tickers = [f"{c}.T" for c in codes]
    return tickers


def market_regime_ok(market_ticker: str, target_date) -> tuple[bool, dict]:
    """Simple market regime filter: close > SMA50 and SMA50 > SMA200 on 1D data."""
    end = pd.Timestamp(target_date) + pd.Timedelta(days=1)
    start = end - pd.Timedelta(days=400)

    df = yf.download(
        market_ticker, start=str(start.date()), end=str(end.date()), interval="1d", progress=False
    )
    df = normalize_ohlcv(df)
    if df is None or df.empty or len(df) < 210:
        info = {"ticker": market_ticker, "ok": True, "note": "insufficient_market_data"}
        return True, info

    close = float(df["Close"].iloc[-1])
    sma50 = float(df["Close"].rolling(50).mean().iloc[-1])
    sma200 = float(df["Close"].rolling(200).mean().iloc[-1])

    ok = (close > sma50) and (sma50 > sma200)
    info = {"close": close, "sma50": sma50, "sma200": sma200, "ok": ok, "ticker": market_ticker}
    return ok, info


def resample_4h(df_1h: pd.DataFrame) -> pd.DataFrame:
    """Resample 1H to 4H aligned on UTC index, then convert to JST index for comparisons."""
    # index must be tz-aware UTC
    d = df_1h.copy()
    if d.index.tz is None:
        d.index = pd.to_datetime(d.index, utc=True)
    else:
        d.index = d.index.tz_convert("UTC")

    o = d["Open"].resample("4h").first()
    h = d["High"].resample("4h").max()
    l = d["Low"].resample("4h").min()
    c = d["Close"].resample("4h").last()
    v = d["Volume"].resample("4h").sum() if "Volume" in d.columns else None

    out = pd.DataFrame({"Open": o, "High": h, "Low": l, "Close": c})
    if v is not None:
        out["Volume"] = v
    out = out.dropna()

    # Use JST index for comparing with sel_ts_jst
    out.index = _to_jst(out.index)
    return out


def pick_confirm_1h_bar(df_1h: pd.DataFrame, asof_jst: pd.Timestamp):
    """Pick the last 1H bar up to cutoff (asof_jst) in JST.
    Returns: (sel_ts_utc, sel_row, vol_ma20)
    """
    d = df_1h.copy()
    d = normalize_ohlcv(d)
    d = d.dropna()

    # force UTC tz-aware index
    d.index = pd.to_datetime(d.index, utc=True)

    # compute vol ma20 on 1h
    vol = d["Volume"] if "Volume" in d.columns else None
    if isinstance(vol, pd.DataFrame):
        vol = vol.iloc[:, 0]
    vol_ma20 = vol.rolling(20).mean().iloc[-1] if vol is not None and len(vol) >= 20 else float("nan")

    # choose last bar <= asof_jst (JST)
    d_jst = d.copy()
    d_jst.index = _to_jst(d_jst.index)

    eligible = d_jst[d_jst.index <= asof_jst]
    if eligible.empty:
        raise RuntimeError("no eligible 1h bar up to cutoff")

    sel = eligible.iloc[-1]
    sel_ts_jst = eligible.index[-1]
    sel_ts_utc = sel_ts_jst.tz_convert("UTC")

    return sel_ts_utc, sel, float(vol_ma20)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--session", type=str, required=True, choices=["morning", "close"])
    p.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (JST). default: today JST")
    p.add_argument("--max_positions", type=int, default=None)
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
    signal_filename = f"signals_{date_str}_{session}.json"
    signal_path = os.path.join(SIGNALS_DIR, signal_filename)

    # avoid overwrite (if same session run twice)
    if os.path.exists(signal_path):
        suffix = _now_jst().strftime("%H%M%S")
        signal_filename = f"signals_{date_str}_{session}_{suffix}.json"
        signal_path = os.path.join(SIGNALS_DIR, signal_filename)

    drop_stats_template = {
        "no_signal_4h": 0,
        "dist_filter": 0,
        "confirm_1h": 0,
        "extend_filter": 0,
        "risk_zero": 0,
        "price_filter": 0,
        "liquidity_filter": 0,
    }

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
                json.dump(
                    {
                        "asof": payload["asof"],
                        "session": session,
                        "target_date_jst": date_str,
                        "latest_file": signal_path.replace("\\", "/"),
                        "market_filter": args.market_filter,
                        "market_info": market_info,
                        "cutoff": cutoff,
                    },
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
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

        # --- Liquidity filter: minimum average hourly volume (robust to duplicated columns) ---
        vol_col = df["Volume"] if "Volume" in df.columns else None
        if isinstance(vol_col, pd.DataFrame):
            vol_col = vol_col.iloc[:, 0]
        avg_vol_1h = float(vol_col.mean()) if vol_col is not None else 0.0
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
        vol1h = float(sel_row["Volume"]) if "Volume" in sel_row.index else 0.0

        if USE_1H_CONFIRM:
            if math.isnan(vol1h_ma20) or vol1h_ma20 <= 0:
                drop_stats["confirm_1h"] += 1
                continue
            # volume spike or positive candle etc.（あなたの既存条件に合わせて調整）
            if vol1h < vol1h_ma20:
                drop_stats["confirm_1h"] += 1
                continue

        atr = float(df4.iloc[ema_ref_idx]["ATR"])
        if atr <= 0 or math.isnan(atr):
            drop_stats["risk_zero"] += 1
            continue

        sl = entry - (atr * ATR_MULT)
        risk_per_share = entry - sl
        if risk_per_share <= 0:
            drop_stats["risk_zero"] += 1
            continue

        tp = entry + (risk_per_share * RR)

        # position sizing (simple)
        risk_yen = ACCOUNT_SIZE * RISK_PER_TRADE
        shares = int(max(1, risk_yen / risk_per_share))

        sig_hits_total += 1

        score = 0.0
        score += (min(2.0, max(0.0, (vol1h / (vol1h_ma20 + 1e-9)))))  # volume factor
        score += max(0.0, (candle1h_pct / 2.0))  # candle factor
        score += max(0.0, 1.0 - abs(dist_now) * 50.0)  # dist closeness

        candidates.append(
            {
                "ticker": ticker,
                "entry": round(entry, 2),
                "sl": round(sl, 2),
                "tp": round(tp, 2),
                "shares": shares,
                "dist_to_ema20": round(dist_now * 100, 2),
                "rr": RR,
                "sig_bar": str(sel_ts),
                "score": round(score, 4),
                "avg_vol_1h": round(avg_vol_1h, 2),
            }
        )

    # sort and take top N
    candidates = sorted(candidates, key=lambda x: x["score"], reverse=True)[:max_positions]

    payload = {
        "asof": str(asof_jst),
        "session": session,
        "session_note": preset.get("note"),
        "target_date_jst": date_str,
        "cutoff": cutoff,
        "market_filter": args.market_filter,
        "market_info": market_info,
        "max_positions": max_positions,
        "count": len(candidates),
        "sig_hits_total": sig_hits_total,
        "drop_stats": drop_stats,
        "candidates": candidates,
    }

    # write signals (no overwrite)
    with open(signal_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # update latest
    latest_path = os.path.join(OUTPUT_DIR, "today_candidates_latest.json")
    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    meta_path = os.path.join(OUTPUT_DIR, "today_candidates_latest_meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "asof": payload["asof"],
                "session": session,
                "target_date_jst": date_str,
                "latest_file": signal_path.replace("\\", "/"),
                "market_filter": args.market_filter,
                "market_info": market_info,
                "cutoff": cutoff,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    print(f"=== DONE ({date_str} {session}) ===", flush=True)
    print(f"[WRITE] {signal_path}", flush=True)
    print(f"[WRITE] {latest_path}", flush=True)
    print(f"[WRITE] {meta_path}", flush=True)
    print("=== TODAY CANDIDATES (top) ===", flush=True)
    for c in candidates:
        print(c, flush=True)


if __name__ == "__main__":
    main()