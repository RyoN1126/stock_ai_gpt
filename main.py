# -*- coding: utf-8 -*-
"""
SCAN v3.1 (GitHub Actions stable)
- session (morning/close) + date (JST) support
- market regime filter on/off
- universe excel auto-detect (tse_listed_issues.xlsx preferred)
- yfinance MultiIndex/duplicate-columns safe normalization
- ALWAYS write signals + latest files (even when errors occur)
"""

import os
import json
import math
import argparse
from datetime import datetime, timezone, date as dt_date

import pandas as pd
import yfinance as yf

# Optional TA (if you already use it)
try:
    import ta  # type: ignore
except Exception:
    ta = None

# =========================
# Constants / Defaults
# =========================

TZ_NAME = "Asia/Tokyo"
OUTPUT_DIR = "outputs"
SIGNALS_DIR = "signals"

INTERVAL_1H = "1h"

# Risk / sizing (placeholder; adjust as needed)
ACCOUNT_SIZE = 100_000
RISK_PER_TRADE = 0.01
RR_DEFAULT = 1.8
ATR_MULT_DEFAULT = 1.0
MAX_POSITIONS_DEFAULT = 2

# Filters (tune)
MIN_PRICE = 300
MAX_PRICE = 50_000
MIN_AVG_VOL_1H = 10_000  # shares/hour average

NOW_MIN_DIST = -0.005  # -0.5%
NOW_MAX_DIST = 0.020   # +2.0%

USE_1H_CONFIRM = True  # require vol >= ma20
VOL_MA_WINDOW = 20

# Universe file preference
UNIVERSE_XLSX_PRIMARY = "tse_listed_issues.xlsx"
UNIVERSE_XLSX_FALLBACK = "data_j.xlsx"

SESSION_PRESETS = {
    "morning": {
        "cutoff": "11:30",
        "note": "Run after morning session. Use last 1H bar up to 11:30 JST.",
    },
    "close": {
        "cutoff": "15:30",
        "note": "Run after market close. Use last 1H bar up to 15:30 JST.",
    },
}


# =========================
# Helpers: JSON IO
# =========================

def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(SIGNALS_DIR, exist_ok=True)


def write_json(path: str, obj: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def safe_print(*args, **kwargs):
    # GitHub Actions: flush always
    kwargs.setdefault("flush", True)
    print(*args, **kwargs)


# =========================
# Helpers: Time
# =========================

def jst_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz=TZ_NAME)


def parse_jst_date(date_str: str | None) -> dt_date:
    if not date_str:
        return jst_now().date()
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def asof_jst_for_date_session(d: dt_date, cutoff_hhmm: str) -> pd.Timestamp:
    hh, mm = cutoff_hhmm.split(":")
    return pd.Timestamp(d.year, d.month, d.day, int(hh), int(mm), tz=TZ_NAME)


def to_utc_index(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    idx = pd.to_datetime(df.index, utc=True, errors="coerce")
    df = df.copy()
    df.index = idx
    df = df[~df.index.isna()]
    return df


def utc_to_jst(ts: pd.Timestamp) -> pd.Timestamp:
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert(TZ_NAME)


# =========================
# Helpers: yfinance normalize
# =========================

def normalize_ohlcv(df: pd.DataFrame, ticker_hint: str | None = None) -> pd.DataFrame:
    """
    Normalize yfinance OHLCV output to single-level columns: Open/High/Low/Close/Adj Close/Volume.
    Handles:
      - MultiIndex columns (Field, Ticker)
      - duplicated column names (causing df["Volume"] to become DataFrame)
    """
    if df is None or df.empty:
        return df

    df = df.copy()

    if isinstance(df.columns, pd.MultiIndex):
        # columns: (field, ticker) - try to select ticker first
        fields = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
        lvl0 = df.columns.get_level_values(0)
        lvl1 = df.columns.get_level_values(1)

        chosen_ticker = None
        if ticker_hint and ticker_hint in set(lvl1):
            chosen_ticker = ticker_hint
        else:
            # pick first available ticker
            chosen_ticker = list(dict.fromkeys(lvl1))[0]

        # build single-level DF
        out = {}
        for f in fields:
            if f in set(lvl0):
                cols = [c for c in df.columns if c[0] == f and c[1] == chosen_ticker]
                if cols:
                    out[f] = df[cols[0]]
                else:
                    # fallback: take first column for that field
                    cols_any = [c for c in df.columns if c[0] == f]
                    if cols_any:
                        out[f] = df[cols_any[0]]
        df = pd.DataFrame(out, index=df.index)

    # remove duplicated column names
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()

    # ensure UTC index
    df = to_utc_index(df)
    return df


def safe_series(df: pd.DataFrame, col: str) -> pd.Series | None:
    if df is None or df.empty:
        return None
    if col not in df.columns:
        return None
    s = df[col]
    if isinstance(s, pd.DataFrame):
        # choose first column
        s = s.iloc[:, 0]
    return s


# =========================
# Universe loading (robust)
# =========================

def pick_universe_file() -> str:
    if os.path.exists(UNIVERSE_XLSX_PRIMARY):
        return UNIVERSE_XLSX_PRIMARY
    return UNIVERSE_XLSX_FALLBACK


def load_universe_from_excel(path: str) -> list[str]:
    """
    Read an Excel universe and return tickers like '5406.T'.
    Robustly detects the code column (日本語/英語の揺れ + 4桁推定).
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Universe file not found: {path}")

    df = pd.read_excel(path)

    # 1) try name-based candidates
    candidate_cols = []
    for c in df.columns:
        s = str(c)
        if any(k in s for k in ["コード", "銘柄コード", "証券コード", "Code", "code", "Security Code"]):
            candidate_cols.append(c)

    def score_code_col(series: pd.Series) -> int:
        x = series.astype(str).str.replace(r"\D", "", regex=True)
        return int((x.str.len() == 4).sum())

    code_col = None
    if candidate_cols:
        code_col = max(candidate_cols, key=lambda c: score_code_col(df[c]))
    else:
        # 2) guess by 4-digit density
        best = None
        best_score = 0
        for c in df.columns:
            try:
                sc = score_code_col(df[c])
                if sc > best_score:
                    best_score = sc
                    best = c
            except Exception:
                continue
        code_col = best

    if code_col is None:
        raise RuntimeError("Could not detect code column in universe excel")

    codes = (
        df[code_col]
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.zfill(4)
    )
    codes = codes[codes.str.len() == 4].dropna().unique().tolist()

    return [f"{c}.T" for c in codes]


# =========================
# Market regime (simple)
# =========================

def market_regime_ok(market_ticker: str, target_date: dt_date) -> tuple[bool, dict]:
    """
    Simple: close > SMA50 and SMA50 > SMA200 on daily data.
    If insufficient data, return ok=True with note.
    """
    end = pd.Timestamp(target_date) + pd.Timedelta(days=1)
    start = end - pd.Timedelta(days=420)

    df = yf.download(
        market_ticker,
        start=str(start.date()),
        end=str(end.date()),
        interval="1d",
        auto_adjust=False,
        progress=False,
    )
    df = normalize_ohlcv(df, market_ticker)

    if df is None or df.empty or len(df) < 210:
        info = {"ticker": market_ticker, "ok": True, "note": "insufficient_market_data"}
        return True, info

    close = float(df["Close"].iloc[-1])
    sma50 = float(df["Close"].rolling(50).mean().iloc[-1])
    sma200 = float(df["Close"].rolling(200).mean().iloc[-1])

    ok = (close > sma50) and (sma50 > sma200)
    info = {"ticker": market_ticker, "close": close, "sma50": sma50, "sma200": sma200, "ok": ok}
    return ok, info


# =========================
# Resample / confirm
# =========================

def resample_4h_from_1h(df_1h: pd.DataFrame) -> pd.DataFrame:
    """
    Resample 1H OHLCV to 4H in UTC, then convert index to JST for comparisons.
    """
    d = df_1h.copy()
    d = to_utc_index(d)
    if d is None or d.empty:
        return d

    o = d["Open"].resample("4h").first()
    h = d["High"].resample("4h").max()
    l = d["Low"].resample("4h").min()
    c = d["Close"].resample("4h").last()

    out = pd.DataFrame({"Open": o, "High": h, "Low": l, "Close": c}).dropna()

    vol = safe_series(d, "Volume")
    if vol is not None:
        out["Volume"] = vol.resample("4h").sum()

    out.index = utc_to_jst(out.index)
    return out


def pick_last_1h_bar_upto_cutoff(df_1h: pd.DataFrame, asof_jst: pd.Timestamp):
    """
    Choose last 1H bar with timestamp <= asof_jst (JST).
    Returns:
      sel_ts_utc (pd.Timestamp),
      sel_row (pd.Series),
      vol_ma (float)
    """
    d = df_1h.copy()
    d = to_utc_index(d)
    if d is None or d.empty:
        raise RuntimeError("empty 1h data")

    vol = safe_series(d, "Volume")
    if vol is not None and len(vol) >= VOL_MA_WINDOW:
        vol_ma = float(vol.rolling(VOL_MA_WINDOW).mean().iloc[-1])
    else:
        vol_ma = float("nan")

    d_jst = d.copy()
    d_jst.index = utc_to_jst(d_jst.index)

    eligible = d_jst[d_jst.index <= asof_jst]
    if eligible.empty:
        raise RuntimeError("no eligible 1h bar up to cutoff")

    sel_row = eligible.iloc[-1]
    sel_ts_jst = eligible.index[-1]
    sel_ts_utc = sel_ts_jst.tz_convert("UTC")
    return sel_ts_utc, sel_row, vol_ma


# =========================
# Output writer (ALWAYS)
# =========================

def write_signals_and_latest(payload: dict, date_str: str, session: str) -> str:
    """
    Always writes:
      - signals/signals_YYYY-MM-DD_session.json (or with suffix if already exists)
      - outputs/today_candidates_latest.json
      - outputs/today_candidates_latest_meta.json (latest_file points to real path)
    Returns signal_path written.
    """
    ensure_dirs()

    base_name = f"signals_{date_str}_{session}.json"
    signal_path = os.path.join(SIGNALS_DIR, base_name)

    # Avoid overwrite if re-run
    if os.path.exists(signal_path):
        suffix = jst_now().strftime("%H%M%S")
        signal_path = os.path.join(SIGNALS_DIR, f"signals_{date_str}_{session}_{suffix}.json")

    write_json(signal_path, payload)

    latest_json = os.path.join(OUTPUT_DIR, "today_candidates_latest.json")
    write_json(latest_json, payload)

    meta_path = os.path.join(OUTPUT_DIR, "today_candidates_latest_meta.json")
    meta = {
        "written_at_utc": datetime.now(timezone.utc).isoformat(),
        "target_date_jst": date_str,
        "session": session,
        "latest_file": signal_path.replace("\\", "/"),  # must be valid relative path
        "cutoff": payload.get("cutoff"),
        "market_filter": payload.get("market_filter"),
        "market_info": payload.get("market_info"),
    }
    write_json(meta_path, meta)

    safe_print("[WRITE] signals:", signal_path)
    safe_print("[WRITE] latest_json:", latest_json)
    safe_print("[WRITE] latest_meta:", meta_path)

    return signal_path


# =========================
# Args
# =========================

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--session", required=True, choices=["morning", "close"])
    p.add_argument("--date", default=None, help="YYYY-MM-DD (JST). default: today JST")
    p.add_argument("--max_positions", type=int, default=MAX_POSITIONS_DEFAULT)
    p.add_argument("--rr", type=float, default=RR_DEFAULT)
    p.add_argument("--atr_mult", type=float, default=ATR_MULT_DEFAULT)

    p.add_argument("--market_filter", choices=["on", "off"], default="on")
    p.add_argument("--market_ticker", default="^N225")

    # optional overrides
    p.add_argument("--universe_xlsx", default=None, help="Override universe excel file path")
    return p.parse_args()


# =========================
# Main
# =========================

def main():
    args = parse_args()
    ensure_dirs()

    session = args.session
    preset = SESSION_PRESETS[session]
    cutoff = preset["cutoff"]
    note = preset["note"]

    target_date = parse_jst_date(args.date)
    date_str = target_date.strftime("%Y-%m-%d")
    asof_jst = asof_jst_for_date_session(target_date, cutoff)

    # base payload (used even on failure)
    payload = {
        "version": "v3.1",
        "asof": str(asof_jst),
        "session": session,
        "session_note": note,
        "target_date_jst": date_str,
        "cutoff": cutoff,
        "market_filter": args.market_filter,
        "market_info": None,
        "max_positions": int(args.max_positions),
        "count": 0,
        "sig_hits_total": 0,
        "drop_stats": {
            "no_data_1h": 0,
            "price_filter": 0,
            "liquidity_filter": 0,
            "dist_filter": 0,
            "confirm_1h": 0,
            "risk_zero": 0,
            "ta_missing": 0,
        },
        "errors": [],
        "candidates": [],
    }

    safe_print(f"=== SCAN v3.1 (session={session}) ===")
    safe_print(f"[ASOF JST] {asof_jst}")

    # Market filter
    try:
        if args.market_filter == "on":
            ok, info = market_regime_ok(args.market_ticker, target_date)
            payload["market_info"] = info
            safe_print("[MARKET]", info)
            if not ok:
                payload["errors"].append("market_filter_blocked_entries")
                # still write (empty candidates)
                write_signals_and_latest(payload, date_str, session)
                return
        else:
            payload["market_info"] = {"ticker": args.market_ticker, "ok": True, "note": "market_filter_off"}
            safe_print("[MARKET] filter=off")
    except Exception as e:
        payload["errors"].append(f"market_check_failed: {repr(e)}")
        write_signals_and_latest(payload, date_str, session)
        return

    # Load universe
    safe_print("=== LOAD UNIVERSE ===")
    try:
        universe_path = args.universe_xlsx or pick_universe_file()
        safe_print("[UNIVERSE FILE]", universe_path)
        tickers = load_universe_from_excel(universe_path)
        if not tickers:
            raise RuntimeError("universe empty")
    except Exception as e:
        payload["errors"].append(f"universe_load_failed: {repr(e)}")
        write_signals_and_latest(payload, date_str, session)
        return

    # Download window for date-mode (past)
    dl_start = (pd.Timestamp(target_date) - pd.Timedelta(days=90)).date()
    dl_end = (pd.Timestamp(target_date) + pd.Timedelta(days=2)).date()

    candidates = []
    sig_hits_total = 0

    # Scan each ticker
    for ticker in tickers:
        try:
            df1h = yf.download(
                ticker,
                start=str(dl_start),
                end=str(dl_end),
                interval=INTERVAL_1H,
                auto_adjust=False,
                progress=False,
            )
            df1h = normalize_ohlcv(df1h, ticker)

            if df1h is None or df1h.empty or len(df1h) < 80:
                payload["drop_stats"]["no_data_1h"] += 1
                continue

            # Price/Liquidity filters based on 1H
            vol_s = safe_series(df1h, "Volume")
            avg_vol_1h = float(vol_s.mean()) if vol_s is not None and len(vol_s) > 0 else 0.0
            if avg_vol_1h < MIN_AVG_VOL_1H:
                payload["drop_stats"]["liquidity_filter"] += 1
                continue

            # Build 4H series
            df4h = resample_4h_from_1h(df1h)
            if df4h is None or df4h.empty or len(df4h) < 40:
                payload["drop_stats"]["no_data_1h"] += 1
                continue

            close_now = float(df4h["Close"].iloc[-1])
            if not (MIN_PRICE <= close_now <= MAX_PRICE):
                payload["drop_stats"]["price_filter"] += 1
                continue

            # TA requirements
            df4h = df4h.copy()
            df4h["EMA20"] = df4h["Close"].ewm(span=20, adjust=False).mean()

            if ta is None:
                payload["drop_stats"]["ta_missing"] += 1
                # fallback ATR via simple TR rolling (rough but works)
                tr = pd.concat([
                    (df4h["High"] - df4h["Low"]),
                    (df4h["High"] - df4h["Close"].shift(1)).abs(),
                    (df4h["Low"] - df4h["Close"].shift(1)).abs(),
                ], axis=1).max(axis=1)
                df4h["ATR"] = tr.rolling(14).mean()
            else:
                df4h["ATR"] = ta.volatility.AverageTrueRange(
                    high=df4h["High"], low=df4h["Low"], close=df4h["Close"], window=14
                ).average_true_range()

            # pick confirm 1H bar up to cutoff
            sel_ts_utc, sel_row, vol_ma = pick_last_1h_bar_upto_cutoff(df1h, asof_jst)

            entry = float(sel_row["Close"])
            sel_ts_jst = utc_to_jst(pd.Timestamp(sel_ts_utc))

            # match 4H bar at/before confirm time (avoid future leak)
            idxs = [i for i, t in enumerate(df4h.index) if t <= sel_ts_jst]
            if not idxs:
                continue
            ref_i = idxs[-1]

            ema_ref = float(df4h["EMA20"].iloc[ref_i])
            dist_now = (entry / ema_ref) - 1.0
            if not (NOW_MIN_DIST <= dist_now <= NOW_MAX_DIST):
                payload["drop_stats"]["dist_filter"] += 1
                continue

            # 1H confirm
            if USE_1H_CONFIRM:
                vol1h = float(sel_row["Volume"]) if "Volume" in sel_row.index else 0.0
                if math.isnan(vol_ma) or vol_ma <= 0 or vol1h < vol_ma:
                    payload["drop_stats"]["confirm_1h"] += 1
                    continue

            atr = float(df4h["ATR"].iloc[ref_i]) if "ATR" in df4h.columns else float("nan")
            if atr <= 0 or math.isnan(atr):
                payload["drop_stats"]["risk_zero"] += 1
                continue

            sl = entry - (atr * float(args.atr_mult))
            risk_per_share = entry - sl
            if risk_per_share <= 0:
                payload["drop_stats"]["risk_zero"] += 1
                continue

            tp = entry + (risk_per_share * float(args.rr))

            risk_yen = ACCOUNT_SIZE * RISK_PER_TRADE
            shares = int(max(1, risk_yen / risk_per_share))

            # score (simple)
            candle_pct = (float(sel_row["Close"]) / float(sel_row["Open"]) - 1.0) * 100.0 if float(sel_row["Open"]) != 0 else 0.0
            vol_factor = (float(sel_row["Volume"]) / (vol_ma + 1e-9)) if (not math.isnan(vol_ma) and vol_ma > 0 and "Volume" in sel_row.index) else 0.0
            score = 0.0
            score += min(2.0, max(0.0, vol_factor))
            score += max(0.0, candle_pct / 2.0)
            score += max(0.0, 1.0 - abs(dist_now) * 50.0)

            sig_hits_total += 1

            candidates.append({
                "ticker": ticker,
                "entry": round(entry, 2),
                "sl": round(sl, 2),
                "tp": round(tp, 2),
                "shares": int(shares),
                "rr": float(args.rr),
                "atr_mult": float(args.atr_mult),
                "dist_to_ema20_pct": round(dist_now * 100, 2),
                "sig_bar_utc": str(pd.Timestamp(sel_ts_utc)),
                "score": round(score, 4),
                "avg_vol_1h": round(avg_vol_1h, 2),
            })

        except Exception as e:
            # never crash entire run due to one ticker
            payload["errors"].append(f"{ticker}: {repr(e)}")
            continue

    # finalize
    candidates = sorted(candidates, key=lambda x: x.get("score", 0.0), reverse=True)[: int(args.max_positions)]
    payload["candidates"] = candidates
    payload["count"] = len(candidates)
    payload["sig_hits_total"] = sig_hits_total

    # ALWAYS write outputs
    write_signals_and_latest(payload, date_str, session)

    safe_print("=== TODAY CANDIDATES (top) ===")
    for c in candidates:
        safe_print(c)

    safe_print("=== DONE ===")


if __name__ == "__main__":
    main()