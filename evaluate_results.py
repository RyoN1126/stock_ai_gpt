import os
import json
import glob
import argparse
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf


DEFAULT_SIGNALS_DIR = "signals"
DEFAULT_RESULTS_DIR = "results"
DEFAULT_LATEST_META = os.path.join("outputs", "today_candidates_latest_meta.json")


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _read_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, obj: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def _coerce_utc_index(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure datetime index is tz-aware UTC to avoid timezone surprises."""
    if df is None or df.empty:
        return df
    idx = pd.to_datetime(df.index, utc=True, errors="coerce")
    df = df.copy()
    df.index = idx
    df = df[~df.index.isna()]
    return df


def _normalize_ohlcv(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    yfinance sometimes returns:
      - single-index columns: Open/High/Low/Close/Adj Close/Volume
      - MultiIndex columns: (Field, Ticker)
    This normalizes to a single-ticker OHLCV DataFrame with single-level columns.
    """
    if df is None or df.empty:
        return df

    df = df.copy()

    # MultiIndex: (Field, Ticker)
    if isinstance(df.columns, pd.MultiIndex):
        # try to select the requested ticker
        lvl1 = df.columns.get_level_values(1)
        if ticker in set(lvl1):
            df = df.xs(ticker, level=1, axis=1)
        else:
            # fallback: pick the first ticker present
            first_ticker = list(dict.fromkeys(lvl1))[0]
            df = df.xs(first_ticker, level=1, axis=1)

    # Some environments still yield duplicated columns; keep first occurrence
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]

    # Ensure expected columns exist (Title-case variants)
    # yfinance uses "Adj Close" (space) sometimes.
    col_map = {c: str(c) for c in df.columns}
    df.rename(columns=col_map, inplace=True)

    return _coerce_utc_index(df)


def _resolve_signal_path_from_latest_meta(latest_meta_path: str, signals_dir: str) -> str:
    meta = _read_json(latest_meta_path)
    latest_file = meta.get("latest_file")
    if not latest_file:
        raise FileNotFoundError(f"latest_file not found in {latest_meta_path}")

    # Build candidate paths from meta
    candidates = []

    base = os.path.basename(latest_file)

    # 1) as-is
    candidates.append(latest_file)

    # 2) base only (repo root)
    candidates.append(base)

    # 3) under signals_dir
    candidates.append(os.path.join(signals_dir, base))

    # 4) if base is like "YYYY-MM-DD_close.json", add "signals_" prefix
    if base and not base.startswith("signals_") and "_" in base:
        candidates.append(os.path.join(signals_dir, f"signals_{base}"))
        candidates.append(f"signals_{base}")  # repo root variant

    # 5) if meta had "signals/..." but file actually "signals/signals_..."
    if base and base.startswith("signals_"):
        candidates.append(os.path.join(signals_dir, base))

    # 6) glob fallback using date/session parsed from meta
    # Try to parse "...YYYY-MM-DD_session..."
    stem = base.replace(".json", "")
    parts = stem.split("_")
    date_part = None
    session_part = None

    if len(parts) >= 3 and parts[0] == "signals":
        # signals_YYYY-MM-DD_close
        date_part, session_part = parts[1], parts[2]
    elif len(parts) >= 2:
        # YYYY-MM-DD_close
        date_part, session_part = parts[0], parts[1]

    if date_part and session_part:
        patt1 = os.path.join(signals_dir, f"signals_{date_part}_{session_part}.json")
        patt2 = os.path.join(signals_dir, f"*{date_part}*{session_part}*.json")
        for patt in (patt1, patt2):
            hits = glob.glob(patt)
            if hits:
                hits.sort(key=lambda p: os.path.getmtime(p), reverse=True)
                return hits[0]

    # 7) try candidates directly
    for p in candidates:
        if p and os.path.exists(p):
            return p

    # 8) FINAL SAFETY NET: pick most recent signals file
    all_signals = glob.glob(os.path.join(signals_dir, "*.json"))
    if all_signals:
        all_signals.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        return all_signals[0]

    raise FileNotFoundError(
        f"Could not resolve signal file from latest_meta. Tried: {candidates} "
        f"(latest_meta={latest_meta_path}) and no signals found in {signals_dir}/"
    )

def _resolve_signal_path_by_date(date_str: str, session: str, signals_dir: str) -> str:
    expected = os.path.join(signals_dir, f"signals_{date_str}_{session}.json")
    if os.path.exists(expected):
        return expected

    # fallback glob (buffer for any suffix variants)
    hits = glob.glob(os.path.join(signals_dir, f"signals_{date_str}_{session}*.json"))
    if hits:
        # choose newest
        hits.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        return hits[0]

    raise FileNotFoundError(f"Signal file not found: {expected}")


def _evaluate_long_trade_1h(df_1h: pd.DataFrame, entry: float, sl: float, tp: float) -> str:
    """
    Evaluate long trade using 1H bars.
    If within the same bar both SL and TP are touched, we assume SL first (conservative).
    Returns: 'WIN' | 'LOSS' | 'OPEN'
    """
    if df_1h is None or df_1h.empty:
        return "OPEN"

    for _, row in df_1h.iterrows():
        h = float(row["High"])
        l = float(row["Low"])

        hit_sl = l <= sl
        hit_tp = h >= tp

        if hit_sl and hit_tp:
            return "LOSS"  # conservative on ambiguous bar
        if hit_sl:
            return "LOSS"
        if hit_tp:
            return "WIN"

    return "OPEN"


def main():
    parser = argparse.ArgumentParser()
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--latest", action="store_true", help="Use outputs/today_candidates_latest_meta.json")
    g.add_argument("--date", help="YYYY-MM-DD (JST date)")

    parser.add_argument("--session", choices=["morning", "close"], help="Required when using --date")
    parser.add_argument("--signals_dir", default=DEFAULT_SIGNALS_DIR)
    parser.add_argument("--results_dir", default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--latest_meta", default=DEFAULT_LATEST_META)

    args = parser.parse_args()

    _ensure_dir(args.results_dir)

    # Resolve signal path
    if args.latest:
        signal_path = _resolve_signal_path_from_latest_meta(args.latest_meta, args.signals_dir)
    else:
        if not args.session:
            raise ValueError("--session is required when using --date")
        signal_path = _resolve_signal_path_by_date(args.date, args.session, args.signals_dir)

    signals = _read_json(signal_path)

    # signals schema (expected from main v3):
    # {
    #   "asof": "...",
    #   "session": "...",
    #   "candidates": [{ticker, entry, sl, tp, shares, ...}],
    #   "market_info": {...},
    #   "market_filter": "on/off" or bool,
    #   ...
    # }
    asof = signals.get("asof", "")
    asof_date = (asof[:10] if isinstance(asof, str) and len(asof) >= 10 else None)

    # Download start: asof date is good enough for 1H confirm-based eval
    # (If you want strict “after entry time” filtering, we can tighten later.)
    start = asof_date or args.date

    results = []
    sum_pnl = 0.0
    sum_r = 0.0
    n_resolved = 0
    n_win = 0
    n_loss = 0

    for c in signals.get("candidates", []):
        ticker = c["ticker"]
        entry = float(c["entry"])
        sl = float(c["sl"])
        tp = float(c["tp"])
        shares = int(c.get("shares", 0))

        # Pull 1H data
        df = yf.download(
            ticker,
            start=start,
            interval="1h",
            auto_adjust=False,
            progress=False,
        )
        df = _normalize_ohlcv(df, ticker)

        outcome = _evaluate_long_trade_1h(df, entry, sl, tp)

        if outcome == "WIN":
            r = (tp - entry) / (entry - sl) if (entry - sl) != 0 else 0.0
            pnl = (tp - entry) * shares
            n_win += 1
            n_resolved += 1
        elif outcome == "LOSS":
            r = -1.0
            pnl = (sl - entry) * shares
            n_loss += 1
            n_resolved += 1
        else:
            r = 0.0
            pnl = 0.0

        sum_r += r
        sum_pnl += pnl

        results.append(
            {
                "ticker": ticker,
                "result": outcome,
                "R": float(r),
                "pnl_yen": float(pnl),
                "entry": entry,
                "sl": sl,
                "tp": tp,
                "shares": shares,
            }
        )

    # Output naming: keep stable + avoid overwrite
    # result_YYYY-MM-DD_session_YYYYmmdd-HHMMSSZ.json
    # If signals file name contains date/session, use it.
    base = os.path.basename(signal_path).replace(".json", "")
    # signals_YYYY-MM-DD_session
    parts = base.split("_")
    date_part = None
    session_part = None
    if len(parts) >= 3 and parts[0] == "signals":
        date_part = parts[1]
        session_part = parts[2]

    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ")
    if date_part and session_part:
        out_name = f"result_{date_part}_{session_part}_{ts}.json"
        latest_name = f"result_{date_part}_{session_part}_latest.json"
    else:
        out_name = f"result_{ts}.json"
        latest_name = "result_latest.json"

    out_path = os.path.join(args.results_dir, out_name)
    latest_path = os.path.join(args.results_dir, latest_name)

    payload = {
        "meta": {
            "signal_path": signal_path,
            "asof": asof,
            "start": start,
            "assumption_same_bar": "LOSS_first_if_SL_and_TP_touched_in_same_1h_bar",
            "market_info": signals.get("market_info"),
            "market_filter": signals.get("market_filter"),
            "session": signals.get("session", session_part),
        },
        "summary": {
            "trades_total": len(results),
            "resolved": n_resolved,
            "wins": n_win,
            "losses": n_loss,
            "win_rate_resolved": (n_win / n_resolved * 100.0) if n_resolved else 0.0,
            "total_R": float(sum_r),
            "total_pnl_yen": float(sum_pnl),
        },
        "results": results,
    }

    _write_json(out_path, payload)
    _write_json(latest_path, payload)

    print("Saved:", out_path)
    print("Saved:", latest_path)


if __name__ == "__main__":
    main()