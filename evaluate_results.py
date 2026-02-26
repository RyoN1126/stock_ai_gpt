import os
import json
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
import yfinance as yf
import pytz


TZ_NAME = "Asia/Tokyo"
OUTPUT_DIR = "outputs"
DEFAULT_SIGNALS_DIR = os.path.join(OUTPUT_DIR, "signals")
DEFAULT_RESULTS_DIR = os.path.join(OUTPUT_DIR, "results")
LATEST_META_PATH = os.path.join(OUTPUT_DIR, "today_candidates_latest_meta.json")
LATEST_JSON_PATH = os.path.join(OUTPUT_DIR, "today_candidates_latest.json")


# -------------------------
# Time helpers
# -------------------------
def now_jst() -> datetime:
    return datetime.now(pytz.timezone(TZ_NAME))


def parse_iso_to_dt(s: str) -> datetime:
    """
    Parse ISO8601 string to aware datetime.
    Accepts "2026-02-26T12:10:00+09:00" etc.
    """
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = pytz.timezone(TZ_NAME).localize(dt)
    return dt


def to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = pytz.timezone(TZ_NAME).localize(dt)
    return dt.astimezone(pytz.UTC)


def jst_label(dt: datetime) -> str:
    dtj = dt.astimezone(pytz.timezone(TZ_NAME))
    return dtj.strftime("%Y-%m-%d_%H%M")


# -------------------------
# IO helpers
# -------------------------
def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def resolve_signal_path(
    input_path: Optional[str],
    date: Optional[str],
    slot: Optional[str],
    signals_dir: str,
    use_latest_meta: bool,
) -> str:
    """
    Priority:
      1) --input
      2) --date + --slot  -> outputs/signals/YYYY-MM-DD_HHMM.json
      3) --latest-meta    -> outputs/today_candidates_latest_meta.json -> latest_file
      4) fallback latest json (NOT recommended but supported)
    """
    if input_path:
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"--input not found: {input_path}")
        return input_path

    if date and slot:
        slot_norm = slot.replace(":", "")
        fname = f"{date}_{slot_norm}.json"
        path = os.path.join(signals_dir, fname)
        if not os.path.exists(path):
            raise FileNotFoundError(f"signal not found for --date/--slot: {path}")
        return path

    if use_latest_meta:
        if not os.path.exists(LATEST_META_PATH):
            raise FileNotFoundError(f"latest meta not found: {LATEST_META_PATH}")
        meta = load_json(LATEST_META_PATH)
        rel = meta.get("latest_file")
        if not rel:
            raise ValueError("latest meta missing 'latest_file'")
        path = os.path.join(OUTPUT_DIR, rel)
        if not os.path.exists(path):
            raise FileNotFoundError(f"latest meta points to missing file: {path}")
        return path

    # fallback (old behavior)
    if os.path.exists(LATEST_JSON_PATH):
        return LATEST_JSON_PATH

    raise ValueError("Could not resolve signal input. Use --input or --date/--slot or --latest-meta.")


def result_path_for_signal(signal_path: str, results_dir: str) -> str:
    base = os.path.basename(signal_path)
    if base.endswith(".json"):
        base = base[:-5]
    return os.path.join(results_dir, f"{base}_result.json")


# -------------------------
# Price fetch + evaluation
# -------------------------
def _yf_download_intraday(
    ticker: str,
    start_utc: datetime,
    end_utc: datetime,
    interval: str,
) -> pd.DataFrame:
    """
    Use yfinance.download with start/end. Returns DataFrame with OHLCV.
    """
    df = yf.download(
        tickers=ticker,
        start=start_utc,
        end=end_utc,
        interval=interval,
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    if df is None or len(df) == 0:
        return pd.DataFrame()

    # MultiIndex columns safety
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Normalize index to UTC-aware
    idx = pd.to_datetime(df.index, errors="coerce", utc=True)
    df = df.loc[~idx.isna()].copy()
    df.index = idx[~idx.isna()]
    return df


def fetch_forward_bars(
    ticker: str,
    signal_time_jst: datetime,
    max_forward_days: int,
) -> Tuple[pd.DataFrame, str]:
    """
    Fetch bars from shortly before signal_time to now, with best-effort intervals.
    Returns (df, used_interval).
    """
    end_utc = to_utc(now_jst())
    start_utc = to_utc(signal_time_jst - timedelta(hours=6))
    # clamp lookahead window
    max_end_utc = to_utc(signal_time_jst + timedelta(days=max_forward_days))
    if end_utc > max_end_utc:
        end_utc = max_end_utc

    # Try more granular first; yfinance has limits (1m usually ~7 days)
    age_days = (now_jst().astimezone(pytz.timezone(TZ_NAME)) - signal_time_jst.astimezone(pytz.timezone(TZ_NAME))).total_seconds() / 86400.0

    intervals: List[str]
    if age_days <= 7.0:
        intervals = ["1m", "2m", "5m", "15m", "60m", "1d"]
    elif age_days <= 60.0:
        intervals = ["5m", "15m", "60m", "1d"]
    else:
        intervals = ["60m", "1d"]

    last_err = None
    for itv in intervals:
        try:
            df = _yf_download_intraday(ticker, start_utc, end_utc, itv)
            if len(df) > 0 and set(["Open", "High", "Low", "Close"]).issubset(df.columns):
                return df, itv
        except Exception as e:
            last_err = e

    if last_err:
        # still return empty with info
        return pd.DataFrame(), f"failed({type(last_err).__name__})"
    return pd.DataFrame(), "empty"


def evaluate_one(
    ticker: str,
    entry: float,
    sl: float,
    tp: float,
    signal_time_jst: datetime,
    max_forward_days: int,
) -> Dict[str, Any]:
    """
    Determine which was touched first after signal_time:
      - WIN if High >= TP occurs before Low <= SL
      - LOSS if Low <= SL occurs before High >= TP
      - OPEN if neither touched in available data window
      - AMBIG if both touched in the same bar (cannot know order)
    R:
      WIN  -> (tp-entry)/(entry-sl)
      LOSS -> -1
      OPEN/AMBIG -> 0
    """
    df, used_interval = fetch_forward_bars(ticker, signal_time_jst, max_forward_days)

    item: Dict[str, Any] = {
        "ticker": ticker,
        "entry": float(entry),
        "sl": float(sl),
        "tp": float(tp),
        "status": "OPEN",
        "r": 0.0,
        "used_interval": used_interval,
        "first_hit_time": None,
        "note": None,
    }

    risk = entry - sl
    if risk <= 0:
        item["status"] = "OPEN"
        item["note"] = "invalid_risk(entry<=sl)"
        return item

    if df is None or len(df) == 0:
        item["note"] = "no_price_data"
        return item

    # focus only bars AFTER signal time
    signal_utc = to_utc(signal_time_jst)
    fwd = df[df.index > signal_utc].copy()
    if len(fwd) == 0:
        item["note"] = "no_forward_bars"
        return item

    # Find first indices where TP/SL touched
    hit_tp = (fwd["High"] >= tp)
    hit_sl = (fwd["Low"] <= sl)

    tp_idx = hit_tp.idxmax() if hit_tp.any() else None
    sl_idx = hit_sl.idxmax() if hit_sl.any() else None

    # idxmax() returns first index of max(True=1) if any True
    if tp_idx is None and sl_idx is None:
        item["status"] = "OPEN"
        return item

    if tp_idx is not None and sl_idx is None:
        item["status"] = "WIN"
        item["r"] = round((tp - entry) / risk, 4)
        item["first_hit_time"] = tp_idx.astimezone(pytz.timezone(TZ_NAME)).isoformat()
        return item

    if sl_idx is not None and tp_idx is None:
        item["status"] = "LOSS"
        item["r"] = -1.0
        item["first_hit_time"] = sl_idx.astimezone(pytz.timezone(TZ_NAME)).isoformat()
        return item

    # Both exist
    if tp_idx < sl_idx:
        item["status"] = "WIN"
        item["r"] = round((tp - entry) / risk, 4)
        item["first_hit_time"] = tp_idx.astimezone(pytz.timezone(TZ_NAME)).isoformat()
        return item
    elif sl_idx < tp_idx:
        item["status"] = "LOSS"
        item["r"] = -1.0
        item["first_hit_time"] = sl_idx.astimezone(pytz.timezone(TZ_NAME)).isoformat()
        return item
    else:
        # same bar
        item["status"] = "AMBIG"
        item["r"] = 0.0
        item["first_hit_time"] = tp_idx.astimezone(pytz.timezone(TZ_NAME)).isoformat()
        item["note"] = "tp_and_sl_hit_same_bar"
        return item


# -------------------------
# Aggregation (single file)
# -------------------------
def summarize(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(items)
    win = sum(1 for x in items if x["status"] == "WIN")
    loss = sum(1 for x in items if x["status"] == "LOSS")
    open_ = sum(1 for x in items if x["status"] == "OPEN")
    ambig = sum(1 for x in items if x["status"] == "AMBIG")
    total_r = round(sum(float(x.get("r", 0.0)) for x in items), 4)

    denom = (win + loss)
    win_rate = round(win / denom, 4) if denom > 0 else None

    return {
        "n": n,
        "win": win,
        "loss": loss,
        "open": open_,
        "ambig": ambig,
        "total_r": total_r,
        "win_rate": win_rate,
    }


# -------------------------
# CLI
# -------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Evaluate WIN/LOSS/OPEN for a signal file and save results.")
    g = p.add_mutually_exclusive_group(required=False)
    g.add_argument("--input", type=str, default=None, help="Signal json path (recommended)")
    g.add_argument("--latest-meta", action="store_true", help="Use outputs/today_candidates_latest_meta.json")
    p.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (used with --slot)")
    p.add_argument("--slot", type=str, default=None, help="HHMM or HH:MM (used with --date)")

    p.add_argument("--signals-dir", type=str, default=DEFAULT_SIGNALS_DIR, help="Signals directory")
    p.add_argument("--results-dir", type=str, default=DEFAULT_RESULTS_DIR, help="Results directory")
    p.add_argument("--max-forward-days", type=int, default=14, help="Evaluate within N days from signal time")
    p.add_argument("--force", action="store_true", help="Overwrite existing result file")

    return p.parse_args()


def main():
    args = parse_args()
    safe_mkdir(args.results_dir)

    signal_path = resolve_signal_path(
        input_path=args.input,
        date=args.date,
        slot=args.slot,
        signals_dir=args.signals_dir,
        use_latest_meta=bool(args.latest_meta),
    )

    signal = load_json(signal_path)

    # read signal time
    asof = signal.get("asof")
    if not asof:
        raise ValueError("Signal json missing 'asof'")
    signal_time = parse_iso_to_dt(asof).astimezone(pytz.timezone(TZ_NAME))

    candidates = signal.get("candidates", [])
    if not isinstance(candidates, list):
        raise ValueError("Signal json 'candidates' must be a list")

    items: List[Dict[str, Any]] = []
    for c in candidates:
        try:
            ticker = c["ticker"]
            entry = float(c["entry"])
            sl = float(c["sl"])
            tp = float(c["tp"])
        except Exception:
            # skip malformed entry
            continue

        item = evaluate_one(
            ticker=ticker,
            entry=entry,
            sl=sl,
            tp=tp,
            signal_time_jst=signal_time,
            max_forward_days=int(args.max_forward_days),
        )
        items.append(item)

    out = {
        "signal_file": os.path.relpath(signal_path).replace("\\", "/"),
        "signal_asof": signal_time.isoformat(),
        "evaluated_at": now_jst().isoformat(),
        "tool": "evaluate_results",
        "items": items,
        "summary": summarize(items),
    }

    out_path = result_path_for_signal(signal_path, args.results_dir)
    if os.path.exists(out_path) and not args.force:
        raise FileExistsError(f"Result exists (use --force to overwrite): {out_path}")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)

    print(f"SAVED: {out_path}")
    print(json.dumps(out["summary"], ensure_ascii=False))


if __name__ == "__main__":
    main()