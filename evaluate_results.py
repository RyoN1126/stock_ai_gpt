import os
import json
import argparse
from datetime import datetime, timedelta, date as dt_date
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


def tz():
    return pytz.timezone(TZ_NAME)


def now_jst() -> datetime:
    return datetime.now(tz())


def parse_iso_to_dt(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = tz().localize(dt)
    return dt


def to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = tz().localize(dt)
    return dt.astimezone(pytz.UTC)


def safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def infer_session_from_filename(path: str) -> Optional[str]:
    base = os.path.basename(path)
    # YYYY-MM-DD_morning.json or YYYY-MM-DD_close.json (or with suffix)
    if "_morning" in base:
        return "morning"
    if "_close" in base:
        return "close"
    return None


def resolve_signal_path(
    input_path: Optional[str],
    signals_dir: str,
    use_latest_meta: bool,
) -> str:
    """
    Priority:
      1) --input
      2) --latest-meta -> outputs/today_candidates_latest_meta.json -> latest_file
      3) fallback latest json
    """
    if input_path:
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"--input not found: {input_path}")
        return input_path

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

    if os.path.exists(LATEST_JSON_PATH):
        return LATEST_JSON_PATH

    raise ValueError("Could not resolve signal input. Use --input or --latest-meta.")


def result_path_for_signal(signal_path: str, results_dir: str) -> str:
    base = os.path.basename(signal_path)
    if base.endswith(".json"):
        base = base[:-5]
    return os.path.join(results_dir, f"{base}_result.json")


def next_business_day_simple(d: dt_date) -> dt_date:
    """
    Simple next business day: skips Sat/Sun only.
    (JP holidays not covered; upgrade if needed.)
    """
    nd = d + timedelta(days=1)
    while nd.weekday() >= 5:  # 5=Sat,6=Sun
        nd += timedelta(days=1)
    return nd


def session_start_time(signal_time_jst: datetime, session: str) -> datetime:
    """
    Align evaluation start time with your operation:
      - morning signal => evaluate from same-day 12:30 JST (afternoon session)
      - close signal   => evaluate from next business day 09:00 JST (next day morning)
    """
    sig_date = signal_time_jst.astimezone(tz()).date()

    if session == "morning":
        return tz().localize(datetime(sig_date.year, sig_date.month, sig_date.day, 12, 30, 0))
    if session == "close":
        nd = next_business_day_simple(sig_date)
        return tz().localize(datetime(nd.year, nd.month, nd.day, 9, 0, 0))

    # fallback
    return signal_time_jst


def _yf_download_intraday(
    ticker: str,
    start_utc: datetime,
    end_utc: datetime,
    interval: str,
) -> pd.DataFrame:
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

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    idx = pd.to_datetime(df.index, errors="coerce", utc=True)
    df = df.loc[~idx.isna()].copy()
    df.index = idx[~idx.isna()]
    return df


def fetch_forward_bars(
    ticker: str,
    start_time_jst: datetime,
    max_forward_days: int,
) -> Tuple[pd.DataFrame, str]:
    end_utc = to_utc(now_jst())
    start_utc = to_utc(start_time_jst - timedelta(hours=6))

    max_end_utc = to_utc(start_time_jst + timedelta(days=max_forward_days))
    if end_utc > max_end_utc:
        end_utc = max_end_utc

    age_days = (now_jst() - start_time_jst.astimezone(tz())).total_seconds() / 86400.0

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
        return pd.DataFrame(), f"failed({type(last_err).__name__})"
    return pd.DataFrame(), "empty"


def evaluate_one(
    ticker: str,
    entry: float,
    sl: float,
    tp: float,
    start_time_jst: datetime,
    max_forward_days: int,
) -> Dict[str, Any]:
    """
    Determine which was touched first after start_time_jst:
      - WIN if High >= TP occurs before Low <= SL
      - LOSS if Low <= SL occurs before High >= TP
      - OPEN if neither touched in available data window
      - AMBIG if both touched in the same bar
    R:
      WIN  -> (tp-entry)/(entry-sl)
      LOSS -> -1
      OPEN/AMBIG -> 0
    """
    df, used_interval = fetch_forward_bars(ticker, start_time_jst, max_forward_days)

    item: Dict[str, Any] = {
        "ticker": ticker,
        "entry": float(entry),
        "sl": float(sl),
        "tp": float(tp),
        "status": "OPEN",
        "r": 0.0,
        "used_interval": used_interval,
        "eval_start_time": start_time_jst.isoformat(),
        "first_hit_time": None,
        "note": None,
    }

    risk = entry - sl
    if risk <= 0:
        item["note"] = "invalid_risk(entry<=sl)"
        return item

    if df is None or len(df) == 0:
        item["note"] = "no_price_data"
        return item

    start_utc = to_utc(start_time_jst)
    fwd = df[df.index > start_utc].copy()
    if len(fwd) == 0:
        item["note"] = "no_forward_bars"
        return item

    hit_tp = (fwd["High"] >= tp)
    hit_sl = (fwd["Low"] <= sl)

    tp_idx = hit_tp.idxmax() if hit_tp.any() else None
    sl_idx = hit_sl.idxmax() if hit_sl.any() else None

    if tp_idx is None and sl_idx is None:
        return item

    if tp_idx is not None and sl_idx is None:
        item["status"] = "WIN"
        item["r"] = round((tp - entry) / risk, 4)
        item["first_hit_time"] = tp_idx.astimezone(tz()).isoformat()
        return item

    if sl_idx is not None and tp_idx is None:
        item["status"] = "LOSS"
        item["r"] = -1.0
        item["first_hit_time"] = sl_idx.astimezone(tz()).isoformat()
        return item

    # both exist
    if tp_idx < sl_idx:
        item["status"] = "WIN"
        item["r"] = round((tp - entry) / risk, 4)
        item["first_hit_time"] = tp_idx.astimezone(tz()).isoformat()
        return item
    if sl_idx < tp_idx:
        item["status"] = "LOSS"
        item["r"] = -1.0
        item["first_hit_time"] = sl_idx.astimezone(tz()).isoformat()
        return item

    item["status"] = "AMBIG"
    item["note"] = "tp_and_sl_hit_same_bar"
    item["first_hit_time"] = tp_idx.astimezone(tz()).isoformat()
    return item


def summarize(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(items)
    win = sum(1 for x in items if x["status"] == "WIN")
    loss = sum(1 for x in items if x["status"] == "LOSS")
    open_ = sum(1 for x in items if x["status"] == "OPEN")
    ambig = sum(1 for x in items if x["status"] == "AMBIG")
    total_r = round(sum(float(x.get("r", 0.0) or 0.0) for x in items), 4)

    denom = win + loss
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


def parse_args():
    p = argparse.ArgumentParser(description="Evaluate WIN/LOSS/OPEN for a signal file and save results (session-aware).")
    p.add_argument("--input", type=str, default=None, help="Signal json path (recommended)")
    p.add_argument("--latest-meta", action="store_true", help="Use outputs/today_candidates_latest_meta.json")
    p.add_argument("--signals-dir", type=str, default=DEFAULT_SIGNALS_DIR, help="Signals directory")
    p.add_argument("--results-dir", type=str, default=DEFAULT_RESULTS_DIR, help="Results directory")

    p.add_argument("--max-forward-days", type=int, default=14, help="Evaluate within N days from eval start time")
    p.add_argument("--force", action="store_true", help="Overwrite existing result file")

    # Advanced: override behavior
    p.add_argument("--start-mode", type=str, default="session", choices=["session", "signal"],
                   help="session: start from (morning=12:30, close=next 09:00). signal: start right after signal time.")
    return p.parse_args()


def main():
    args = parse_args()
    safe_mkdir(args.results_dir)

    signal_path = resolve_signal_path(
        input_path=args.input,
        signals_dir=args.signals_dir,
        use_latest_meta=bool(args.latest_meta),
    )

    signal = load_json(signal_path)

    asof = signal.get("asof")
    if not asof:
        raise ValueError("Signal json missing 'asof'")
    signal_time = parse_iso_to_dt(asof).astimezone(tz())

    session = signal.get("session") or infer_session_from_filename(signal_path) or "unknown"

    if args.start_mode == "signal":
        eval_start = signal_time
    else:
        eval_start = session_start_time(signal_time, session)

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
            continue

        item = evaluate_one(
            ticker=ticker,
            entry=entry,
            sl=sl,
            tp=tp,
            start_time_jst=eval_start,
            max_forward_days=int(args.max_forward_days),
        )
        items.append(item)

    out = {
        "signal_file": os.path.relpath(signal_path).replace("\\", "/"),
        "signal_asof": signal_time.isoformat(),
        "session": session,
        "start_mode": args.start_mode,
        "eval_start_time": eval_start.isoformat(),
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