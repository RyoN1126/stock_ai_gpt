import os
import re
import json
import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

import pytz
import pandas as pd
import yfinance as yf


TZ_NAME = "Asia/Tokyo"

DEFAULT_OUTPUT_DIR = "outputs"
DEFAULT_SIGNALS_DIR = os.path.join(DEFAULT_OUTPUT_DIR, "signals")
DEFAULT_RESULTS_DIR = os.path.join(DEFAULT_OUTPUT_DIR, "results")

# If multiple intervals are attempted, earlier = higher resolution.
INTERVAL_CANDIDATES = ["1m", "2m", "5m", "15m", "60m", "1d"]

# Try to include some buffer before eval_start to avoid missing the first bar.
START_BUFFER_HOURS = 6


def tz_jst():
    return pytz.timezone(TZ_NAME)


def now_jst() -> datetime:
    return datetime.now(tz_jst())


def parse_iso_dt(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = tz_jst().localize(dt)
    return dt.astimezone(tz_jst())


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def is_weekend(d: date) -> bool:
    return d.weekday() >= 5  # Sat=5, Sun=6


def next_business_day(d: date) -> date:
    x = d + timedelta(days=1)
    while is_weekend(x):
        x += timedelta(days=1)
    return x


def floor_to_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


@dataclass(frozen=True)
class SignalMeta:
    asof_jst: datetime
    session: str  # "morning" or "close"
    source_file: str


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: Dict[str, Any]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def detect_session_from_filename(path: str) -> Optional[str]:
    fn = os.path.basename(path)
    m = re.search(r"_(morning|close)\.json$", fn)
    if m:
        return m.group(1)
    return None


def parse_signal_meta(signal_path: str, data: Dict[str, Any]) -> SignalMeta:
    meta = data.get("meta") if isinstance(data, dict) else None
    asof_str = None
    session = None

    if isinstance(meta, dict):
        asof_str = meta.get("asof") or meta.get("asof_jst") or meta.get("generated_at") or meta.get("timestamp")
        session = meta.get("session")

    if not session:
        session = detect_session_from_filename(signal_path) or "close"

    if not asof_str:
        # fallback: file mtime
        mtime = os.path.getmtime(signal_path)
        asof = datetime.fromtimestamp(mtime, tz=tz_jst())
    else:
        asof = parse_iso_dt(asof_str)

    session = session.lower().strip()
    if session not in ("morning", "close"):
        session = "close"

    return SignalMeta(asof_jst=asof, session=session, source_file=signal_path)


def extract_candidates(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Accepts either:
      - {"candidates":[...]} (preferred)
      - {"items":[...]} (legacy)
    Each candidate should contain: ticker, entry, sl, tp
    """
    if not isinstance(data, dict):
        return []

    if isinstance(data.get("candidates"), list):
        return data["candidates"]
    if isinstance(data.get("items"), list):
        return data["items"]
    # fallback: sometimes nested
    payload = data.get("payload")
    if isinstance(payload, dict) and isinstance(payload.get("candidates"), list):
        return payload["candidates"]
    return []


def compute_eval_start(signal: SignalMeta) -> datetime:
    """
    morning: same date 12:30 JST
    close: next business day 09:00 JST
    """
    d = signal.asof_jst.date()
    if signal.session == "morning":
        start = tz_jst().localize(datetime(d.year, d.month, d.day, 12, 30, 0))
    else:
        nd = next_business_day(d)
        start = tz_jst().localize(datetime(nd.year, nd.month, nd.day, 9, 0, 0))
    return start


def summarize(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(items)
    win = sum(1 for x in items if x.get("status") == "WIN")
    loss = sum(1 for x in items if x.get("status") == "LOSS")
    open_ = sum(1 for x in items if x.get("status") == "OPEN")
    ambig = sum(1 for x in items if x.get("status") == "AMBIG")
    total_r = float(sum(float(x.get("r", 0.0) or 0.0) for x in items))
    resolved = win + loss
    win_rate = (win / resolved) if resolved > 0 else None
    return {
        "n": n,
        "win": win,
        "loss": loss,
        "open": open_,
        "ambig": ambig,
        "total_r": round(total_r, 4),
        "win_rate": round(win_rate, 4) if win_rate is not None else None,
    }


def rr(entry: float, sl: float, tp: float) -> Optional[float]:
    risk = entry - sl
    if risk <= 0:
        return None
    return (tp - entry) / risk


def _download_ohlc(
    ticker: str,
    start_jst: datetime,
    end_jst: datetime,
    interval: str,
) -> pd.DataFrame:
    """
    Download OHLCV from yfinance, using UTC timestamps.
    """
    start_utc = start_jst.astimezone(pytz.UTC)
    end_utc = end_jst.astimezone(pytz.UTC)
    # yfinance expects naive or UTC; passing timezone-aware works in most cases,
    # but we'll pass ISO strings to reduce surprises.
    df = yf.download(
        tickers=ticker,
        start=start_utc.isoformat(),
        end=end_utc.isoformat(),
        interval=interval,
        progress=False,
        auto_adjust=False,
        group_by="column",
        threads=False,
    )
    if df is None or df.empty:
        return pd.DataFrame()

    # If multiindex columns happen, flatten to standard columns
    if isinstance(df.columns, pd.MultiIndex):
        # take first level where matches
        df.columns = [c[0] for c in df.columns]

    # Ensure required columns exist
    for col in ["Open", "High", "Low", "Close"]:
        if col not in df.columns:
            return pd.DataFrame()

    # Normalize index to JST
    if df.index.tz is None:
        # yfinance often returns UTC-naive
        df.index = df.index.tz_localize("UTC").tz_convert(TZ_NAME)
    else:
        df.index = df.index.tz_convert(TZ_NAME)

    return df


def evaluate_one(
    ticker: str,
    entry: float,
    sl: float,
    tp: float,
    eval_start_jst: datetime,
    eval_end_jst: datetime,
) -> Dict[str, Any]:
    # --- GUARD 1: invalid levels ---
    if not (entry and sl and tp) or entry <= 0 or sl <= 0 or tp <= 0:
        return {
            "ticker": ticker,
            "entry": float(entry),
            "sl": float(sl),
            "tp": float(tp),
            "status": "OPEN",
            "r": 0.0,
            "used_interval": "n/a",
            "eval_start_time": eval_start_jst.isoformat(),
            "first_hit_time": None,
            "note": "invalid_levels",
        }

    # --- GUARD 2: future eval_start (THIS is the “clean” fix) ---
    if eval_start_jst > now_jst():
        return {
            "ticker": ticker,
            "entry": float(entry),
            "sl": float(sl),
            "tp": float(tp),
            "status": "OPEN",
            "r": 0.0,
            "used_interval": "n/a",
            "eval_start_time": eval_start_jst.isoformat(),
            "first_hit_time": None,
            "note": "eval_start_in_future",
        }

    # Add buffer to avoid missing first bar around eval_start
    dl_start = eval_start_jst - timedelta(hours=START_BUFFER_HOURS)
    dl_start = floor_to_minute(dl_start)
    dl_end = floor_to_minute(eval_end_jst)

    if dl_start >= dl_end:
        # Should not happen after future guard, but keep safe
        return {
            "ticker": ticker,
            "entry": float(entry),
            "sl": float(sl),
            "tp": float(tp),
            "status": "OPEN",
            "r": 0.0,
            "used_interval": "n/a",
            "eval_start_time": eval_start_jst.isoformat(),
            "first_hit_time": None,
            "note": "start_after_end",
        }

    # Evaluate: find first time price hits TP or SL after eval_start.
    first_tp: Optional[datetime] = None
    first_sl: Optional[datetime] = None
    used_interval = None
    note = None

    for interval in INTERVAL_CANDIDATES:
        try:
            df = _download_ohlc(ticker, dl_start, dl_end, interval=interval)
        except Exception as e:
            df = pd.DataFrame()
            note = f"download_error:{type(e).__name__}"

        if df.empty:
            continue

        used_interval = interval

        # Only consider bars at/after eval_start
        df2 = df[df.index >= eval_start_jst]
        if df2.empty:
            # No forward bars at this interval; try coarser
            continue

        # Find first bar where TP/SL hit
        hit_tp = df2[df2["High"] >= tp]
        hit_sl = df2[df2["Low"] <= sl]

        if not hit_tp.empty:
            first_tp = hit_tp.index[0].to_pydatetime()
        if not hit_sl.empty:
            first_sl = hit_sl.index[0].to_pydatetime()

        # If neither hit in this interval, we can still accept OPEN
        # But coarser intervals won't reveal hits if finer didn't, so we can break.
        break

    # Decide status
    status = "OPEN"
    first_hit_time = None

    if first_tp and first_sl:
        # If both hit, decide which came first.
        if first_tp < first_sl:
            status = "WIN"
            first_hit_time = first_tp
        elif first_sl < first_tp:
            status = "LOSS"
            first_hit_time = first_sl
        else:
            # Same timestamp (same bar) => ambiguous
            status = "AMBIG"
            first_hit_time = first_tp
    elif first_tp:
        status = "WIN"
        first_hit_time = first_tp
    elif first_sl:
        status = "LOSS"
        first_hit_time = first_sl
    else:
        status = "OPEN"

    # Compute R
    r_value = 0.0
    rr_value = rr(entry, sl, tp)
    if rr_value is None:
        # invalid risk definition -> keep OPEN with note
        status = "OPEN" if status in ("WIN", "LOSS", "AMBIG") else status
        note = (note + ";" if note else "") + "invalid_risk"
        r_value = 0.0
    else:
        if status == "WIN":
            r_value = float(rr_value)
        elif status == "LOSS":
            r_value = -1.0
        else:
            r_value = 0.0

    return {
        "ticker": ticker,
        "entry": float(entry),
        "sl": float(sl),
        "tp": float(tp),
        "status": status,
        "r": round(float(r_value), 4),
        "used_interval": used_interval or "n/a",
        "eval_start_time": eval_start_jst.isoformat(),
        "first_hit_time": first_hit_time.isoformat() if first_hit_time else None,
        "note": note,
    }


def find_latest_signal(signals_dir: str) -> Optional[str]:
    if not os.path.isdir(signals_dir):
        return None
    cands = []
    for fn in os.listdir(signals_dir):
        if not fn.endswith(".json"):
            continue
        # avoid meta/system files if any
        if fn.startswith("_"):
            continue
        path = os.path.join(signals_dir, fn)
        if os.path.isfile(path):
            cands.append(path)
    if not cands:
        return None
    cands.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return cands[0]


def default_result_name(signal_meta: SignalMeta) -> str:
    d = signal_meta.asof_jst.date().isoformat()
    return f"{d}_{signal_meta.session}_result.json"


def unique_path_if_exists(dir_path: str, filename: str) -> str:
    base = os.path.join(dir_path, filename)
    if not os.path.exists(base):
        return base
    # add time suffix
    ts = now_jst().strftime("%H%M%S")
    root, ext = os.path.splitext(filename)
    return os.path.join(dir_path, f"{root}_{ts}{ext}")


def parse_args():
    p = argparse.ArgumentParser(description="Evaluate signal JSON (TP/SL first-hit) and save results JSON.")
    g = p.add_mutually_exclusive_group(required=False)
    g.add_argument("--input", type=str, help="Path to a specific signal JSON file.")
    g.add_argument("--latest", action="store_true", help="Use the latest signal JSON in outputs/signals/.")

    p.add_argument("--signals-dir", type=str, default=DEFAULT_SIGNALS_DIR, help="Signals directory (default: outputs/signals).")
    p.add_argument("--results-dir", type=str, default=DEFAULT_RESULTS_DIR, help="Results directory (default: outputs/results).")

    p.add_argument("--end", type=str, default=None, help="Evaluation end time (ISO8601, JST). Default: now.")
    p.add_argument("--quiet", action="store_true", help="Reduce prints (still prints SAVE path).")

    return p.parse_args()


def main():
    args = parse_args()

    if args.input:
        signal_path = args.input
    elif args.latest:
        signal_path = find_latest_signal(args.signals_dir)
    else:
        # default to latest for convenience
        signal_path = find_latest_signal(args.signals_dir)

    if not signal_path or not os.path.exists(signal_path):
        print(f"No signal file found. (signals-dir={args.signals_dir})")
        return

    data = load_json(signal_path)
    meta = parse_signal_meta(signal_path, data)
    candidates = extract_candidates(data)

    eval_start = compute_eval_start(meta)
    eval_end = parse_iso_dt(args.end) if args.end else now_jst()

    # If eval_end is before eval_start (e.g., close signal evaluated too early),
    # our per-ticker future guard will handle it, but keep eval_end sane:
    if eval_end < meta.asof_jst:
        eval_end = now_jst()

    if not args.quiet:
        print("=== EVALUATE ===")
        print(f"signal_file: {signal_path}")
        print(f"session: {meta.session}")
        print(f"signal_asof(JST): {meta.asof_jst.isoformat()}")
        print(f"eval_start(JST): {eval_start.isoformat()}")
        print(f"eval_end(JST):   {eval_end.isoformat()}")
        print(f"candidates: {len(candidates)}")

    items: List[Dict[str, Any]] = []

    for c in candidates:
        try:
            ticker = str(c.get("ticker") or c.get("code") or "").strip()
            entry = float(c.get("entry"))
            sl = float(c.get("sl"))
            tp = float(c.get("tp"))
        except Exception:
            # malformed row -> OPEN
            ticker = str(c.get("ticker") or "").strip() or "UNKNOWN"
            entry = float(c.get("entry") or 0)
            sl = float(c.get("sl") or 0)
            tp = float(c.get("tp") or 0)

        if not ticker:
            continue

        res = evaluate_one(
            ticker=ticker,
            entry=entry,
            sl=sl,
            tp=tp,
            eval_start_jst=eval_start,
            eval_end_jst=eval_end,
        )
        items.append(res)

        if not args.quiet and res.get("note") == "eval_start_in_future":
            # one-line, no noisy yahoo errors
            print(f"[SKIP] {ticker}: eval_start_in_future -> OPEN")

    out = {
        "meta": {
            "evaluated_at": now_jst().isoformat(),
            "timezone": TZ_NAME,
            "signal_file": os.path.relpath(signal_path).replace("\\", "/"),
            "signal_asof": meta.asof_jst.isoformat(),
            "session": meta.session,
            "eval_start": eval_start.isoformat(),
            "eval_end": eval_end.isoformat(),
        },
        "summary": summarize(items),
        "items": items,
    }

    ensure_dir(args.results_dir)
    out_name = default_result_name(meta)
    out_path = unique_path_if_exists(args.results_dir, out_name)
    save_json(out_path, out)

    print(f"SAVED: {out_path}")
    print(json.dumps(out["summary"], ensure_ascii=False))


if __name__ == "__main__":
    main()