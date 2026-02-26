import os
import re
import json
import argparse
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional, Tuple

import pytz

TZ_NAME = "Asia/Tokyo"
OUTPUT_DIR = "outputs"
DEFAULT_RESULTS_DIR = os.path.join(OUTPUT_DIR, "results")
DEFAULT_AGG_DIR = os.path.join(OUTPUT_DIR, "aggregates")

# filename pattern: YYYY-MM-DD_HHMM_result.json
RESULT_RE = re.compile(r"^(?P<d>\d{4}-\d{2}-\d{2})_(?P<slot>\d{4})_result\.json$")


def now_jst() -> datetime:
    return datetime.now(pytz.timezone(TZ_NAME))


def parse_iso_dt(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = pytz.timezone(TZ_NAME).localize(dt)
    return dt


def safe_mkdir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


@dataclass(frozen=True)
class ResultFile:
    path: str
    day: date
    slot: str  # "1030", "1210"
    signal_dt_jst: datetime
    evaluated_at_jst: Optional[datetime]


def parse_result_filename(fn: str) -> Optional[Tuple[date, str]]:
    m = RESULT_RE.match(fn)
    if not m:
        return None
    d = datetime.strptime(m.group("d"), "%Y-%m-%d").date()
    slot = m.group("slot")
    return d, slot


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def list_result_files(results_dir: str) -> List[ResultFile]:
    tz = pytz.timezone(TZ_NAME)
    out: List[ResultFile] = []
    if not os.path.isdir(results_dir):
        return out

    for fn in os.listdir(results_dir):
        parsed = parse_result_filename(fn)
        if not parsed:
            continue
        d, slot = parsed
        # signal time from filename (JST)
        hh = int(slot[:2])
        mm = int(slot[2:])
        sig_dt = tz.localize(datetime(d.year, d.month, d.day, hh, mm, 0))

        path = os.path.join(results_dir, fn)

        evaluated = None
        try:
            data = load_json(path)
            if isinstance(data, dict) and data.get("evaluated_at"):
                evaluated = parse_iso_dt(data["evaluated_at"]).astimezone(tz)
        except Exception:
            evaluated = None

        out.append(ResultFile(path=path, day=d, slot=slot, signal_dt_jst=sig_dt, evaluated_at_jst=evaluated))

    out.sort(key=lambda x: (x.signal_dt_jst, x.path))
    return out


def compute_items_summary(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(items)
    win = sum(1 for x in items if x.get("status") == "WIN")
    loss = sum(1 for x in items if x.get("status") == "LOSS")
    open_ = sum(1 for x in items if x.get("status") == "OPEN")
    ambig = sum(1 for x in items if x.get("status") == "AMBIG")

    total_r = round(sum(float(x.get("r", 0.0) or 0.0) for x in items), 4)

    denom = win + loss
    win_rate = round(win / denom, 4) if denom > 0 else None

    # average R over resolved only (WIN/LOSS)
    resolved_rs = [float(x.get("r", 0.0) or 0.0) for x in items if x.get("status") in ("WIN", "LOSS")]
    avg_r_resolved = round(sum(resolved_rs) / len(resolved_rs), 4) if resolved_rs else None

    # average R over all signals (includes OPEN/AMBIG as 0)
    avg_r_all = round(total_r / n, 4) if n > 0 else None

    open_ratio = round(open_ / n, 4) if n > 0 else None

    return {
        "n": n,
        "win": win,
        "loss": loss,
        "open": open_,
        "ambig": ambig,
        "resolved_n": denom,
        "total_r": total_r,
        "win_rate": win_rate,
        "avg_r_resolved": avg_r_resolved,
        "avg_r_all": avg_r_all,
        "open_ratio": open_ratio,
    }


def max_losing_streak(resolved_outcomes: List[str]) -> int:
    """
    resolved_outcomes: chronological list of "WIN"/"LOSS" only
    """
    mx = 0
    cur = 0
    for o in resolved_outcomes:
        if o == "LOSS":
            cur += 1
            mx = max(mx, cur)
        elif o == "WIN":
            cur = 0
    return mx


def filter_by_week(files: List[ResultFile], week_str: str) -> List[ResultFile]:
    # week_str like "2026-W09"
    m = re.match(r"^(?P<y>\d{4})-W(?P<w>\d{2})$", week_str)
    if not m:
        raise ValueError("Invalid --week format. Use YYYY-Www, e.g., 2026-W09")
    y = int(m.group("y"))
    w = int(m.group("w"))

    # ISO week: Monday..Sunday
    start = date.fromisocalendar(y, w, 1)
    end = date.fromisocalendar(y, w, 7)

    return [f for f in files if start <= f.day <= end]


def filter_by_range(files: List[ResultFile], d_from: Optional[str], d_to: Optional[str]) -> List[ResultFile]:
    if d_from:
        df = datetime.strptime(d_from, "%Y-%m-%d").date()
    else:
        df = None
    if d_to:
        dt = datetime.strptime(d_to, "%Y-%m-%d").date()
    else:
        dt = None

    out = []
    for f in files:
        if df and f.day < df:
            continue
        if dt and f.day > dt:
            continue
        out.append(f)
    return out


def pick_latest_per_slot(files: List[ResultFile], strict: bool) -> List[ResultFile]:
    """
    If duplicates exist for same (day,slot), pick the one with latest evaluated_at,
    else latest mtime. If strict=True, raise.
    """
    by_key: Dict[Tuple[date, str], List[ResultFile]] = {}
    for f in files:
        by_key.setdefault((f.day, f.slot), []).append(f)

    out: List[ResultFile] = []
    for key, group in by_key.items():
        if len(group) == 1:
            out.append(group[0])
            continue

        if strict:
            paths = [g.path for g in group]
            raise ValueError(f"Duplicate result files for {key}: {paths}")

        # sort by evaluated_at, fallback mtime
        def score(g: ResultFile):
            ev = g.evaluated_at_jst.timestamp() if g.evaluated_at_jst else -1
            mt = os.path.getmtime(g.path)
            return (ev, mt)

        group.sort(key=score, reverse=True)
        out.append(group[0])

    out.sort(key=lambda x: (x.signal_dt_jst, x.path))
    return out


def load_items_from_result(path: str) -> List[Dict[str, Any]]:
    data = load_json(path)
    items = data.get("items", [])
    if not isinstance(items, list):
        return []
    return items


def aggregate_files(files: List[ResultFile]) -> Dict[str, Any]:
    """
    Build a global aggregate across given files.
    Also builds per-day and per-slot aggregates.
    """
    tz = pytz.timezone(TZ_NAME)

    # Flatten items with metadata
    flat_items: List[Dict[str, Any]] = []
    resolved_outcomes: List[str] = []

    for rf in files:
        items = load_items_from_result(rf.path)
        for it in items:
            status = it.get("status")
            rec = {
                "date": rf.day.isoformat(),
                "slot": rf.slot,
                "signal_time": rf.signal_dt_jst.isoformat(),
                "ticker": it.get("ticker"),
                "status": status,
                "r": float(it.get("r", 0.0) or 0.0),
                "source_result": os.path.relpath(rf.path).replace("\\", "/"),
            }
            flat_items.append(rec)
            if status in ("WIN", "LOSS"):
                resolved_outcomes.append(status)

    overall = compute_items_summary(flat_items)

    # per-day
    per_day: Dict[str, Dict[str, Any]] = {}
    for d in sorted({x["date"] for x in flat_items}):
        day_items = [x for x in flat_items if x["date"] == d]
        per_day[d] = compute_items_summary(day_items)

    # per-slot
    per_slot: Dict[str, Dict[str, Any]] = {}
    for s in sorted({x["slot"] for x in flat_items}):
        s_items = [x for x in flat_items if x["slot"] == s]
        per_slot[s] = compute_items_summary(s_items)

    # max losing streak (resolved only)
    overall["max_losing_streak"] = max_losing_streak(resolved_outcomes)

    # helpful meta
    meta = {
        "generated_at": now_jst().astimezone(tz).isoformat(),
        "timezone": TZ_NAME,
        "files_count": len(files),
        "signals_count": overall["n"],
        "results_files": [os.path.relpath(f.path).replace("\\", "/") for f in files],
    }

    return {
        "meta": meta,
        "overall": overall,
        "per_day": per_day,
        "per_slot": per_slot,
        "items": flat_items,  # keep for later analysis / plotting
    }


def save_snapshot(out: Dict[str, Any], agg_dir: str, name: str, force: bool) -> str:
    safe_mkdir(agg_dir)
    path = os.path.join(agg_dir, name)
    if os.path.exists(path) and not force:
        raise FileExistsError(f"Aggregate snapshot exists (use --force): {path}")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    return path


def print_brief(out: Dict[str, Any]) -> None:
    o = out["overall"]
    print("=== AGGREGATE SUMMARY ===")
    print(f"signals: {o['n']} | resolved: {o['resolved_n']} | WIN: {o['win']} LOSS: {o['loss']} OPEN: {o['open']} AMBIG: {o['ambig']}")
    print(f"total_R: {o['total_r']} | win_rate: {o['win_rate']} | avg_R_resolved: {o['avg_r_resolved']} | open_ratio: {o['open_ratio']}")
    print(f"max_losing_streak: {o.get('max_losing_streak')}")
    # slots
    if out.get("per_slot"):
        print("--- per_slot ---")
        for slot, s in out["per_slot"].items():
            print(f"{slot}: total_R={s['total_r']} win_rate={s['win_rate']} resolved={s['resolved_n']} open_ratio={s['open_ratio']}")


def parse_args():
    p = argparse.ArgumentParser(description="Aggregate trading results from outputs/results/*_result.json")
    p.add_argument("--results-dir", type=str, default=DEFAULT_RESULTS_DIR, help="results directory")
    p.add_argument("--agg-dir", type=str, default=DEFAULT_AGG_DIR, help="aggregates output directory")

    scope = p.add_mutually_exclusive_group(required=False)
    scope.add_argument("--week", type=str, default=None, help="ISO week like 2026-W09")
    scope.add_argument("--daily", action="store_true", help="Generate daily snapshots for all days")
    scope.add_argument("--all", action="store_true", help="Aggregate all results (default if no scope specified)")

    p.add_argument("--from", dest="date_from", type=str, default=None, help="YYYY-MM-DD (range start, inclusive)")
    p.add_argument("--to", dest="date_to", type=str, default=None, help="YYYY-MM-DD (range end, inclusive)")

    p.add_argument("--strict", action="store_true", help="Error if duplicates exist for same date/slot")
    p.add_argument("--force", action="store_true", help="Overwrite existing aggregate snapshots")

    return p.parse_args()


def main():
    args = parse_args()
    files = list_result_files(args.results_dir)
    if not files:
        print(f"No result files found in: {args.results_dir}")
        return

    # remove duplicates per (day,slot)
    files = pick_latest_per_slot(files, strict=bool(args.strict))

    # range filter (works with any scope)
    files = filter_by_range(files, args.date_from, args.date_to)

    # decide scope
    if args.daily:
        # generate daily snapshots for each day in filtered set
        days = sorted({f.day for f in files})
        if not days:
            print("No files after filters.")
            return

        for d in days:
            day_files = [f for f in files if f.day == d]
            out = aggregate_files(day_files)
            name = f"daily_{d.isoformat()}.json"
            saved = save_snapshot(out, args.agg_dir, name, force=bool(args.force))
            print(f"SAVED: {saved}")
        return

    if args.week:
        week_files = filter_by_week(files, args.week)
        out = aggregate_files(week_files)
        name = f"weekly_{args.week}.json"
        saved = save_snapshot(out, args.agg_dir, name, force=bool(args.force))
        print(f"SAVED: {saved}")
        print_brief(out)
        return

    # if range explicitly provided and not daily/week, create a range snapshot
    if args.date_from or args.date_to:
        out = aggregate_files(files)
        frm = args.date_from or "START"
        to = args.date_to or "END"
        name = f"range_{frm}_to_{to}.json"
        saved = save_snapshot(out, args.agg_dir, name, force=bool(args.force))
        print(f"SAVED: {saved}")
        print_brief(out)
        return

    # default/all
    out = aggregate_files(files)
    name = "all_time.json"
    saved = save_snapshot(out, args.agg_dir, name, force=bool(args.force))
    print(f"SAVED: {saved}")
    print_brief(out)


if __name__ == "__main__":
    main()