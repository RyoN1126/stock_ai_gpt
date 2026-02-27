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

# filenames:
#   YYYY-MM-DD_morning_result.json
#   YYYY-MM-DD_close_result.json
# and also allow suffix:
#   YYYY-MM-DD_morning_235959_result.json (if signal duplicated, we suffix signal filename)
RESULT_RE = re.compile(
    r"^(?P<d>\d{4}-\d{2}-\d{2})_(?P<session>morning|close)(?:_(?P<suffix>\d{6}))?_result\.json$"
)


def tz():
    return pytz.timezone(TZ_NAME)


def now_jst() -> datetime:
    return datetime.now(tz())


def parse_iso_dt(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = tz().localize(dt)
    return dt


def safe_mkdir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


@dataclass(frozen=True)
class ResultFile:
    path: str
    day: date
    session: str  # "morning" or "close"
    evaluated_at_jst: Optional[datetime]
    signal_asof_jst: Optional[datetime]


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_result_filename(fn: str) -> Optional[Tuple[date, str]]:
    m = RESULT_RE.match(fn)
    if not m:
        return None
    d = datetime.strptime(m.group("d"), "%Y-%m-%d").date()
    session = m.group("session")
    return d, session


def list_result_files(results_dir: str) -> List[ResultFile]:
    out: List[ResultFile] = []
    if not os.path.isdir(results_dir):
        return out

    for fn in os.listdir(results_dir):
        parsed = parse_result_filename(fn)
        if not parsed:
            continue
        d, session = parsed
        path = os.path.join(results_dir, fn)

        evaluated = None
        signal_asof = None
        try:
            data = load_json(path)
            if isinstance(data, dict):
                # result JSON stores these under "meta" key
                meta = data.get("meta") or {}
                ev_str = meta.get("evaluated_at") or data.get("evaluated_at")
                sa_str = meta.get("signal_asof") or data.get("signal_asof")
                if ev_str:
                    evaluated = parse_iso_dt(ev_str).astimezone(tz())
                if sa_str:
                    signal_asof = parse_iso_dt(sa_str).astimezone(tz())
        except Exception:
            evaluated = None
            signal_asof = None

        out.append(
            ResultFile(
                path=path,
                day=d,
                session=session,
                evaluated_at_jst=evaluated,
                signal_asof_jst=signal_asof,
            )
        )

    # sort: day then session order morning->close then evaluated time
    session_order = {"morning": 0, "close": 1}
    out.sort(
        key=lambda x: (
            x.day,
            session_order.get(x.session, 9),
            x.evaluated_at_jst.timestamp() if x.evaluated_at_jst else -1,
            x.path,
        )
    )
    return out


def compute_items_summary(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(items)
    win = sum(1 for x in items if x.get("status") == "WIN")
    loss = sum(1 for x in items if x.get("status") == "LOSS")
    open_ = sum(1 for x in items if x.get("status") == "OPEN")
    ambig = sum(1 for x in items if x.get("status") == "AMBIG")

    total_r = round(sum(float(x.get("r", 0.0) or 0.0) for x in items), 4)

    resolved_n = win + loss
    win_rate = round(win / resolved_n, 4) if resolved_n > 0 else None

    resolved_rs = [float(x.get("r", 0.0) or 0.0) for x in items if x.get("status") in ("WIN", "LOSS")]
    avg_r_resolved = round(sum(resolved_rs) / len(resolved_rs), 4) if resolved_rs else None

    avg_r_all = round(total_r / n, 4) if n > 0 else None
    open_ratio = round(open_ / n, 4) if n > 0 else None

    return {
        "n": n,
        "win": win,
        "loss": loss,
        "open": open_,
        "ambig": ambig,
        "resolved_n": resolved_n,
        "total_r": total_r,
        "win_rate": win_rate,
        "avg_r_resolved": avg_r_resolved,
        "avg_r_all": avg_r_all,
        "open_ratio": open_ratio,
    }


def max_losing_streak(resolved_outcomes: List[str]) -> int:
    mx = 0
    cur = 0
    for o in resolved_outcomes:
        if o == "LOSS":
            cur += 1
            mx = max(mx, cur)
        elif o == "WIN":
            cur = 0
    return mx


def load_items_from_result(path: str) -> List[Dict[str, Any]]:
    data = load_json(path)
    items = data.get("items", [])
    if not isinstance(items, list):
        return []
    return items


def pick_latest_per_session(files: List[ResultFile], strict: bool) -> List[ResultFile]:
    """
    If multiple results exist for same (day,session), pick the latest evaluated_at,
    else latest mtime. If strict=True, raise.
    """
    by_key: Dict[Tuple[date, str], List[ResultFile]] = {}
    for f in files:
        by_key.setdefault((f.day, f.session), []).append(f)

    out: List[ResultFile] = []
    for key, group in by_key.items():
        if len(group) == 1:
            out.append(group[0])
            continue

        if strict:
            raise ValueError(f"Duplicate result files for {key}: {[g.path for g in group]}")

        def score(g: ResultFile):
            ev = g.evaluated_at_jst.timestamp() if g.evaluated_at_jst else -1
            mt = os.path.getmtime(g.path)
            return (ev, mt)

        group.sort(key=score, reverse=True)
        out.append(group[0])

    # stable sort
    session_order = {"morning": 0, "close": 1}
    out.sort(key=lambda x: (x.day, session_order.get(x.session, 9), x.path))
    return out


def filter_by_range(files: List[ResultFile], d_from: Optional[str], d_to: Optional[str]) -> List[ResultFile]:
    from_date = datetime.strptime(d_from, "%Y-%m-%d").date() if d_from else None
    to_date = datetime.strptime(d_to, "%Y-%m-%d").date() if d_to else None

    out = []
    for f in files:
        if from_date and f.day < from_date:
            continue
        if to_date and f.day > to_date:
            continue
        out.append(f)
    return out


def filter_by_week(files: List[ResultFile], week_str: str) -> List[ResultFile]:
    # week_str like "2026-W09"
    m = re.match(r"^(?P<y>\d{4})-W(?P<w>\d{2})$", week_str)
    if not m:
        raise ValueError("Invalid --week format. Use YYYY-Www, e.g., 2026-W09")
    y = int(m.group("y"))
    w = int(m.group("w"))

    start = date.fromisocalendar(y, w, 1)
    end = date.fromisocalendar(y, w, 7)
    return [f for f in files if start <= f.day <= end]


def aggregate_files(files: List[ResultFile]) -> Dict[str, Any]:
    flat_items: List[Dict[str, Any]] = []
    resolved_outcomes: List[str] = []

    for rf in files:
        items = load_items_from_result(rf.path)
        for it in items:
            status = it.get("status")
            rec = {
                "date": rf.day.isoformat(),
                "session": rf.session,
                "ticker": it.get("ticker"),
                "status": status,
                "r": float(it.get("r", 0.0) or 0.0),
                "source_result": os.path.relpath(rf.path).replace("\\", "/"),
            }
            flat_items.append(rec)
            if status in ("WIN", "LOSS"):
                resolved_outcomes.append(status)

    overall = compute_items_summary(flat_items)
    overall["max_losing_streak"] = max_losing_streak(resolved_outcomes)

    # per_day
    per_day: Dict[str, Dict[str, Any]] = {}
    for d in sorted({x["date"] for x in flat_items}):
        day_items = [x for x in flat_items if x["date"] == d]
        per_day[d] = compute_items_summary(day_items)

    # per_session
    per_session: Dict[str, Dict[str, Any]] = {}
    for s in ["morning", "close"]:
        s_items = [x for x in flat_items if x["session"] == s]
        if s_items:
            per_session[s] = compute_items_summary(s_items)

    meta = {
        "generated_at": now_jst().isoformat(),
        "timezone": TZ_NAME,
        "files_count": len(files),
        "signals_count": overall["n"],
        "results_files": [os.path.relpath(f.path).replace("\\", "/") for f in files],
    }

    return {
        "meta": meta,
        "overall": overall,
        "per_day": per_day,
        "per_session": per_session,
        "items": flat_items,
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
    print(
        f"signals: {o['n']} | resolved: {o['resolved_n']} | "
        f"WIN: {o['win']} LOSS: {o['loss']} OPEN: {o['open']} AMBIG: {o['ambig']}"
    )
    print(
        f"total_R: {o['total_r']} | win_rate: {o['win_rate']} | "
        f"avg_R_resolved: {o['avg_r_resolved']} | open_ratio: {o['open_ratio']}"
    )
    print(f"max_losing_streak: {o.get('max_losing_streak')}")

    ps = out.get("per_session") or {}
    if ps:
        print("--- per_session ---")
        for s in ["morning", "close"]:
            if s in ps:
                v = ps[s]
                print(
                    f"{s}: total_R={v['total_r']} win_rate={v['win_rate']} "
                    f"resolved={v['resolved_n']} open_ratio={v['open_ratio']}"
                )


def parse_args():
    p = argparse.ArgumentParser(description="Aggregate results from outputs/results/*_result.json (morning/close).")
    p.add_argument("--results-dir", type=str, default=DEFAULT_RESULTS_DIR, help="results directory")
    p.add_argument("--agg-dir", type=str, default=DEFAULT_AGG_DIR, help="aggregates output directory")

    scope = p.add_mutually_exclusive_group(required=False)
    scope.add_argument("--week", type=str, default=None, help="ISO week like 2026-W09")
    scope.add_argument("--daily", action="store_true", help="Generate daily snapshots for all days")
    scope.add_argument("--all", action="store_true", help="Aggregate all results (default)")

    p.add_argument("--from", dest="date_from", type=str, default=None, help="YYYY-MM-DD (range start, inclusive)")
    p.add_argument("--to", dest="date_to", type=str, default=None, help="YYYY-MM-DD (range end, inclusive)")

    p.add_argument("--strict", action="store_true", help="Error if duplicates exist for same date/session")
    p.add_argument("--force", action="store_true", help="Overwrite existing aggregate snapshots")

    return p.parse_args()


def main():
    args = parse_args()
    files = list_result_files(args.results_dir)
    if not files:
        print(f"No result files found in: {args.results_dir}")
        return

    files = pick_latest_per_session(files, strict=bool(args.strict))
    files = filter_by_range(files, args.date_from, args.date_to)

    if args.daily:
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