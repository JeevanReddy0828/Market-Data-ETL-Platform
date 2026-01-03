from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta

from dateutil.parser import isoparse

from src.pipeline import run_pipeline_for_date


def _daterange(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def main() -> int:
    parser = argparse.ArgumentParser(description="Market Data ETL Platform CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    once = sub.add_parser("run-once", help="Run for a single date")
    once.add_argument("--date", required=False, help="YYYY-MM-DD (default: today-1)")
    once.add_argument("--start", required=False, help="YYYY-MM-DD")
    once.add_argument("--end", required=False, help="YYYY-MM-DD")

    args = parser.parse_args()

    if args.cmd == "run-once":
        if args.date:
            d = isoparse(args.date).date()
            run_pipeline_for_date(d)
            return 0

        if args.start and args.end:
            start = isoparse(args.start).date()
            end = isoparse(args.end).date()
            for d in _daterange(start, end):
                run_pipeline_for_date(d)
            return 0

        # default: yesterday
        d = (datetime.utcnow().date() - timedelta(days=1))
        run_pipeline_for_date(d)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
