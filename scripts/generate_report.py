#!/usr/bin/env python
"""Generate an executive summary report from the recall database.

Usage:
    python scripts/generate_report.py                             # last 365 days
    python scripts/generate_report.py --start 2023-01-01          # since date
    python scripts/generate_report.py --product-type Drugs        # drugs only
    python scripts/generate_report.py --output report.md          # write to file
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

from src.analytics.queries import load_recalls
from src.analytics.reports import ExecutiveReport


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, default=None,
                        help="Start date (YYYY-MM-DD). Defaults to 365 days ago.")
    parser.add_argument("--end", type=date.fromisoformat, default=None,
                        help="End date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--product-type", choices=["Drugs", "Devices", "Food"],
                        default=None, help="Filter to a single product type.")
    parser.add_argument("--output", "-o", type=Path, default=None,
                        help="Output file path (default: stdout).")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start = args.start or (date.today() - timedelta(days=365))
    end = args.end or date.today()

    df = load_recalls(start_date=start, end_date=end, product_type=args.product_type)
    if df.empty:
        sys.stderr.write(f"No recall data found for {start} -> {end}\n")
        return 1

    markdown = ExecutiveReport().render_markdown(df)

    if args.output:
        args.output.write_text(markdown, encoding="utf-8")
        sys.stderr.write(f"Wrote {len(markdown):,} chars to {args.output}\n")
    else:
        sys.stdout.write(markdown)
    return 0


if __name__ == "__main__":
    sys.exit(main())
