#!/usr/bin/env python3
"""CLI entry point for historical backfill."""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import settings
from src.ingestion.backfill import Backfiller
from src.ingestion.fda_client import FDAClient
from src.models.base import SessionLocal

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Backfill historical FDA recall data")
    parser.add_argument(
        "--start-year", type=int, default=settings.BACKFILL_START_YEAR,
        help=f"Start year (default: {settings.BACKFILL_START_YEAR})",
    )
    parser.add_argument(
        "--end-year", type=int, default=date.today().year,
        help=f"End year (default: {date.today().year})",
    )
    parser.add_argument(
        "--types", nargs="+", default=["Drugs", "Devices", "Food"],
        choices=["Drugs", "Devices", "Food"],
        help="Product types to backfill (default: all)",
    )
    args = parser.parse_args()

    session = SessionLocal()
    try:
        client = FDAClient()
        backfiller = Backfiller(session=session, client=client)

        logger.info(
            "Starting backfill: %d-%d for %s",
            args.start_year, args.end_year, ", ".join(args.types),
        )
        total = backfiller.run(
            start_year=args.start_year,
            end_year=args.end_year,
            product_types=args.types,
        )
        print(f"\nBackfill complete. Total records processed: {total}")

    except Exception:
        logger.exception("Backfill failed")
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
