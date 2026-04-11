#!/usr/bin/env python3
"""CLI entry point for running the daily pipeline."""

import argparse
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import settings
from src.ingestion.fda_client import FDAClient
from src.ingestion.fda_scraper import FDAScraper
from src.ingestion.normalizer import RecallNormalizer
from src.ingestion.pipeline import RecallPipeline
from src.models.base import SessionLocal

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Run the daily FDA recall pipeline")
    parser.add_argument(
        "--skip-website", action="store_true",
        help="Skip FDA website scraping (API only)",
    )
    args = parser.parse_args()

    session = SessionLocal()
    try:
        client = FDAClient()
        scraper = None if args.skip_website else FDAScraper()
        normalizer = RecallNormalizer()

        pipeline = RecallPipeline(
            session=session,
            fda_client=client,
            scraper=scraper,
            normalizer=normalizer,
        )

        logger.info("Starting daily pipeline...")
        stats = pipeline.run_daily()

        print(f"\n--- Daily Pipeline Results ---")
        print(f"Records fetched:    {stats.records_fetched}")
        print(f"Records inserted:   {stats.records_inserted}")
        print(f"Records updated:    {stats.records_updated}")
        print(f"Records skipped:    {stats.records_skipped}")
        print(f"Validation failures: {stats.validation_failures}")
        if stats.errors:
            print(f"Errors: {len(stats.errors)}")
            for err in stats.errors:
                print(f"  - {err}")

    except Exception:
        logger.exception("Daily pipeline failed")
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    main()
