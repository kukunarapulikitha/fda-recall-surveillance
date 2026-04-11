"""APScheduler-based daily pipeline scheduler."""

import logging
import signal
import sys
import time

from apscheduler.schedulers.background import BackgroundScheduler

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


def run_daily_pipeline():
    """Execute the daily pipeline run."""
    logger.info("Starting daily pipeline run...")
    session = SessionLocal()
    try:
        client = FDAClient()
        scraper = FDAScraper()
        normalizer = RecallNormalizer()
        pipeline = RecallPipeline(
            session=session,
            fda_client=client,
            scraper=scraper,
            normalizer=normalizer,
        )
        stats = pipeline.run_daily()
        logger.info(
            "Daily pipeline finished: fetched=%d inserted=%d updated=%d failures=%d",
            stats.records_fetched,
            stats.records_inserted,
            stats.records_updated,
            stats.validation_failures,
        )
    except Exception:
        logger.exception("Daily pipeline failed")
    finally:
        session.close()


def main():
    """Start the scheduler."""
    scheduler = BackgroundScheduler()

    # Daily API pipeline at 6:00 AM
    scheduler.add_job(
        run_daily_pipeline,
        "cron",
        hour=6,
        minute=0,
        id="daily_pipeline",
        misfire_grace_time=3600,
    )

    scheduler.start()
    logger.info("Scheduler started. Daily pipeline scheduled for 6:00 AM.")
    logger.info("Press Ctrl+C to exit.")

    def shutdown(signum, frame):
        logger.info("Shutting down scheduler...")
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Keep the main thread alive
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
