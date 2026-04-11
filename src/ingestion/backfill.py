"""Historical backfill module for loading years of FDA recall data."""

import logging
from datetime import date

from sqlalchemy.orm import Session

from src.config import settings
from src.ingestion.fda_client import FDAClient
from src.ingestion.normalizer import RecallNormalizer
from src.ingestion.pipeline import RecallPipeline

logger = logging.getLogger(__name__)

PRODUCT_TYPES = ["Drugs", "Devices", "Food"]


class Backfiller:
    """Loads historical recall data by chunking requests month-by-month."""

    def __init__(self, session: Session, client: FDAClient | None = None):
        self.session = session
        self.client = client or FDAClient()
        self.normalizer = RecallNormalizer()
        self.pipeline = RecallPipeline(
            session=session,
            fda_client=self.client,
            normalizer=self.normalizer,
        )

    def run(
        self,
        start_year: int | None = None,
        end_year: int | None = None,
        product_types: list[str] | None = None,
    ):
        """Run backfill for the specified year range and product types.

        Chunks by month to stay under the API's skip cap (26,000).
        """
        start_year = start_year or settings.BACKFILL_START_YEAR
        end_year = end_year or date.today().year
        product_types = product_types or PRODUCT_TYPES

        total_records = 0

        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                # Skip future months
                if year == date.today().year and month > date.today().month:
                    break

                date_from = f"{year}{month:02d}01"
                # Calculate last day of month
                if month == 12:
                    date_to = f"{year}1231"
                else:
                    next_month_first = date(year, month + 1, 1)
                    last_day = (next_month_first - __import__("datetime").timedelta(days=1))
                    date_to = last_day.strftime("%Y%m%d")

                for product_type in product_types:
                    logger.info(
                        "Backfilling %s for %d-%02d...", product_type, year, month
                    )
                    try:
                        stats = self.pipeline.run_api_only(
                            product_type=product_type,
                            date_from=date_from,
                            date_to=date_to,
                            run_type="backfill",
                        )
                        month_total = stats.records_inserted + stats.records_updated
                        total_records += month_total
                        logger.info(
                            "%d-%02d %s: %d fetched, %d inserted, %d updated, %d failed",
                            year, month, product_type,
                            stats.records_fetched,
                            stats.records_inserted,
                            stats.records_updated,
                            stats.validation_failures,
                        )
                    except Exception as e:
                        logger.error(
                            "Backfill error for %s %d-%02d: %s",
                            product_type, year, month, e,
                        )

        logger.info("Backfill complete. Total records processed: %d", total_records)
        return total_records
