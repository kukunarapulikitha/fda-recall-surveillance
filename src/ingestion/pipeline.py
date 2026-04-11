"""Pipeline orchestrator: fetch -> normalize -> validate -> dedupe -> upsert."""

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.config import settings
from src.ingestion.fda_client import FDAClient
from src.ingestion.fda_scraper import FDAScraper
from src.ingestion.normalizer import RecallNormalizer
from src.ingestion.validator import validate_batch
from src.models.firm import Firm
from src.models.ingestion_log import IngestionLog
from src.models.recall import Recall

logger = logging.getLogger(__name__)

PRODUCT_TYPES = ["Drugs", "Devices", "Food"]


@dataclass
class IngestionStats:
    """Statistics from a pipeline run."""

    records_fetched: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_skipped: int = 0
    validation_failures: int = 0
    errors: list[str] = field(default_factory=list)


class RecallPipeline:
    """Orchestrates the full recall data pipeline."""

    def __init__(
        self,
        session: Session,
        fda_client: FDAClient | None = None,
        scraper: FDAScraper | None = None,
        normalizer: RecallNormalizer | None = None,
    ):
        self.session = session
        self.client = fda_client or FDAClient()
        self.scraper = scraper or FDAScraper()
        self.normalizer = normalizer or RecallNormalizer()

    def run_daily(self) -> IngestionStats:
        """Run the daily pipeline: fetch recent recalls from all sources."""
        end_date = date.today()
        start_date = end_date - timedelta(days=settings.DAILY_LOOKBACK_DAYS)
        date_from = start_date.strftime("%Y%m%d")
        date_to = end_date.strftime("%Y%m%d")

        total_stats = IngestionStats()

        # 1. Fetch from all 3 API endpoints
        for product_type in PRODUCT_TYPES:
            stats = self._ingest_api_endpoint(
                product_type, date_from, date_to, run_type="daily"
            )
            total_stats.records_fetched += stats.records_fetched
            total_stats.records_inserted += stats.records_inserted
            total_stats.records_updated += stats.records_updated
            total_stats.records_skipped += stats.records_skipped
            total_stats.validation_failures += stats.validation_failures
            total_stats.errors.extend(stats.errors)

        # 2. Fetch from FDA website scraper
        website_stats = self._ingest_website(run_type="daily")
        total_stats.records_fetched += website_stats.records_fetched
        total_stats.records_inserted += website_stats.records_inserted
        total_stats.records_updated += website_stats.records_updated
        total_stats.records_skipped += website_stats.records_skipped
        total_stats.validation_failures += website_stats.validation_failures
        total_stats.errors.extend(website_stats.errors)

        logger.info(
            "Daily pipeline complete: fetched=%d inserted=%d updated=%d skipped=%d failures=%d",
            total_stats.records_fetched,
            total_stats.records_inserted,
            total_stats.records_updated,
            total_stats.records_skipped,
            total_stats.validation_failures,
        )
        return total_stats

    def run_api_only(
        self, product_type: str, date_from: str, date_to: str, run_type: str = "manual"
    ) -> IngestionStats:
        """Ingest data from a single API endpoint for a date range."""
        return self._ingest_api_endpoint(product_type, date_from, date_to, run_type)

    def _ingest_api_endpoint(
        self, product_type: str, date_from: str, date_to: str, run_type: str
    ) -> IngestionStats:
        """Fetch, normalize, validate, dedupe, and upsert from one API endpoint."""
        stats = IngestionStats()
        log = self._start_log(
            run_type, "openfda_api", product_type, date_from, date_to
        )
        start_time = time.time()

        try:
            all_raw = []
            for batch in self.client.get_records(product_type, date_from, date_to):
                all_raw.extend(batch)
            stats.records_fetched = len(all_raw)

            # Normalize
            normalized = []
            for raw in all_raw:
                try:
                    record = self.normalizer.normalize(raw, product_type, "openfda_api")
                    normalized.append(record)
                except Exception as e:
                    logger.warning("Normalization failed: %s", e)
                    stats.validation_failures += 1

            # Validate
            valid, failed, warning_count = validate_batch(normalized)
            stats.validation_failures += len(failed)

            # Deduplicate
            deduped = self._deduplicate(valid)
            stats.records_skipped = len(valid) - len(deduped)

            # Upsert
            inserted, updated = self._upsert_recalls(deduped)
            stats.records_inserted = inserted
            stats.records_updated = updated

            # Update firm aggregates
            self._update_firms(deduped)

            self._finish_log(log, stats, time.time() - start_time)

        except Exception as e:
            logger.error("Pipeline error for %s: %s", product_type, e)
            stats.errors.append(str(e))
            self._finish_log(log, stats, time.time() - start_time, error=str(e))

        return stats

    def _ingest_website(self, run_type: str) -> IngestionStats:
        """Fetch and ingest records from the FDA website scraper."""
        stats = IngestionStats()
        log = self._start_log(run_type, "fda_website", "website", None, None)
        start_time = time.time()

        try:
            raw_entries = self.scraper.fetch_recent_alerts()
            stats.records_fetched = len(raw_entries)

            # Normalize
            normalized = []
            for raw in raw_entries:
                try:
                    record = self.normalizer.normalize(raw, "Food", "fda_website")
                    normalized.append(record)
                except Exception as e:
                    logger.warning("Website normalization failed: %s", e)
                    stats.validation_failures += 1

            # Validate
            valid, failed, _ = validate_batch(normalized)
            stats.validation_failures += len(failed)

            # Deduplicate
            deduped = self._deduplicate(valid)
            stats.records_skipped = len(valid) - len(deduped)

            # Upsert
            inserted, updated = self._upsert_recalls(deduped)
            stats.records_inserted = inserted
            stats.records_updated = updated

            self._finish_log(log, stats, time.time() - start_time)

        except Exception as e:
            logger.error("Website scraper error: %s", e)
            stats.errors.append(str(e))
            self._finish_log(log, stats, time.time() - start_time, error=str(e))

        return stats

    def _upsert_recalls(self, records: list[dict]) -> tuple[int, int]:
        """Upsert recall records into the database. Returns (inserted, updated)."""
        if not records:
            return 0, 0

        inserted = 0
        updated = 0
        batch_size = settings.BATCH_SIZE

        for i in range(0, len(records), batch_size):
            batch = records[i: i + batch_size]
            for record in batch:
                # Remove keys not in the model
                db_record = {
                    k: v for k, v in record.items()
                    if hasattr(Recall, k) and k != "id"
                }
                # Ensure raw_json is serializable
                if "raw_json" in db_record and isinstance(db_record["raw_json"], dict):
                    db_record["raw_json"] = json.loads(json.dumps(db_record["raw_json"], default=str))

                stmt = pg_insert(Recall).values(**db_record)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["recall_number"],
                    set_={
                        "status": stmt.excluded.status,
                        "classification": stmt.excluded.classification,
                        "termination_date": stmt.excluded.termination_date,
                        "last_updated_at": datetime.utcnow(),
                        "raw_json": stmt.excluded.raw_json,
                        "is_validated": stmt.excluded.is_validated,
                        "validation_errors": stmt.excluded.validation_errors,
                    },
                )
                result = self.session.execute(stmt)
                # Check if it was an insert or update
                if result.rowcount:
                    # PostgreSQL: rowcount=1 for both insert and update with ON CONFLICT
                    # We need to check if the record existed before
                    existing = self.session.execute(
                        text(
                            "SELECT first_seen_at, last_updated_at FROM recalls "
                            "WHERE recall_number = :rn"
                        ),
                        {"rn": record["recall_number"]},
                    ).fetchone()
                    if existing and existing[0] != existing[1]:
                        updated += 1
                    else:
                        inserted += 1

            self.session.commit()

        return inserted, updated

    def _update_firms(self, records: list[dict]):
        """Update or insert firm aggregate records."""
        firms_data: dict[str, dict] = {}
        for rec in records:
            name = rec.get("recalling_firm", "").strip()
            if not name:
                continue
            if name not in firms_data:
                firms_data[name] = {
                    "city": rec.get("city"),
                    "state": rec.get("state"),
                    "country": rec.get("country"),
                    "recalls": [],
                }
            firms_data[name]["recalls"].append(rec)

        for name, data in firms_data.items():
            classification_counts = {"Class I": 0, "Class II": 0, "Class III": 0}
            dates = []
            for r in data["recalls"]:
                cls = r.get("classification", "")
                if cls in classification_counts:
                    classification_counts[cls] += 1
                d = r.get("recall_initiation_date") or r.get("report_date")
                if d:
                    dates.append(d)

            stmt = pg_insert(Firm).values(
                name=name,
                city=data["city"],
                state=data["state"],
                country=data["country"],
                total_recalls=len(data["recalls"]),
                class_i_count=classification_counts["Class I"],
                class_ii_count=classification_counts["Class II"],
                class_iii_count=classification_counts["Class III"],
                first_recall_date=min(dates) if dates else None,
                latest_recall_date=max(dates) if dates else None,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["name"],
                set_={
                    "total_recalls": Firm.total_recalls + len(data["recalls"]),
                    "class_i_count": Firm.class_i_count + classification_counts["Class I"],
                    "class_ii_count": Firm.class_ii_count + classification_counts["Class II"],
                    "class_iii_count": Firm.class_iii_count + classification_counts["Class III"],
                    "latest_recall_date": stmt.excluded.latest_recall_date,
                },
            )
            self.session.execute(stmt)

        self.session.commit()

    @staticmethod
    def _deduplicate(records: list[dict]) -> list[dict]:
        """Deduplicate records by recall_number, preferring API over website."""
        seen: dict[str, dict] = {}
        for rec in records:
            rn = rec.get("recall_number", "")
            if not rn:
                continue
            existing = seen.get(rn)
            if existing is None:
                seen[rn] = rec
            elif rec.get("source") == "openfda_api" and existing.get("source") == "fda_website":
                seen[rn] = rec  # API wins over website
            elif rec.get("report_date") and existing.get("report_date"):
                if rec["report_date"] > existing["report_date"]:
                    seen[rn] = rec  # More recent report wins
        return list(seen.values())

    def _start_log(
        self, run_type: str, source: str, endpoint: str,
        date_from: str | None, date_to: str | None,
    ) -> IngestionLog:
        """Create an ingestion log entry."""
        log = IngestionLog(
            run_type=run_type,
            source=source,
            endpoint=endpoint,
            date_range_start=(
                datetime.strptime(date_from, "%Y%m%d").date() if date_from else None
            ),
            date_range_end=(
                datetime.strptime(date_to, "%Y%m%d").date() if date_to else None
            ),
            status="running",
        )
        self.session.add(log)
        self.session.commit()
        return log

    def _finish_log(
        self, log: IngestionLog, stats: IngestionStats,
        duration: float, error: str | None = None,
    ):
        """Update an ingestion log with final stats."""
        log.finished_at = datetime.utcnow()
        log.records_fetched = stats.records_fetched
        log.records_inserted = stats.records_inserted
        log.records_updated = stats.records_updated
        log.records_skipped = stats.records_skipped
        log.validation_failures = stats.validation_failures
        log.duration_seconds = round(duration, 2)
        log.status = "failed" if error else "success"
        log.error_message = error
        self.session.commit()
