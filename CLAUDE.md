# FDA Recall Surveillance — Project Guide

## Overview

Automated FDA drug/device/food recall data pipeline. Ingests from OpenFDA API + FDA website, validates, deduplicates, stores in PostgreSQL, and provides a Streamlit monitoring dashboard.

## Commands

```bash
# Run tests (unit only, no API calls)
pytest tests/ -v -m "not integration"

# Run tests with coverage
pytest tests/ -v -m "not integration" --cov=src --cov-report=term-missing

# Run integration tests (hits real FDA API)
pytest tests/ -v -m integration

# Start PostgreSQL
docker compose up postgres -d

# Run daily pipeline
python scripts/run_daily.py

# Run daily pipeline (API only, skip website scraper)
python scripts/run_daily.py --skip-website

# Run historical backfill
python scripts/run_backfill.py --start-year 2020

# Start monitoring + analytics dashboard
streamlit run src/monitoring/app.py

# Generate executive summary report (markdown)
python scripts/generate_report.py --start 2023-01-01 -o report.md

# Start scheduler (daily 6 AM cron)
python -m src.scheduler

# Check database contents
docker exec fda-recall-surveillance-postgres-1 psql -U fda_user -d fda_recalls -c "SELECT product_type, count(*) FROM recalls GROUP BY product_type"
```

## Architecture

- **src/config.py** — Pydantic Settings loaded from `.env`
- **src/models/** — SQLAlchemy 2.0 ORM models (Recall, Firm, IngestionLog)
- **src/ingestion/fda_client.py** — OpenFDA API client (rate limiting, pagination, retry)
- **src/ingestion/fda_scraper.py** — FDA website scraper (BeautifulSoup)
- **src/ingestion/normalizer.py** — Maps different API field names to common schema
- **src/ingestion/validator.py** — Pydantic v2 validation with warnings
- **src/ingestion/pipeline.py** — Orchestrator: fetch -> normalize -> validate -> dedupe -> upsert
- **src/ingestion/backfill.py** — Historical loader (month-by-month chunking)
- **src/scheduler.py** — APScheduler BackgroundScheduler
- **src/analytics/** — Categorization, risk scoring, temporal/correlation analysis, exec report
- **src/monitoring/** — Streamlit dashboard (6 pages: 3 monitoring + 3 analytics)

## Coding Conventions

- Python 3.11+, type hints everywhere
- SQLAlchemy 2.0 style (Mapped[] annotations, not Column())
- Pydantic v2 for validation (BaseModel, field_validator)
- httpx for HTTP (not requests)
- Use generators for large result sets (memory efficiency)
- All database writes use PostgreSQL ON CONFLICT for upserts
- Store full raw API response in `raw_json` JSONB column
- Every pipeline run gets logged to `ingestion_logs` table
- Tests use fixtures from `tests/sample_responses/` (real FDA API format)

## Database

PostgreSQL on port 5433 (Docker). Three tables:
- `recalls` — central fact table (unique on `recall_number`)
- `firms` — aggregated firm statistics (unique on `name`)
- `ingestion_logs` — pipeline run tracking

## Key Design Decisions

- Device API uses different field names (e.g., `event_date_initiated` vs `recall_initiation_date`) — normalizer handles mapping
- FDA date format is `YYYYMMDD` — normalizer converts to Python `date`
- OpenFDA API `skip` parameter caps at 26,000 — backfiller chunks by month
- API records win over website scrapes in deduplication (more complete data)
- Rate limited at 200 req/min (safety buffer under FDA's 240 limit)
