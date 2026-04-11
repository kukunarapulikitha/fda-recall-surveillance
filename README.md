# FDA Drug Recall & Safety Alert Surveillance System

An automated data pipeline that ingests FDA recall data from the OpenFDA API and FDA website, validates and deduplicates records, stores them in PostgreSQL with full historical tracking, and provides a data quality monitoring dashboard.

## Architecture

```
                    +-------------------+     +-------------------+
                    |   OpenFDA API     |     |   FDA Website     |
                    | (Drug/Device/Food)|     | (Recalls Page)    |
                    +--------+----------+     +--------+----------+
                             |                         |
                    +--------v----------+     +--------v----------+
                    |   FDAClient       |     |   FDAScraper      |
                    | (Rate limiting,   |     | (BeautifulSoup,   |
                    |  pagination,      |     |  HTML parsing)    |
                    |  retry logic)     |     |                   |
                    +--------+----------+     +--------+----------+
                             |                         |
                    +--------v-------------------------v----------+
                    |              RecallNormalizer                |
                    | (Drug/Device/Food/Website field mapping)    |
                    +---------------------+-----------------------+
                                          |
                    +---------------------v-----------------------+
                    |              RecallValidator                 |
                    | (Pydantic v2, type checks, warnings)        |
                    +---------------------+-----------------------+
                                          |
                    +---------------------v-----------------------+
                    |              RecallPipeline                  |
                    | (Deduplicate -> Upsert -> Firm aggregation) |
                    +---------------------+-----------------------+
                                          |
                    +---------------------v-----------------------+
                    |              PostgreSQL                      |
                    | recalls | firms | ingestion_logs             |
                    +---------------------+-----------------------+
                                          |
                    +---------------------v-----------------------+
                    |         Streamlit Monitoring Dashboard       |
                    | Pipeline Health | Data Quality | Coverage    |
                    +------------------------------------------------+
```

## Data Sources

| Source | Type | Endpoint | Coverage | Update Frequency |
|--------|------|----------|----------|------------------|
| Drug Enforcement | REST API | `api.fda.gov/drug/enforcement.json` | 2004-present | Weekly |
| Device Recalls | REST API | `api.fda.gov/device/recall.json` | 2002-present | Weekly |
| Food Enforcement | REST API | `api.fda.gov/food/enforcement.json` | 2004-present | Weekly |
| FDA Recalls Page | Web Scrape | `fda.gov/safety/recalls-market-withdrawals-safety-alerts` | Current | Daily |

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.11+ |
| Database | PostgreSQL 15 |
| ORM | SQLAlchemy 2.0 + Alembic |
| HTTP Client | httpx + tenacity (retry) |
| Web Scraping | BeautifulSoup4 + lxml |
| Validation | Pydantic v2 |
| Scheduling | APScheduler |
| Dashboard | Streamlit + Plotly |
| Data Processing | Pandas |
| Containerization | Docker Compose |

## Project Structure

```
fda-recall-surveillance/
├── src/
│   ├── config.py                     # Pydantic Settings (.env)
│   ├── models/
│   │   ├── base.py                   # SQLAlchemy engine + session
│   │   ├── recall.py                 # Recall model (30+ columns, 7 indexes)
│   │   ├── firm.py                   # Firm aggregate model
│   │   └── ingestion_log.py          # Pipeline run tracking
│   ├── ingestion/
│   │   ├── fda_client.py             # OpenFDA API client
│   │   ├── fda_scraper.py            # FDA website scraper
│   │   ├── normalizer.py             # Multi-source field mapping
│   │   ├── validator.py              # Pydantic validation schemas
│   │   ├── pipeline.py               # Orchestrator (fetch->validate->dedupe->upsert)
│   │   └── backfill.py               # Historical data loader
│   ├── scheduler.py                  # APScheduler daily cron
│   └── monitoring/
│       ├── app.py                    # Streamlit dashboard entry
│       └── pages/
│           ├── pipeline_health.py    # Run history, success rates
│           ├── data_quality.py       # Null rates, anomaly detection
│           └── coverage.py           # Heatmap, gap detection
├── scripts/
│   ├── init_db.sql                   # DDL (auto-runs via Docker)
│   ├── run_daily.py                  # CLI: daily pipeline
│   └── run_backfill.py               # CLI: historical backfill
├── tests/
│   ├── test_fda_client.py            # API client tests
│   ├── test_fda_scraper.py           # Scraper tests
│   ├── test_normalizer.py            # Normalizer tests
│   ├── test_validator.py             # Validator tests
│   ├── test_pipeline.py              # Deduplication tests
│   └── sample_responses/             # Fixture JSON from FDA API
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
└── alembic.ini
```

## Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose

### 1. Clone and setup

```bash
cd fda-recall-surveillance

# Create .env from template
cp .env.example .env

# Create virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e ".[dev]"
```

### 2. Start PostgreSQL

```bash
docker compose up postgres -d
```

This starts PostgreSQL on port **5433** and automatically creates the database schema via `scripts/init_db.sql`.

Verify the tables exist:

```bash
docker exec fda-recall-surveillance-postgres-1 \
  psql -U fda_user -d fda_recalls -c "\dt"
```

Expected output:

```
 Schema |      Name      | Type  |  Owner
--------+----------------+-------+----------
 public | firms          | table | fda_user
 public | ingestion_logs | table | fda_user
 public | recalls        | table | fda_user
```

### 3. Run tests

```bash
pytest tests/ -v -m "not integration"
```

All 32 unit tests should pass.

To also run integration tests (hits the real FDA API):

```bash
pytest tests/ -v -m integration
```

### 4. Backfill historical data

Load 5+ years of recall data:

```bash
python scripts/run_backfill.py --start-year 2020
```

Or load a specific year and product type:

```bash
python scripts/run_backfill.py --start-year 2024 --end-year 2024 --types Drugs
```

Example output:

```
2024-01 Drugs: 110 fetched, 110 inserted, 0 updated, 0 failed
2024-02 Drugs: 73 fetched, 73 inserted, 0 updated, 0 failed
...
Backfill complete. Total records processed: 657
```

### 5. Run the daily pipeline

```bash
python scripts/run_daily.py
```

This fetches the last 7 days of recalls from all 3 API endpoints plus the FDA website.

```
--- Daily Pipeline Results ---
Records fetched:    30
Records inserted:   10
Records updated:    0
Records skipped:    20
Validation failures: 0
```

Use `--skip-website` to only fetch from the API:

```bash
python scripts/run_daily.py --skip-website
```

### 6. Start the monitoring dashboard

```bash
streamlit run src/monitoring/app.py
```

Open [http://localhost:8501](http://localhost:8501) to view:

- **Pipeline Health** - Run history with success/fail badges, duration trends, error log viewer
- **Data Quality** - Null rates per column, validation failure trends, anomaly detection (2 std devs)
- **Coverage** - Monthly heatmap by product type, gap detection, staleness checks

### 7. Start the scheduler (automated daily runs)

```bash
python -m src.scheduler
```

This runs the pipeline automatically at **6:00 AM daily** using APScheduler.

## Database Schema

### `recalls` (central fact table)

Stores every recall record from all sources. Key columns:

| Column | Description |
|--------|-------------|
| `recall_number` | Unique FDA recall ID (e.g., `D-0179-2024`) |
| `product_type` | `Drugs`, `Devices`, or `Food` |
| `classification` | `Class I` (serious), `Class II` (moderate), `Class III` (minor) |
| `status` | `Ongoing`, `Terminated`, `Completed` |
| `recalling_firm` | Company name |
| `reason_for_recall` | Why the product was recalled |
| `product_description` | Product details |
| `code_info` | Lot numbers, expiration dates |
| `distribution_pattern` | Geographic scope (e.g., `Nationwide`) |
| `report_date` | When reported to FDA |
| `brand_name` | Brand name (from OpenFDA nested data) |
| `raw_json` | Full original API response (JSONB) |
| `is_validated` | Whether the record passed Pydantic validation |
| `validation_errors` | List of validation warnings (JSONB) |
| `first_seen_at` | When we first ingested this record |
| `last_updated_at` | When the record was last updated |

### `firms` (aggregated statistics)

| Column | Description |
|--------|-------------|
| `name` | Company name (unique) |
| `total_recalls` | Total recall count |
| `class_i_count` | Number of Class I recalls |
| `class_ii_count` | Number of Class II recalls |
| `class_iii_count` | Number of Class III recalls |

### `ingestion_logs` (pipeline observability)

| Column | Description |
|--------|-------------|
| `run_type` | `daily`, `backfill`, or `manual` |
| `source` | `openfda_api` or `fda_website` |
| `endpoint` | Which API endpoint was called |
| `records_fetched` | How many records the API returned |
| `records_inserted` | New records added |
| `records_updated` | Existing records that changed |
| `records_skipped` | Duplicates skipped |
| `validation_failures` | Records that failed validation |
| `status` | `running`, `success`, or `failed` |
| `duration_seconds` | How long the run took |

## Pipeline Details

### Data Flow

```
1. FETCH     - API client paginate all results (1000/page, rate limited at 200 req/min)
             - Website scraper parses FDA recalls page (3 pages)
2. NORMALIZE - Map endpoint-specific fields to a common schema
             - Flatten nested openfda object
             - Parse YYYYMMDD dates to Python date objects
3. VALIDATE  - Pydantic v2 strict type checking
             - Required: recall_number, recalling_firm, reason_for_recall, product_type
             - Warnings for: missing classification, product_description, dates
             - Rejects: empty required fields, invalid product types
4. DEDUPE    - By recall_number (unique key)
             - API records win over website scrapes
             - More recent report_date wins between duplicates
5. UPSERT    - PostgreSQL ON CONFLICT (recall_number) DO UPDATE
             - Updates: status, classification, termination_date, raw_json
             - Preserves first_seen_at timestamp
6. AGGREGATE - Update firm-level statistics (total_recalls, class counts)
7. LOG       - Write run stats to ingestion_logs table
```

### Backfill Strategy

The OpenFDA API caps `skip` at 26,000 results. The backfiller handles this by chunking queries month-by-month, ensuring each chunk stays well under the limit.

### Error Handling

- **API errors**: 3 retries with exponential backoff (2-8 seconds) via tenacity
- **Rate limiting**: Token bucket enforcing 200 requests/minute (under the 240 API limit)
- **Validation failures**: Logged and counted but don't halt the pipeline
- **Pipeline errors**: Caught per-endpoint so one failure doesn't block others
- **All runs logged**: Every pipeline execution is tracked in `ingestion_logs` with duration, counts, and error messages

## Configuration

All settings are in `.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://fda_user:fda_pass@localhost:5433/fda_recalls` | PostgreSQL connection |
| `FDA_API_KEY` | (empty) | Optional OpenFDA API key (increases daily limit from 1K to 120K) |
| `BACKFILL_START_YEAR` | `2020` | Default start year for backfill |
| `DAILY_LOOKBACK_DAYS` | `7` | How many days back the daily pipeline looks |
| `BATCH_SIZE` | `500` | Database commit batch size |
| `LOG_LEVEL` | `INFO` | Logging level |

Get a free API key at [https://open.fda.gov/apis/authentication/](https://open.fda.gov/apis/authentication/).

## Verified Results

Tested with real FDA data (2024 backfill):

| Metric | Value |
|--------|-------|
| Drug recalls loaded | 658 |
| Device recalls loaded | 356 |
| Food recalls loaded | 421 |
| **Total records** | **1,435** |
| Unique firms tracked | 501 |
| Unit tests passing | 32/32 |
| Validation correctly rejected | Records with empty `recalling_firm` |
| Deduplication working | 20 website duplicates skipped in daily run |
| Website scraper working | 30 entries scraped, 10 new records inserted |
