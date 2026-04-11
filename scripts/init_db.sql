-- DDL fallback for direct PostgreSQL initialization
-- Prefer using Alembic migrations; this file is for docker-entrypoint-initdb.d

CREATE TABLE IF NOT EXISTS recalls (
    id              BIGSERIAL PRIMARY KEY,
    recall_number   VARCHAR(50) UNIQUE NOT NULL,
    event_id        VARCHAR(50),
    source          VARCHAR(20) NOT NULL,
    product_type    VARCHAR(20) NOT NULL,
    classification  VARCHAR(20),
    status          VARCHAR(30),
    recalling_firm  VARCHAR(500) NOT NULL,
    reason_for_recall TEXT NOT NULL,
    product_description TEXT,
    product_quantity VARCHAR(500),
    code_info       TEXT,
    distribution_pattern TEXT,
    voluntary_mandated VARCHAR(100),
    initial_firm_notification VARCHAR(100),
    recall_initiation_date DATE,
    report_date     DATE,
    center_classification_date DATE,
    termination_date DATE,
    city            VARCHAR(200),
    state           VARCHAR(10),
    country         VARCHAR(100),
    postal_code     VARCHAR(20),
    brand_name      TEXT,
    generic_name    TEXT,
    manufacturer_name TEXT,
    product_ndc     TEXT,
    substance_name  TEXT,
    route           TEXT,
    application_number VARCHAR(50),
    raw_json        JSONB NOT NULL,
    is_validated    BOOLEAN DEFAULT true,
    validation_errors JSONB,
    first_seen_at   TIMESTAMPTZ DEFAULT now(),
    last_updated_at TIMESTAMPTZ DEFAULT now(),
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_recalls_product_type ON recalls(product_type);
CREATE INDEX IF NOT EXISTS idx_recalls_classification ON recalls(classification);
CREATE INDEX IF NOT EXISTS idx_recalls_status ON recalls(status);
CREATE INDEX IF NOT EXISTS idx_recalls_report_date ON recalls(report_date);
CREATE INDEX IF NOT EXISTS idx_recalls_recalling_firm ON recalls(recalling_firm);
CREATE INDEX IF NOT EXISTS idx_recalls_recall_initiation ON recalls(recall_initiation_date);
CREATE INDEX IF NOT EXISTS idx_recalls_source ON recalls(source);

CREATE TABLE IF NOT EXISTS firms (
    id               BIGSERIAL PRIMARY KEY,
    name             VARCHAR(500) UNIQUE NOT NULL,
    city             VARCHAR(200),
    state            VARCHAR(10),
    country          VARCHAR(100),
    total_recalls    INT DEFAULT 0,
    class_i_count    INT DEFAULT 0,
    class_ii_count   INT DEFAULT 0,
    class_iii_count  INT DEFAULT 0,
    first_recall_date DATE,
    latest_recall_date DATE,
    updated_at       TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_firms_name ON firms(name);

CREATE TABLE IF NOT EXISTS ingestion_logs (
    id               BIGSERIAL PRIMARY KEY,
    run_id           UUID NOT NULL DEFAULT gen_random_uuid(),
    run_type         VARCHAR(20) NOT NULL,
    source           VARCHAR(20) NOT NULL,
    endpoint         VARCHAR(100) NOT NULL,
    date_range_start DATE,
    date_range_end   DATE,
    started_at       TIMESTAMPTZ DEFAULT now(),
    finished_at      TIMESTAMPTZ,
    records_fetched  INT DEFAULT 0,
    records_inserted INT DEFAULT 0,
    records_updated  INT DEFAULT 0,
    records_skipped  INT DEFAULT 0,
    validation_failures INT DEFAULT 0,
    status           VARCHAR(20) DEFAULT 'running',
    error_message    TEXT,
    duration_seconds NUMERIC(10,2)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_logs_status ON ingestion_logs(status);
CREATE INDEX IF NOT EXISTS idx_ingestion_logs_started ON ingestion_logs(started_at DESC);
