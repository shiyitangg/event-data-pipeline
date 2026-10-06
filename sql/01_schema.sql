-- Warehouse schema. {raw_glob} is filled in by Python.

CREATE OR REPLACE VIEW raw_events AS
SELECT CAST(event_date AS DATE) AS event_date, event_ts_ms, event_ts, visitor_id,
       event_type, item_id, transaction_id, _source_file, _ingested_at
FROM read_parquet('{raw_glob}', hive_partitioning = true);

CREATE TABLE IF NOT EXISTS clean_events (
    event_id        VARCHAR,
    event_ts_ms     BIGINT,
    event_ts        TIMESTAMP,
    event_date      DATE,
    visitor_id      BIGINT,
    event_type      VARCHAR,
    item_id         BIGINT,
    transaction_id  BIGINT,
    _source_file    VARCHAR,
    _processed_at   TIMESTAMP
);

CREATE TABLE IF NOT EXISTS quarantine_events (
    event_ts_ms     BIGINT,
    event_date      DATE,
    visitor_id      BIGINT,
    event_type      VARCHAR,
    item_id         BIGINT,
    transaction_id  BIGINT,
    reject_reason   VARCHAR,
    _processed_at   TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id              VARCHAR,
    step                VARCHAR,
    event_date          DATE,
    status              VARCHAR,
    rows_in             BIGINT,
    rows_rejected       BIGINT,
    duplicates_removed  BIGINT,
    rows_out            BIGINT,
    started_at          TIMESTAMP,
    finished_at         TIMESTAMP,
    error_message       VARCHAR
);

CREATE TABLE IF NOT EXISTS dq_results (
    run_id        VARCHAR,
    check_name    VARCHAR,
    severity      VARCHAR,
    event_date    DATE,
    status        VARCHAR,
    metric_value  DOUBLE,
    threshold     DOUBLE,
    detail        VARCHAR,
    checked_at    TIMESTAMP
)
