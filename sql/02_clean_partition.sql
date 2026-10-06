-- Clean ONE daily partition. {d} (a date) and {min_ts} are filled in by Python.
-- Re-running a date replaces that date's rows (idempotent).

CREATE OR REPLACE TEMP TABLE staged AS
SELECT *,
       CASE
         WHEN event_ts_ms IS NULL OR visitor_id IS NULL OR event_type IS NULL OR item_id IS NULL
              THEN 'null_key_field'
         WHEN event_type NOT IN ('view', 'addtocart', 'transaction')
              THEN 'invalid_event_type'
         WHEN event_ts < TIMESTAMP '{min_ts}' OR event_ts > CAST(current_timestamp AS TIMESTAMP)
              THEN 'timestamp_out_of_range'
         WHEN CAST(event_ts AS DATE) <> event_date
              THEN 'timestamp_partition_mismatch'
         WHEN event_type = 'transaction' AND transaction_id IS NULL
              THEN 'transaction_missing_id'
         WHEN event_type <> 'transaction' AND transaction_id IS NOT NULL
              THEN 'unexpected_transaction_id'
       END AS reject_reason
FROM raw_events
WHERE event_date = DATE '{d}';

DELETE FROM clean_events WHERE event_date = DATE '{d}';

DELETE FROM quarantine_events WHERE event_date = DATE '{d}';

INSERT INTO quarantine_events
SELECT event_ts_ms, event_date, visitor_id, event_type, item_id, transaction_id,
       reject_reason, now()
FROM staged
WHERE reject_reason IS NOT NULL;

-- Deduplicate exact repeats. event_id is a deterministic hash of the business key.
INSERT INTO clean_events
SELECT md5(CONCAT(CAST(event_ts_ms AS VARCHAR), '|', CAST(visitor_id AS VARCHAR), '|', event_type, '|',
                  CAST(item_id AS VARCHAR), '|', COALESCE(CAST(transaction_id AS VARCHAR), ''))) AS event_id,
       event_ts_ms,
       MIN(event_ts)      AS event_ts,
       MIN(event_date)    AS event_date,
       visitor_id, event_type, item_id, transaction_id,
       MIN(_source_file)  AS _source_file,
       now()              AS _processed_at
FROM staged
WHERE reject_reason IS NULL
GROUP BY event_ts_ms, visitor_id, event_type, item_id, transaction_id
