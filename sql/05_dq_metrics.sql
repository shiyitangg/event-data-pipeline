-- Raw-layer quality metrics, one row per event_date. Filled in and executed by quality_checks.py.
SELECT event_date,
       COUNT(*) AS n_rows,
       SUM(CASE WHEN event_ts_ms IS NULL OR visitor_id IS NULL OR event_type IS NULL OR item_id IS NULL
                THEN 1 ELSE 0 END) AS null_key_rows,
       SUM(CASE WHEN event_type IS NOT NULL AND event_type NOT IN ('view', 'addtocart', 'transaction')
                THEN 1 ELSE 0 END) AS bad_event_type,
       SUM(CASE WHEN event_ts < TIMESTAMP '{min_ts}' OR event_ts > CAST(current_timestamp AS TIMESTAMP)
                THEN 1 ELSE 0 END) AS bad_ts,
       SUM(CASE WHEN CAST(event_ts AS DATE) <> event_date THEN 1 ELSE 0 END) AS date_mismatch,
       SUM(CASE WHEN (event_type = 'transaction' AND transaction_id IS NULL)
                  OR (event_type <> 'transaction' AND transaction_id IS NOT NULL)
                THEN 1 ELSE 0 END) AS txn_rule_violations
FROM raw_events
GROUP BY event_date
