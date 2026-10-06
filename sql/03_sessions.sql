-- Sessionization. A new session starts after {gap_min} minutes of inactivity for the same visitor.
-- Rebuilt in full from clean_events on every run (cheap at this size, and always consistent).

DROP TABLE IF EXISTS events_sessionized;

CREATE TABLE events_sessionized AS
WITH ordered AS (
    SELECT event_id, event_ts_ms, event_ts, event_date, visitor_id, event_type, item_id, transaction_id,
           LAG(event_ts_ms) OVER (PARTITION BY visitor_id
                                  ORDER BY event_ts_ms, event_type, item_id, event_id) AS prev_ts_ms
    FROM clean_events
),
flagged AS (
    SELECT *,
           CASE WHEN prev_ts_ms IS NULL OR event_ts_ms - prev_ts_ms > {gap_ms} THEN 1 ELSE 0 END AS new_session
    FROM ordered
)
SELECT event_id, event_ts_ms, event_ts, event_date, visitor_id, event_type, item_id, transaction_id,
       CAST(visitor_id AS VARCHAR) || '-' ||
       CAST(SUM(new_session) OVER (PARTITION BY visitor_id
                                   ORDER BY event_ts_ms, event_type, item_id, event_id
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS VARCHAR) AS session_id
FROM flagged;

DROP TABLE IF EXISTS sessions;

CREATE TABLE sessions AS
SELECT session_id,
       visitor_id,
       MIN(event_date)                                   AS session_date,
       MIN(event_ts_ms)                                  AS start_ts_ms,
       MAX(event_ts_ms)                                  AS end_ts_ms,
       (MAX(event_ts_ms) - MIN(event_ts_ms)) / 1000.0    AS duration_seconds,
       COUNT(*)                                          AS n_events,
       SUM(CASE WHEN event_type = 'view'        THEN 1 ELSE 0 END) AS n_views,
       SUM(CASE WHEN event_type = 'addtocart'   THEN 1 ELSE 0 END) AS n_addtocart,
       SUM(CASE WHEN event_type = 'transaction' THEN 1 ELSE 0 END) AS n_transactions,
       MAX(CASE WHEN event_type = 'view'        THEN 1 ELSE 0 END) AS has_view,
       MAX(CASE WHEN event_type = 'addtocart'   THEN 1 ELSE 0 END) AS has_cart,
       MAX(CASE WHEN event_type = 'transaction' THEN 1 ELSE 0 END) AS has_txn
FROM events_sessionized
GROUP BY session_id, visitor_id
