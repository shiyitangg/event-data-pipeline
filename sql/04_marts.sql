-- Metric marts for product analytics. Built from clean_events and sessions.
-- is_partial_day marks the first and last UTC days, which are not full days in the source data.

DROP TABLE IF EXISTS daily_metrics;

CREATE TABLE daily_metrics AS
WITH ev AS (
    SELECT event_date,
           COUNT(DISTINCT visitor_id)                                   AS dau,
           COUNT(*)                                                     AS events,
           SUM(CASE WHEN event_type = 'view'        THEN 1 ELSE 0 END)  AS views,
           SUM(CASE WHEN event_type = 'addtocart'   THEN 1 ELSE 0 END)  AS addtocarts,
           SUM(CASE WHEN event_type = 'transaction' THEN 1 ELSE 0 END)  AS transactions
    FROM clean_events
    GROUP BY event_date
),
se AS (
    SELECT session_date AS event_date,
           COUNT(*)                                                      AS sessions,
           SUM(has_cart)                                                 AS cart_sessions,
           SUM(has_txn)                                                  AS purchase_sessions,
           SUM(CASE WHEN has_cart = 1 AND has_txn = 1 THEN 1 ELSE 0 END) AS cart_then_purchase_sessions,
           AVG(duration_seconds)                                         AS avg_session_seconds
    FROM sessions
    GROUP BY session_date
)
SELECT ev.event_date, ev.dau, ev.events, ev.views, ev.addtocarts, ev.transactions,
       se.sessions, se.cart_sessions, se.purchase_sessions, se.cart_then_purchase_sessions,
       ROUND(se.avg_session_seconds, 1)                                   AS avg_session_seconds,
       ROUND(1.0 * se.cart_sessions / se.sessions, 5)                     AS cart_session_rate,
       ROUND(1.0 * se.purchase_sessions / se.sessions, 5)                 AS purchase_session_rate,
       CASE WHEN se.cart_sessions > 0
            THEN ROUND(1.0 * se.cart_then_purchase_sessions / se.cart_sessions, 5) END AS cart_to_purchase_rate,
       CASE WHEN ev.event_date = (SELECT MIN(event_date) FROM clean_events)
              OR ev.event_date = (SELECT MAX(event_date) FROM clean_events)
            THEN 1 ELSE 0 END                                             AS is_partial_day
FROM ev
LEFT JOIN se ON ev.event_date = se.event_date;

DROP TABLE IF EXISTS item_summary;

CREATE TABLE item_summary AS
SELECT item_id,
       SUM(CASE WHEN event_type = 'view'        THEN 1 ELSE 0 END) AS views,
       SUM(CASE WHEN event_type = 'addtocart'   THEN 1 ELSE 0 END) AS addtocarts,
       SUM(CASE WHEN event_type = 'transaction' THEN 1 ELSE 0 END) AS transactions,
       MIN(event_date) AS first_seen,
       MAX(event_date) AS last_seen
FROM clean_events
GROUP BY item_id
