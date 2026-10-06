"""Step 5a: build an ISOLATED faulty copy of 30 days of the raw layer, with known, labeled faults.
The real raw layer and warehouse are never touched. Output: data/faulty/lake/raw and data/faulty/answer_key.json.
ALL injected data is synthetic corruption of real rows, created only to test the monitoring."""
import json, shutil, sys
from config import REAL_LAKE_RAW, FAULTY_DIR

try:
    import duckdb
except ImportError:
    sys.exit("DuckDB is not installed. Run: python3 -m pip install duckdb")

if not REAL_LAKE_RAW.exists():
    sys.exit("Real raw layer not found. Run: python3 src/ingest_raw.py")

con = duckdb.connect()
glob = f"{REAL_LAKE_RAW.as_posix()}/*/*.parquet"
con.execute(f"""
CREATE TABLE base AS
SELECT *, hash(event_ts_ms, visitor_id, item_id) % 10000 AS r      -- deterministic pseudo-random bucket per row. Each fault is isolated: transaction rows are never relabeled, because that would also break the transaction_id rule
FROM (SELECT CAST(event_date AS DATE) AS event_date, event_ts_ms, event_ts, visitor_id, event_type, item_id,
             transaction_id, _source_file, _ingested_at
      FROM read_parquet('{glob}', hive_partitioning = true))
WHERE event_date BETWEEN DATE '2015-06-01' AND DATE '2015-06-30'
""")
dates = [r[0] for r in con.execute("SELECT DISTINCT event_date FROM base ORDER BY 1").fetchall()]
if len(dates) < 30:
    sys.exit(f"Expected 30 daily partitions between 2015-06-01 and 2015-06-30, found {len(dates)}")
D = dict(duplicates=dates[4], bad_type=dates[7], null_key=dates[10], late=dates[13], txn=dates[16],
         volume=dates[19], missing=dates[22], bad_ts=dates[25])
iso = {k: v.isoformat() for k, v in D.items()}

con.execute(f"""
CREATE TABLE faulty AS
SELECT event_date,
       event_ts_ms,
       CASE WHEN event_date = DATE '{iso['late']}'   AND r < 100 THEN epoch_ms(event_ts_ms - 259200000)
            WHEN event_date = DATE '{iso['bad_ts']}' AND r < 30  THEN TIMESTAMP '1999-01-01 00:00:00'
            ELSE event_ts END AS event_ts,
       visitor_id,
       CASE WHEN event_date = DATE '{iso['bad_type']}' AND r < 50 AND event_type <> 'transaction' THEN 'purchase' ELSE event_type END AS event_type,
       CASE WHEN event_date = DATE '{iso['null_key']}' AND r < 50 THEN NULL ELSE item_id END AS item_id,
       CASE WHEN event_date = DATE '{iso['txn']}' AND event_type = 'transaction' AND r < 5000 THEN NULL
            ELSE transaction_id END AS transaction_id,
       _source_file, _ingested_at
FROM base
WHERE event_date <> DATE '{iso['missing']}'
  AND NOT (event_date = DATE '{iso['volume']}' AND r >= 2000)
UNION ALL
SELECT event_date, event_ts_ms, event_ts, visitor_id, event_type, item_id, transaction_id, _source_file, _ingested_at
FROM base
WHERE event_date = DATE '{iso['duplicates']}' AND r < 200
""")

lake = FAULTY_DIR / "lake" / "raw"
if FAULTY_DIR.exists():
    shutil.rmtree(FAULTY_DIR)
lake.parent.mkdir(parents=True, exist_ok=True)
con.execute(f"COPY faulty TO '{lake.as_posix()}' (FORMAT PARQUET, PARTITION_BY (event_date))")

answer_key = [
    dict(fault="2% of rows duplicated",            date=iso["duplicates"], check="duplicate_rate",       status="warn"),
    dict(fault="unknown event type 'purchase'",     date=iso["bad_type"],   check="event_type_valid",     status="fail"),
    dict(fault="null item_id on some rows",         date=iso["null_key"],   check="null_key_fields",      status="fail"),
    dict(fault="late-arriving events (3 days old)", date=iso["late"],       check="timestamp_valid",      status="fail"),
    dict(fault="transactions without transaction_id", date=iso["txn"],      check="transaction_id_rule",  status="warn"),
    dict(fault="volume dropped to about 20%",       date=iso["volume"],     check="daily_volume",         status="warn"),
    dict(fault="missing daily partition",           date=None,              check="partition_continuity", status="warn",
         detail_contains=iso["missing"]),
    dict(fault="implausible timestamp (year 1999)", date=iso["bad_ts"],     check="timestamp_valid",      status="fail"),
]
(FAULTY_DIR / "answer_key.json").write_text(json.dumps(answer_key, indent=2))
n_base = con.execute("SELECT COUNT(*) FROM base").fetchone()[0]
n_faulty = con.execute("SELECT COUNT(*) FROM faulty").fetchone()[0]
print(f"Faulty environment written to {FAULTY_DIR}")
print(f"  source rows (30 days): {n_base:,}   faulty rows: {n_faulty:,}   injected faults: {len(answer_key)}")
for a in answer_key:
    print(f"  - {a['date'] or 'run-level'}: {a['fault']}  -> expect {a['check']} = {a['status']}")
