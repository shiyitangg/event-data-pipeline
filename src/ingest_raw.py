"""Step 1b: load events.csv into the RAW layer: typed columns, partitioned Parquet by event date.
Usage: python src/ingest_raw.py [path_to_events.csv]
Idempotent: removes and rebuilds the raw layer on each run. Nothing is dropped; row counts are reconciled."""
import shutil, sys, time
from pathlib import Path
from config import RAW_CSV, LAKE_RAW, SOURCE_COLUMNS

try:
    import duckdb
except ImportError:
    sys.exit("DuckDB is not installed. Run: python3 -m pip install duckdb")

csv = Path(sys.argv[1]) if len(sys.argv) > 1 else RAW_CSV
if not csv.exists():
    sys.exit(f"File not found: {csv}")

con = duckdb.connect()
src = f"read_csv_auto('{csv.as_posix()}')"

found = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()]
missing = [c for c in SOURCE_COLUMNS if c not in found]
if missing:
    sys.exit(f"Source file is missing expected columns {missing}. Columns found: {found}\n"
             f"Check reports/data_profile.md and update SOURCE_COLUMNS in src/config.py.")

t0 = time.time()
rows_in = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]

if LAKE_RAW.exists():
    shutil.rmtree(LAKE_RAW)
LAKE_RAW.parent.mkdir(parents=True, exist_ok=True)

con.execute(f"""
COPY (
    SELECT
        CAST("timestamp" AS BIGINT)                                   AS event_ts_ms,
        epoch_ms(CAST("timestamp" AS BIGINT))                         AS event_ts,
        CAST(epoch_ms(CAST("timestamp" AS BIGINT)) AS DATE)           AS event_date,
        CAST(visitorid AS BIGINT)                                     AS visitor_id,
        CAST(event AS VARCHAR)                                        AS event_type,
        CAST(itemid AS BIGINT)                                        AS item_id,
        TRY_CAST(transactionid AS BIGINT)                             AS transaction_id,
        '{csv.name}'                                                  AS _source_file,
        now()                                                         AS _ingested_at
    FROM {src}
) TO '{LAKE_RAW.as_posix()}' (FORMAT PARQUET, PARTITION_BY (event_date))
""")

rows_out = con.execute(
    f"SELECT COUNT(*) FROM read_parquet('{LAKE_RAW.as_posix()}/*/*.parquet', hive_partitioning=true)").fetchone()[0]
parts = con.execute(
    f"SELECT COUNT(DISTINCT event_date) FROM read_parquet('{LAKE_RAW.as_posix()}/*/*.parquet', hive_partitioning=true)").fetchone()[0]

print(f"Rows in CSV:        {rows_in:,}")
print(f"Rows in raw layer:  {rows_out:,}   across {parts} daily partitions")
print(f"Elapsed: {time.time() - t0:.1f}s")
if rows_in != rows_out:
    sys.exit("RECONCILIATION FAILED: row counts differ between source and raw layer.")
print("Reconciliation passed: no rows lost.")
