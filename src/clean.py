"""Step 2: build the CLEAN layer in DuckDB from the raw Parquet layer.
Usage:
    python src/clean.py                  # all daily partitions
    python src/clean.py --date 2015-06-01  # one partition (re-running replaces it)
Each partition is validated, rejected rows go to quarantine_events, exact duplicates are removed,
and one row per partition is written to pipeline_runs."""
import argparse, sys, time, uuid
from datetime import date, datetime
from config import DB_PATH, LAKE_RAW, ROOT, PLAUSIBLE_MIN_TS

try:
    import duckdb
except ImportError:
    sys.exit("DuckDB is not installed. Run: python3 -m pip install duckdb")

ap = argparse.ArgumentParser()
ap.add_argument("--date", help="process a single event_date, format YYYY-MM-DD")
args = ap.parse_args()

if not LAKE_RAW.exists():
    sys.exit(f"Raw layer not found at {LAKE_RAW}. Run: python3 src/ingest_raw.py")

def statements(path, **kw):
    raw = (ROOT / "sql" / path).read_text()
    # drop comment lines first, so a ';' inside a comment can never split a statement
    text = "\n".join(l for l in raw.splitlines() if not l.strip().startswith("--")).format(**kw)
    return [s.strip() for s in text.split(";") if s.strip()]

DB_PATH.parent.mkdir(parents=True, exist_ok=True)
con = duckdb.connect(str(DB_PATH))
raw_glob = f"{LAKE_RAW.as_posix()}/*/*.parquet"
for stmt in statements("01_schema.sql", raw_glob=raw_glob):
    con.execute(stmt)

all_dates = [r[0] for r in con.execute("SELECT DISTINCT event_date FROM raw_events WHERE event_date IS NOT NULL ORDER BY 1").fetchall()]
if args.date:
    wanted = date.fromisoformat(args.date)
    if wanted not in all_dates:
        sys.exit(f"No raw partition for {wanted}. Available range: {all_dates[0]} to {all_dates[-1]}")
    all_dates = [wanted]

run_id = uuid.uuid4().hex[:8]
t_start = time.time()
tot = dict(rows_in=0, rejected=0, dups=0, out=0)

def log_run(d, status, rows_in, rejected, dups, out, started, err=None):
    con.execute("INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                [run_id, "clean", d, status, rows_in, rejected, dups, out, started, datetime.now(), err])

for d in all_dates:
    started = datetime.now()
    rows_in = rejected = dups = out = 0
    try:
        stmts = statements("02_clean_partition.sql", d=d.isoformat(), min_ts=PLAUSIBLE_MIN_TS)
        con.execute(stmts[0])                                    # build staged table
        rows_in = con.execute("SELECT COUNT(*) FROM staged").fetchone()[0]
        rejected = con.execute("SELECT COUNT(*) FROM staged WHERE reject_reason IS NOT NULL").fetchone()[0]
        distinct_valid = con.execute("""SELECT COUNT(*) FROM (SELECT DISTINCT event_ts_ms, visitor_id, event_type,
                                        item_id, transaction_id FROM staged WHERE reject_reason IS NULL)""").fetchone()[0]
        for s in stmts[1:]:
            con.execute(s)
        out = con.execute("SELECT COUNT(*) FROM clean_events WHERE event_date = ?", [d]).fetchone()[0]
        dups = (rows_in - rejected) - out
        # Critical reconciliation: rows written must equal distinct valid keys computed independently
        if out != distinct_valid:
            raise RuntimeError(f"reconciliation failed for {d}: wrote {out}, expected {distinct_valid}")
        log_run(d, "success", rows_in, rejected, dups, out, started)
    except Exception as e:
        log_run(d, "failed", rows_in, rejected, dups, out, started, str(e))
        sys.exit(f"STOPPED at partition {d}: {e}")
    tot["rows_in"] += rows_in; tot["rejected"] += rejected; tot["dups"] += dups; tot["out"] += out

print(f"Run {run_id}: processed {len(all_dates)} partition(s) in {time.time() - t_start:.1f}s")
print(f"  rows in (raw):        {tot['rows_in']:,}")
print(f"  rejected (quarantine): {tot['rejected']:,}")
print(f"  duplicates removed:   {tot['dups']:,}")
print(f"  rows out (clean):     {tot['out']:,}")

print("\nDuplicates removed, by event type (from raw):")
print(con.execute("""SELECT event_type, SUM(n - 1) AS duplicate_rows FROM (
        SELECT event_type, COUNT(*) AS n FROM raw_events
        GROUP BY event_ts_ms, visitor_id, event_type, item_id, transaction_id HAVING COUNT(*) > 1)
    GROUP BY event_type ORDER BY 2 DESC""").df().to_string(index=False))

total, checksum = con.execute("SELECT COUNT(*), SUM(hash(event_id)) FROM clean_events").fetchone()
print(f"\nWarehouse state: clean_events={total:,} rows, checksum={checksum}")
print("(Run this script again: both numbers must stay identical. That is the idempotency check.)")
