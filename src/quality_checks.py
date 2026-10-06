"""Step 4: data-quality monitoring over the raw, clean and session layers.
Usage: python src/quality_checks.py [--as-of YYYY-MM-DD]
Writes every result to the dq_results table and to reports/dq_report.md.
Exits with a non-zero code if any CRITICAL check fails (so a scheduler would stop downstream steps).
Freshness is judged against --as-of (default: the latest partition), because the source is a static historical dataset."""
import argparse, sys, uuid
from datetime import datetime
import pandas as pd
from config import (DB_PATH, LAKE_RAW, ROOT, REPORTS, PLAUSIBLE_MIN_TS, DQ_DUP_RATE_WARN, VOLUME_WINDOW_DAYS,
                    VOLUME_MIN_HISTORY_DAYS, VOLUME_RATIO_LOW, VOLUME_RATIO_HIGH, FRESHNESS_MAX_LAG_DAYS,
                    LONG_SESSION_SECONDS, LONG_SESSION_SHARE_WARN, EXPECTED_RAW_TYPES)
import dq_rules as R

try:
    import duckdb
except ImportError:
    sys.exit("DuckDB is not installed. Run: python3 -m pip install duckdb")

ap = argparse.ArgumentParser()
ap.add_argument("--as-of", help="date used for the freshness check, YYYY-MM-DD (default: latest partition)")
args = ap.parse_args()

if not DB_PATH.exists():
    sys.exit("Warehouse not found. Run: python3 src/clean.py")

def statements(path, **kw):
    raw = (ROOT / "sql" / path).read_text()
    text = "\n".join(l for l in raw.splitlines() if not l.strip().startswith("--")).format(**kw)
    return [s.strip() for s in text.split(";") if s.strip()]

con = duckdb.connect(str(DB_PATH))
for s in statements("01_schema.sql", raw_glob=f"{LAKE_RAW.as_posix()}/*/*.parquet"):
    con.execute(s)

run_id = uuid.uuid4().hex[:8]
results = []

# ---- schema
found = {r[0]: r[1] for r in con.execute("DESCRIBE raw_events").fetchall()}
results += R.schema_check(found, EXPECTED_RAW_TYPES)

# ---- per-date raw metrics
metrics = con.execute(statements("05_dq_metrics.sql", min_ts=PLAUSIBLE_MIN_TS)[0]).df()
null_part = metrics[metrics["event_date"].isna()]
metrics = metrics[metrics["event_date"].notna()]
if len(null_part):
    results.append(R._res("null_partition_rows", R.CRITICAL, "fail", int(null_part["n_rows"].sum()), 0,
                          "rows without a valid event_date (default partition)"))
dups = con.execute("""SELECT event_date, SUM(n - 1) AS dup_rows FROM (
        SELECT event_date, COUNT(*) AS n FROM raw_events WHERE event_date IS NOT NULL
        GROUP BY event_date, event_ts_ms, visitor_id, event_type, item_id, transaction_id HAVING COUNT(*) > 1)
    GROUP BY event_date""").df()
recon = con.execute("""
    SELECT r.event_date, r.raw_rows,
           COALESCE(c.clean_rows, 0) AS clean_rows, COALESCE(q.q_rows, 0) AS quarantine_rows, l.duplicates_removed AS dups_logged
    FROM (SELECT event_date, COUNT(*) AS raw_rows FROM raw_events WHERE event_date IS NOT NULL GROUP BY 1) r
    LEFT JOIN (SELECT event_date, COUNT(*) AS clean_rows FROM clean_events GROUP BY 1) c USING (event_date)
    LEFT JOIN (SELECT event_date, COUNT(*) AS q_rows FROM quarantine_events GROUP BY 1) q USING (event_date)
    LEFT JOIN (SELECT event_date, duplicates_removed FROM (
                 SELECT event_date, duplicates_removed,
                        ROW_NUMBER() OVER (PARTITION BY event_date ORDER BY finished_at DESC) AS rn
                 FROM pipeline_runs WHERE step = 'clean' AND status = 'success' AND event_date IS NOT NULL)
               WHERE rn = 1) l USING (event_date)""").df()
results += R.per_date_checks(metrics, dups, recon, DQ_DUP_RATE_WARN)

# ---- volume, continuity, freshness
dates = pd.to_datetime(metrics["event_date"]).dt.date
rows_by_date = pd.Series(metrics["n_rows"].values, index=dates)
partial = {min(dates), max(dates)}
results += R.volume_flags(rows_by_date, partial, VOLUME_WINDOW_DAYS, VOLUME_MIN_HISTORY_DAYS,
                          VOLUME_RATIO_LOW, VOLUME_RATIO_HIGH)
results += R.continuity_check(dates)
as_of = args.as_of or str(max(dates))
results += R.freshness_check(max(dates), as_of, FRESHNESS_MAX_LAG_DAYS)

# ---- sessions
if con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'sessions'").fetchone()[0]:
    total, neg, long_n = con.execute(f"""SELECT COUNT(*), SUM(CASE WHEN duration_seconds < 0 THEN 1 ELSE 0 END),
            SUM(CASE WHEN duration_seconds > {LONG_SESSION_SECONDS} THEN 1 ELSE 0 END) FROM sessions""").fetchone()
    results += R.session_checks(total, int(neg or 0), int(long_n or 0), LONG_SESSION_SHARE_WARN)

# ---- persist
df = pd.DataFrame(results)
df.insert(0, "run_id", run_id); df["checked_at"] = datetime.now()
df["event_date"] = pd.to_datetime(df["event_date"])
con.register("dq_df", df)
con.execute("""INSERT INTO dq_results SELECT run_id, check_name, severity, CAST(event_date AS DATE), status,
               metric_value, threshold, detail, checked_at FROM dq_df""")

# ---- report
summary = df.pivot_table(index=["check_name", "severity"], columns="status", values="detail", aggfunc="count", fill_value=0)
for c in ("pass", "warn", "fail", "skip"):
    if c not in summary: summary[c] = 0
summary = summary[["pass", "warn", "fail", "skip"]]
issues = df[df.status.isin(["fail", "warn"])].sort_values(["status", "check_name", "event_date"])
lines = [f"# Data-quality report (run {run_id})", "", f"Checks executed: {len(df):,}   as-of date: {as_of}", "",
         "## Summary by check", "", summary.to_string(), ""]
lines += ["## Failures and warnings", ""]
if issues.empty:
    lines.append("None.")
else:
    for r in issues.head(40).itertuples():
        lines.append(f"- [{r.status.upper()}] {r.check_name} {'' if pd.isna(r.event_date) else str(r.event_date.date())}: {r.detail}")
    if len(issues) > 40:
        lines.append(f"... and {len(issues) - 40} more (see the dq_results table)")
REPORTS.mkdir(parents=True, exist_ok=True)
(REPORTS / "dq_report.md").write_text("\n".join(lines))
print("\n".join(lines))

n_fail = int((df.status == "fail").sum())
if n_fail:
    sys.exit(f"\n{n_fail} CRITICAL check(s) failed. Downstream steps must not run.")
print("\nNo critical failures.")
