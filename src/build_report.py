"""Step 6: build the dashboard image and summary from the real warehouse.
Usage: python src/build_report.py
Writes reports/dashboard.png and reports/summary.md."""
import sys
from config import DB_PATH, REPORTS
from report_render import render_dashboard, summary_text, kpis

try:
    import duckdb
except ImportError:
    sys.exit("DuckDB is not installed. Run: python3 -m pip install duckdb")
if not DB_PATH.exists():
    sys.exit("Warehouse not found. Run the pipeline first (clean.py, build_marts.py, quality_checks.py).")

con = duckdb.connect(str(DB_PATH), read_only=True)
dm = con.execute("SELECT * FROM daily_metrics ORDER BY event_date").df()
fn = con.execute("""SELECT COUNT(*) AS sessions, SUM(has_view) AS with_view, SUM(has_cart) AS with_cart,
                           SUM(has_txn) AS with_purchase FROM sessions""").fetchone()
funnel = dict(sessions=int(fn[0]), with_view=int(fn[1]), with_cart=int(fn[2]), with_purchase=int(fn[3]))
dq = con.execute("""SELECT check_name, severity, status, COUNT(*) AS n FROM dq_results
                    WHERE run_id = (SELECT run_id FROM dq_results ORDER BY checked_at DESC LIMIT 1)
                    GROUP BY check_name, severity, status""").df()
runs = con.execute("""
    WITH last AS (
        SELECT step, run_id FROM (
            SELECT step, run_id, ROW_NUMBER() OVER (PARTITION BY step ORDER BY MAX(finished_at) DESC) AS rn
            FROM pipeline_runs GROUP BY step, run_id) WHERE rn = 1)
    SELECT p.step AS step, p.status AS status, COUNT(*) AS runs, SUM(p.rows_in) AS rows_in,
           SUM(p.rows_rejected) AS rejected, SUM(p.duplicates_removed) AS dups_removed, SUM(p.rows_out) AS rows_out
    FROM pipeline_runs p JOIN last l ON p.step = l.step AND p.run_id = l.run_id
    GROUP BY p.step, p.status ORDER BY p.step""").df()
for c in ("rows_in", "rejected", "dups_removed", "rows_out"):
    runs[c] = runs[c].map(lambda v: f"{int(v):,}")

REPORTS.mkdir(parents=True, exist_ok=True)
render_dashboard(dm, funnel, dq, runs, REPORTS / "dashboard.png")
text = summary_text(kpis(dm, funnel), dq, runs)
(REPORTS / "summary.md").write_text(text)
print(text)
print(f"\nSaved {REPORTS / 'dashboard.png'} and {REPORTS / 'summary.md'}")
