"""Step 3: sessionization and metric marts, built from the clean layer.
Usage: python src/build_marts.py
Rebuilds events_sessionized, sessions, daily_metrics, item_summary and reconciles them against clean_events.
Trade-off (documented): these layers are rebuilt in full each run. The clean layer is the incremental one."""
import sys, time, uuid
from datetime import datetime
from config import DB_PATH, ROOT, SESSION_GAP_MINUTES

try:
    import duckdb
except ImportError:
    sys.exit("DuckDB is not installed. Run: python3 -m pip install duckdb")

if not DB_PATH.exists():
    sys.exit("Warehouse not found. Run: python3 src/clean.py")
con = duckdb.connect(str(DB_PATH))
if con.execute("SELECT COUNT(*) FROM clean_events").fetchone()[0] == 0:
    sys.exit("clean_events is empty. Run: python3 src/clean.py")

def statements(path, **kw):
    raw = (ROOT / "sql" / path).read_text()
    text = "\n".join(l for l in raw.splitlines() if not l.strip().startswith("--")).format(**kw)
    return [s.strip() for s in text.split(";") if s.strip()]

run_id = uuid.uuid4().hex[:8]
def log(step, status, rows_in, rows_out, started, err=None):
    con.execute("INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                [run_id, step, None, status, rows_in, 0, 0, rows_out, started, datetime.now(), err])

clean_n = con.execute("SELECT COUNT(*) FROM clean_events").fetchone()[0]
gap_kw = dict(gap_min=SESSION_GAP_MINUTES, gap_ms=SESSION_GAP_MINUTES * 60 * 1000)

# ---- sessions
started = datetime.now(); t0 = time.time()
for s in statements("03_sessions.sql", **gap_kw):
    con.execute(s)
n_sess_events = con.execute("SELECT COUNT(*) FROM events_sessionized").fetchone()[0]
n_sessions = con.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
sum_events = con.execute("SELECT SUM(n_events) FROM sessions").fetchone()[0]
problems = []
if n_sess_events != clean_n: problems.append(f"events_sessionized has {n_sess_events:,} rows, clean_events has {clean_n:,}")
if sum_events != clean_n:    problems.append(f"sessions.n_events sums to {sum_events:,}, clean_events has {clean_n:,}")
neg = con.execute("SELECT COUNT(*) FROM sessions WHERE duration_seconds < 0").fetchone()[0]
if neg: problems.append(f"{neg:,} sessions have negative duration")
if problems:
    log("sessions", "failed", clean_n, n_sessions, started, "; ".join(problems))
    sys.exit("SESSION CHECKS FAILED: " + "; ".join(problems))
log("sessions", "success", clean_n, n_sessions, started)
print(f"sessions built in {time.time() - t0:.1f}s: {n_sessions:,} sessions from {clean_n:,} events")

# ---- marts
started = datetime.now(); t0 = time.time()
for s in statements("04_marts.sql"):
    con.execute(s)
n_days = con.execute("SELECT COUNT(*) FROM daily_metrics").fetchone()[0]
dm_events = con.execute("SELECT SUM(events) FROM daily_metrics").fetchone()[0]
dm_sessions = con.execute("SELECT SUM(sessions) FROM daily_metrics").fetchone()[0]
problems = []
if dm_events != clean_n:     problems.append(f"daily_metrics.events sums to {dm_events:,}, expected {clean_n:,}")
if dm_sessions != n_sessions: problems.append(f"daily_metrics.sessions sums to {dm_sessions:,}, expected {n_sessions:,}")
if problems:
    log("marts", "failed", clean_n, n_days, started, "; ".join(problems))
    sys.exit("MART CHECKS FAILED: " + "; ".join(problems))
log("marts", "success", clean_n, n_days, started)
print(f"marts built in {time.time() - t0:.1f}s: daily_metrics={n_days} days, "
      f"item_summary={con.execute('SELECT COUNT(*) FROM item_summary').fetchone()[0]:,} items")
print("Reconciliation passed: events and sessions tie out across all layers.\n")

print("Session shape:")
print(con.execute("""SELECT COUNT(*) AS sessions,
        ROUND(AVG(n_events), 2) AS mean_events, median(n_events) AS median_events,
        ROUND(100.0 * AVG(CASE WHEN n_events = 1 THEN 1 ELSE 0 END), 1) AS pct_single_event,
        ROUND(quantile_cont(duration_seconds, 0.95), 0) AS p95_duration_s,
        ROUND(100.0 * AVG(has_txn), 2) AS pct_with_purchase
        FROM sessions""").df().to_string(index=False))

print("\nDaily metrics (first 3 and last 3 days, partial days flagged):")
dm = con.execute("""SELECT event_date, dau, events, sessions, cart_session_rate, purchase_session_rate,
                    cart_to_purchase_rate, is_partial_day FROM daily_metrics ORDER BY event_date""").df()
print(dm.head(3).to_string(index=False)); print(dm.tail(3).to_string(index=False))
full = dm[dm.is_partial_day == 0]
print(f"\nFull days only ({len(full)}): median DAU={int(full.dau.median()):,}, "
      f"median purchase_session_rate={full.purchase_session_rate.median():.4f}")
