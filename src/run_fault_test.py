"""Step 5b: prove the monitoring works. Injects faults into an isolated copy, runs the whole pipeline on it,
and checks that every injected fault is detected and that nothing else alerts (no false positives).
Usage: python src/run_fault_test.py
Exit code 0 only if all faults were detected with zero unexpected alerts."""
import json, os, subprocess, sys
import pandas as pd
from config import ROOT, FAULTY_DIR

try:
    import duckdb
except ImportError:
    sys.exit("DuckDB is not installed. Run: python3 -m pip install duckdb")

def run(script, expect_rc, env=None):
    p = subprocess.run([sys.executable, str(ROOT / "src" / script)], env=env, cwd=ROOT, capture_output=True, text=True)
    if p.returncode != expect_rc:
        print(p.stdout[-1500:]); print(p.stderr[-1500:])
        sys.exit(f"{script} exited with {p.returncode}, expected {expect_rc}")
    return p

print("1) injecting faults into an isolated copy ...")
print(run("inject_faults.py", 0).stdout)

env = dict(os.environ, PIPELINE_DATA_DIR=str(FAULTY_DIR))
db = FAULTY_DIR / "warehouse.duckdb"
if db.exists():
    db.unlink()
print("2) running clean layer, marts, and quality checks on the faulty data ...")
run("clean.py", 0, env); run("build_marts.py", 0, env)
run("quality_checks.py", 1, env)   # critical faults are present, so exit code 1 is the correct outcome
print("   quality_checks stopped the pipeline with a non-zero exit code, as designed.\n")

key = json.loads((FAULTY_DIR / "answer_key.json").read_text())
con = duckdb.connect(str(db), read_only=True)
res = con.execute("SELECT check_name, event_date, status, detail FROM dq_results").df()
res["date"] = pd.to_datetime(res["event_date"]).dt.date.astype("string")
alerts = res[res.status.isin(["fail", "warn"])]

rows, matched_idx = [], set()
for k in key:
    cand = alerts[(alerts.check_name == k["check"]) & (alerts.status == k["status"])]
    cand = cand[cand.date == k["date"]] if k["date"] else cand[cand.detail.str.contains(k["detail_contains"], na=False)]
    ok = len(cand) > 0
    matched_idx.update(cand.index)
    rows.append((k["date"] or "run-level", k["fault"], f"{k['check']} = {k['status']}", "DETECTED" if ok else "MISSED"))
unexpected = alerts.drop(index=list(matched_idx))

report = ["# Fault-injection results", "", "| Date | Injected fault | Expected alert | Result |", "|---|---|---|---|"]
report += [f"| {d} | {f} | {e} | {r} |" for d, f, e, r in rows]
n_ok = sum(r == "DETECTED" for *_, r in rows)
report += ["", f"Detected {n_ok} of {len(rows)} injected faults. Unexpected alerts (false positives): {len(unexpected)}."]
if len(unexpected):
    report += ["", "Unexpected alerts:"] + [f"- {r.check_name} {r.date}: {r.status} {r.detail}" for r in unexpected.itertuples()]
text = "\n".join(report)
(ROOT / "reports").mkdir(exist_ok=True)
(ROOT / "reports" / "fault_injection_results.md").write_text(text)
print(text)
sys.exit(0 if n_ok == len(rows) and len(unexpected) == 0 else 1)
