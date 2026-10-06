"""Unit tests for the data-quality rules. No database needed."""
import datetime as dt
import numpy as np
import pandas as pd
import pytest
import dq_rules as R

DAYS = pd.date_range("2015-06-01", periods=30).date

def make_inputs():
    rng = np.random.default_rng(1)
    n = rng.integers(950, 1050, 30)
    metrics = pd.DataFrame(dict(event_date=DAYS, n_rows=n, null_key_rows=0, bad_event_type=0, bad_ts=0,
                                date_mismatch=0, txn_rule_violations=0))
    recon = pd.DataFrame(dict(event_date=DAYS, raw_rows=metrics.n_rows, clean_rows=metrics.n_rows,
                              quarantine_rows=0, dups_logged=0.0))
    dups = pd.DataFrame(dict(event_date=[], dup_rows=[]))
    return metrics, dups, recon

def status(results, check, day_index):
    df = pd.DataFrame(results)
    return df[(df.check_name == check) & (df.event_date == DAYS[day_index])].status.iloc[0]

def test_clean_data_has_no_alerts():
    m, d, r = make_inputs()
    res = R.per_date_checks(m, d, r, 0.001)
    assert {x["status"] for x in res} == {"pass"}

def test_unknown_event_type_is_critical():
    m, d, r = make_inputs(); m.loc[10, "bad_event_type"] = 5
    res = R.per_date_checks(m, d, r, 0.001)
    assert status(res, "event_type_valid", 10) == "fail" and status(res, "event_type_valid", 11) == "pass"

def test_null_keys_and_bad_timestamps_are_critical():
    m, d, r = make_inputs(); m.loc[3, "null_key_rows"] = 2; m.loc[4, "date_mismatch"] = 1; m.loc[5, "bad_ts"] = 1
    res = R.per_date_checks(m, d, r, 0.001)
    assert status(res, "null_key_fields", 3) == "fail"
    assert status(res, "timestamp_valid", 4) == "fail" and status(res, "timestamp_valid", 5) == "fail"

def test_transaction_rule_is_a_warning():
    m, d, r = make_inputs(); m.loc[14, "txn_rule_violations"] = 3
    assert status(R.per_date_checks(m, d, r, 0.001), "transaction_id_rule", 14) == "warn"

def test_duplicate_rate_threshold():
    m, d, r = make_inputs()
    d = pd.DataFrame(dict(event_date=[DAYS[16]], dup_rows=[40]))
    r.loc[16, ["clean_rows", "dups_logged"]] = [m.n_rows[16] - 40, 40]
    res = R.per_date_checks(m, d, r, 0.001)
    assert status(res, "duplicate_rate", 16) == "warn" and status(res, "duplicate_rate", 15) == "pass"

def test_reconciliation_mismatch_and_missing_run_fail():
    m, d, r = make_inputs(); r.loc[20, "clean_rows"] -= 7; r.loc[22, "dups_logged"] = np.nan
    res = R.per_date_checks(m, d, r, 0.001)
    assert status(res, "row_reconciliation", 20) == "fail" and status(res, "row_reconciliation", 22) == "fail"
    assert status(res, "row_reconciliation", 21) == "pass"

def test_volume_baseline_partial_days_and_history():
    n = np.full(30, 1000); n[12] = 300
    flags = pd.DataFrame(R.volume_flags(pd.Series(n, index=DAYS), {DAYS[0], DAYS[-1]}, 14, 7, 0.5, 2.0))
    get = lambda i: flags[flags.event_date == DAYS[i]].status.iloc[0]
    assert get(12) == "warn"                      # a real drop is flagged
    assert get(0) == "skip" and get(29) == "skip"  # partial first and last day
    assert get(3) == "skip"                        # not enough history yet
    assert get(15) == "pass"                       # one outlier does not poison later baselines (median)

def test_schema_check():
    exp = {"event_ts_ms": "BIGINT", "visitor_id": "BIGINT"}
    assert R.schema_check({"event_ts_ms": "BIGINT"}, exp)[0]["status"] == "fail"              # missing column
    assert R.schema_check({"event_ts_ms": "VARCHAR", "visitor_id": "BIGINT"}, exp)[0]["status"] == "fail"  # type drift
    ok = {"event_ts_ms": "BIGINT", "visitor_id": "BIGINT", "_source_file": "VARCHAR"}
    assert R.schema_check(ok, exp)[0]["status"] == "pass"                                      # metadata columns allowed

def test_continuity_and_freshness():
    gap = [d for d in DAYS if d != DAYS[7]]
    assert R.continuity_check(gap)[0]["status"] == "warn"
    assert R.continuity_check(DAYS)[0]["status"] == "pass"
    assert R.freshness_check(DAYS[-1], str(DAYS[-1]), 1)[0]["status"] == "pass"
    assert R.freshness_check(DAYS[-1], str(DAYS[-1] + dt.timedelta(days=5)), 1)[0]["status"] == "warn"

def test_session_checks():
    assert [x["status"] for x in R.session_checks(1000, 0, 0, 0.0005)] == ["pass", "pass"]
    assert [x["status"] for x in R.session_checks(1000, 2, 5, 0.0005)] == ["fail", "warn"]
