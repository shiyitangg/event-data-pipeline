"""Data-quality rules as pure functions over pandas DataFrames (no database needed, so they are unit-testable).
Every function returns a list of result dicts with the same keys:
    check_name, severity, event_date, status, metric_value, threshold, detail
status is one of: pass, warn, fail, skip.  A violated 'critical' check is a fail, a violated 'warning' check is a warn."""
import pandas as pd

CRITICAL, WARNING = "critical", "warning"

def _res(check, severity, status, value=None, threshold=None, detail="", event_date=None):
    return dict(check_name=check, severity=severity, event_date=event_date, status=status,
                metric_value=None if value is None else float(value),
                threshold=None if threshold is None else float(threshold), detail=detail)

def _judge(severity, violated):
    if not violated:
        return "pass"
    return "fail" if severity == CRITICAL else "warn"

def _as_dates(s):
    return pd.to_datetime(s).dt.date

# ---------------------------------------------------------------- run-level checks
def schema_check(found_types: dict, expected_types: dict):
    problems = []
    for col, typ in expected_types.items():
        if col not in found_types:
            problems.append(f"missing column {col}")
        elif found_types[col].upper() != typ.upper():
            problems.append(f"{col} is {found_types[col]}, expected {typ}")
    extra = [c for c in found_types if c not in expected_types and not c.startswith("_")]
    if extra:
        problems.append(f"unexpected columns {extra}")
    return [_res("schema_columns", CRITICAL, _judge(CRITICAL, bool(problems)), len(problems), 0,
                 "; ".join(problems) or "all expected columns and types present")]

def continuity_check(dates):
    ds = sorted(set(pd.to_datetime(list(dates)).date))
    if not ds:
        return [_res("partition_continuity", WARNING, "skip", detail="no partitions")]
    full = pd.date_range(ds[0], ds[-1]).date
    missing = [d for d in full if d not in set(ds)]
    detail = f"missing dates: {[str(d) for d in missing[:10]]}" if missing else f"no gaps between {ds[0]} and {ds[-1]}"
    return [_res("partition_continuity", WARNING, _judge(WARNING, bool(missing)), len(missing), 0, detail)]

def freshness_check(latest_date, as_of, max_lag_days):
    lag = (pd.Timestamp(as_of) - pd.Timestamp(latest_date)).days
    return [_res("freshness_lag_days", WARNING, _judge(WARNING, lag > max_lag_days), lag, max_lag_days,
                 f"latest partition {latest_date}, as-of {as_of}")]

def session_checks(total, negative, long_count, long_share_warn):
    share = long_count / total if total else 0
    return [
        _res("session_negative_duration", CRITICAL, _judge(CRITICAL, negative > 0), negative, 0,
             f"{negative:,} of {total:,} sessions"),
        _res("session_over_24h_share", WARNING, _judge(WARNING, share > long_share_warn), share, long_share_warn,
             f"{long_count:,} of {total:,} sessions last more than 24h"),
    ]

# ---------------------------------------------------------------- per-date checks
def volume_flags(rows_by_date: pd.Series, partial_dates, window, min_history, low, high):
    """Compare each full day's row count with the median of the previous `window` full days."""
    s = rows_by_date.copy(); s.index = pd.to_datetime(s.index).date
    partial = set(pd.to_datetime(list(partial_dates)).date)
    full = s[[d not in partial for d in s.index]].sort_index()
    baseline = full.shift(1).rolling(window, min_periods=min_history).median()
    out = []
    for d, n in s.sort_index().items():
        if d in partial:
            out.append(_res("daily_volume", WARNING, "skip", n, None, "partial day (first or last partition)", d)); continue
        b = baseline.get(d)
        if pd.isna(b) or b == 0:
            out.append(_res("daily_volume", WARNING, "skip", n, None, f"fewer than {min_history} prior full days", d)); continue
        ratio = n / b
        out.append(_res("daily_volume", WARNING, _judge(WARNING, ratio < low or ratio > high), ratio, None,
                        f"{int(n):,} rows vs trailing median {b:,.0f} (allowed ratio {low} to {high})", d))
    return out

def per_date_checks(metrics: pd.DataFrame, dups: pd.DataFrame, recon: pd.DataFrame, dup_rate_warn: float):
    """metrics: event_date, n_rows, null_key_rows, bad_event_type, bad_ts, date_mismatch, txn_rule_violations
       dups:    event_date, dup_rows
       recon:   event_date, raw_rows, clean_rows, quarantine_rows, dups_logged (NaN when no successful clean run)"""
    m = metrics.copy(); m["event_date"] = _as_dates(m["event_date"])
    d = dups.copy(); d["event_date"] = _as_dates(d["event_date"])
    r = recon.copy(); r["event_date"] = _as_dates(r["event_date"])
    dup_by_date = dict(zip(d["event_date"], d["dup_rows"]))
    recon_by_date = {row.event_date: row for row in r.itertuples()}
    out = []
    for row in m.itertuples():
        dt = row.event_date
        out.append(_res("null_key_fields", CRITICAL, _judge(CRITICAL, row.null_key_rows > 0), row.null_key_rows, 0,
                        f"{int(row.null_key_rows):,} rows with a null key field", dt))
        out.append(_res("event_type_valid", CRITICAL, _judge(CRITICAL, row.bad_event_type > 0), row.bad_event_type, 0,
                        f"{int(row.bad_event_type):,} rows with an unknown event type", dt))
        bad = row.bad_ts + row.date_mismatch
        out.append(_res("timestamp_valid", CRITICAL, _judge(CRITICAL, bad > 0), bad, 0,
                        f"{int(row.bad_ts):,} out of range, {int(row.date_mismatch):,} outside their partition date", dt))
        out.append(_res("transaction_id_rule", WARNING, _judge(WARNING, row.txn_rule_violations > 0), row.txn_rule_violations, 0,
                        f"{int(row.txn_rule_violations):,} rows break the transaction_id rule", dt))
        rate = dup_by_date.get(dt, 0) / row.n_rows if row.n_rows else 0
        out.append(_res("duplicate_rate", WARNING, _judge(WARNING, rate > dup_rate_warn), rate, dup_rate_warn,
                        f"{int(dup_by_date.get(dt, 0)):,} duplicate rows of {int(row.n_rows):,}", dt))
        rc = recon_by_date.get(dt)
        if rc is None or pd.isna(rc.dups_logged):
            out.append(_res("row_reconciliation", CRITICAL, "fail", None, 0, "no successful clean run logged for this date", dt))
        else:
            expected = rc.clean_rows + rc.quarantine_rows + rc.dups_logged
            diff = rc.raw_rows - expected
            out.append(_res("row_reconciliation", CRITICAL, _judge(CRITICAL, diff != 0), diff, 0,
                            f"raw {int(rc.raw_rows):,} = clean {int(rc.clean_rows):,} + quarantined {int(rc.quarantine_rows):,} "
                            f"+ duplicates {int(rc.dups_logged):,}", dt))
    return out
