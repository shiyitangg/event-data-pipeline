# Design: Product Event Data Pipeline with Quality Monitoring (E-commerce Clickstream)

## Goal
Build a small but complete data platform for product event data: ingest raw events, validate and clean them,
load them into an analytics database, build product-metric tables, monitor data quality, and report results.

## Data source
RetailRocket e-commerce events (Kaggle, `events.csv`): visitor-level events (view / addtocart / transaction)
from an online retailer, about 4.5 months. Real data. No session id in the source, so sessions are derived.
Columns are verified in Step 1 (see `reports/data_profile.md`); this design adapts to what the file really contains.

## Layers
| Layer | Location | What it holds |
|---|---|---|
| raw | `data/lake/raw/event_date=YYYY-MM-DD/*.parquet` | Source events with standardized column names and types, partitioned by event date. Nothing dropped. |
| clean | DuckDB table `clean_events` | Deduplicated, validated events with a deterministic `event_id` and derived `session_id` |
| marts | DuckDB tables | `daily_metrics`, `funnel_daily`, `item_daily`, `sessions` |
| monitoring | DuckDB tables | `pipeline_runs` (one row per run and step), `dq_results` (one row per check per partition) |
| report | `reports/` | Dashboard image/HTML and a data-quality summary |

## Processing rules
- **Incremental by date (raw and clean layers).** The unit of work is one `event_date` partition.
- **Full rebuild (sessions and marts).** A session can cross midnight UTC, so sessions are derived from the whole clean table on each run. This is cheap at about 2.7M events and always consistent. At larger scale this would become incremental with a lookback window; that is a design note, not something implemented here.
- **Idempotent.** Re-running a date replaces that date's rows; running twice gives identical results.
- **Fail fast on critical checks;** warn on non-critical ones; every result is logged.
- **Sessionization.** A new session starts after 30 minutes of inactivity for the same visitor.

## Data-quality checks (planned)
| # | Check | Severity |
|---|---|---|
| 1 | Required columns present, expected types | critical |
| 2 | Row count in = row count out (reconciliation) | critical |
| 3 | No nulls in key fields (timestamp, visitor, event type, item) | critical |
| 4 | Event type in the allowed set | critical |
| 5 | Timestamp within the plausible range, not in the future | critical |
| 6 | `transaction_id` present exactly when event type is transaction | warning |
| 7 | Duplicate rate under a threshold | warning |
| 8 | Daily volume vs trailing median (anomaly) | warning |
| 9 | Freshness: latest partition is as expected | warning |
| 10 | Session sanity: non-negative durations | warning |

## Fault injection (testing the checks)
A separate script copies a clean partition and injects labeled faults: duplicates, late-arriving events,
missing fields, invalid event types, a drifted schema. The monitoring layer must flag each one.
All injected data is clearly marked as synthetic and never mixed into reported results.

## Out of scope (stated honestly)
Single-machine batch processing with DuckDB and Parquet. No streaming system, no cloud platform, no orchestrator.
README will include a short "how this would change at larger scale" section as design reasoning only.

## Step plan
1. Setup, data profiling, raw ingestion
2. Clean layer (types, dedup, validation)
3. Sessionization and metric marts
4. Data-quality checks, pipeline run log
5. Fault injection and tests
6. Dashboard and report
7. README, GitHub, resume bullets

## Findings from Step 1 (real data) that shape the design
- 2,756,101 events, 139 daily partitions (UTC), 3 event types, no missing days.
- 460 fully identical rows (exact duplicates). Identical rows share a timestamp, so they always fall in the same
  date partition; per-partition deduplication is therefore sufficient.
- The first and last UTC days are partial (data starts and ends at 03:00 UTC). The daily-volume anomaly check
  must exclude the first and last partitions or it will flag them falsely.
- A transaction id can appear on several rows (one row per item in the order). Those rows are NOT duplicates;
  the deduplication key includes item id.

## Step 2 results (real data)
- 2,756,101 rows in, 0 rejected, 460 exact duplicates removed (366 add-to-cart, 94 view, 0 transaction), 2,755,641 rows out.
- No transaction rows were duplicated, so order counts are not inflated by duplicate events.

## Step 3 results (real data)
- 1,761,675 sessions from 2,755,641 events (about 1.25 sessions per visitor). Median session has 1 event; 78.3% are single-event sessions.
- 0.81% of sessions contain a purchase. Full-day median DAU is 12,387 (137 full days; first and last day flagged as partial).
- Daily cart-to-purchase rates are noisy because a typical day has only a few hundred cart sessions.

## Step 4: monitoring design
- Rules live in `src/dq_rules.py` as pure functions over DataFrames (unit-testable). `src/quality_checks.py` runs the SQL, applies the rules,
  writes `dq_results` and `reports/dq_report.md`, and exits non-zero if any critical check fails.
- Critical (fail): schema and types, null key fields, unknown event types, timestamps out of range or outside their partition date, row reconciliation, negative session durations.
- Warning (warn): transaction_id rule, duplicate rate, daily volume vs trailing 14-day median, partition gaps, freshness, sessions over 24h.
- Freshness is judged against an as-of date because the source is a static historical dataset; in this project it is a simulated freshness check.

## Step 4 results (real data)
978 checks across 139 partitions: 0 failures, 0 warnings, 9 skipped (2 partial days, 7 days without enough history for the volume baseline).
A clean real-data run does not prove the checks work, which is why Step 5 injects known faults.

## Step 5: fault injection and tests
- `src/inject_faults.py` builds an isolated copy of 30 real days (`data/faulty/`) and corrupts it in eight known ways:
  2% duplicates, unknown event type, null key field, late-arriving events, transactions without id, volume drop to 20%,
  a missing daily partition, and an implausible (1999) timestamp. The real raw layer and warehouse are never modified.
- `src/run_fault_test.py` runs the whole pipeline on the faulty copy and compares alerts with the answer key:
  every fault must be detected, and there must be no unexpected alerts. It also requires the quality gate to exit non-zero.
- Clean layer change: rows whose timestamp falls outside their partition date are now quarantined (`timestamp_partition_mismatch`).
- Unit tests (`tests/`): data-quality rules (no database), sessionization and marts on hand-built cases, clean-layer validation,
  deduplication and idempotency.
- Not covered end to end: schema drift. The schema check is unit-tested but not demonstrated on injected files.

## Step 5 results (real data)
Fault-injection test: 8 of 8 injected faults detected, 0 unexpected alerts, and the quality gate stopped the pipeline (non-zero exit).
The first run showed one unexpected alert. It was caused by the test itself (the event-type fault relabeled a transaction row, which also broke
the transaction_id rule), so the injection was fixed to isolate each fault instead of loosening the check.
Re-running the clean layer after the rule change reproduced the same row count and checksum (idempotent).

## Step 6: report
`src/build_report.py` reads the real warehouse and writes `reports/dashboard.png` (KPIs, daily events and visitors, purchase-rate trend,
session funnel, data-quality results, latest pipeline runs) and `reports/summary.md`. Rendering is separated from querying so it can be tested with synthetic frames.
