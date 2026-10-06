"""Shared paths and constants."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# PIPELINE_DATA_DIR lets the fault-injection test run the whole pipeline in an isolated copy.
DATA_DIR = Path(os.environ["PIPELINE_DATA_DIR"]) if "PIPELINE_DATA_DIR" in os.environ else ROOT / "data"
RAW_CSV = ROOT / "data" / "raw" / "events.csv"
REAL_LAKE_RAW = ROOT / "data" / "lake" / "raw"      # the real raw layer (never modified by tests)
FAULTY_DIR = ROOT / "data" / "faulty"               # isolated environment for injected faults
LAKE_RAW = DATA_DIR / "lake" / "raw"                # partitioned Parquet (raw layer)
DB_PATH = DATA_DIR / "warehouse.duckdb"
REPORTS = (DATA_DIR / "reports") if "PIPELINE_DATA_DIR" in os.environ else ROOT / "reports"

# Columns we expect in the source file (verified in Step 1)
SOURCE_COLUMNS = ["timestamp", "visitorid", "event", "itemid", "transactionid"]
EVENT_TYPES = {"view", "addtocart", "transaction"}
SESSION_GAP_MINUTES = 30

# Clean layer
PLAUSIBLE_MIN_TS = "2010-01-01"   # events earlier than this are rejected as implausible

# Data-quality thresholds
DQ_DUP_RATE_WARN = 0.001          # warn if more than 0.1% of a day's rows are exact duplicates
VOLUME_WINDOW_DAYS = 14           # trailing window for the daily-volume baseline
VOLUME_MIN_HISTORY_DAYS = 7       # need at least this many prior full days to judge volume
VOLUME_RATIO_LOW = 0.5            # warn if a day has < 50% of the trailing median
VOLUME_RATIO_HIGH = 2.0           # warn if a day has > 200% of the trailing median
FRESHNESS_MAX_LAG_DAYS = 1
LONG_SESSION_SECONDS = 24 * 3600
LONG_SESSION_SHARE_WARN = 0.0005  # warn if more than 0.05% of sessions last over 24 hours
EXPECTED_RAW_TYPES = {"event_ts_ms": "BIGINT", "event_ts": "TIMESTAMP", "event_date": "DATE",
                      "visitor_id": "BIGINT", "event_type": "VARCHAR", "item_id": "BIGINT",
                      "transaction_id": "BIGINT"}
