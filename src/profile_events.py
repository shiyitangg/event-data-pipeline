"""Step 1a: profile the source file BEFORE building anything on top of it.
Usage: python src/profile_events.py [path_to_events.csv]
Writes reports/data_profile.md and prints the same summary."""
import sys
from pathlib import Path
import pandas as pd
from config import RAW_CSV, REPORTS, SOURCE_COLUMNS, EVENT_TYPES

path = Path(sys.argv[1]) if len(sys.argv) > 1 else RAW_CSV
df = pd.read_csv(path)
lines = []
def out(s=""):
    print(s); lines.append(s)

out(f"# Data profile: {path.name}\n")
out(f"Rows: {len(df):,}")
out(f"Columns found: {list(df.columns)}")
missing = [c for c in SOURCE_COLUMNS if c not in df.columns]
extra = [c for c in df.columns if c not in SOURCE_COLUMNS]
out(f"Expected columns missing: {missing or 'none'}")
out(f"Unexpected extra columns: {extra or 'none'}\n")

out("## Types and nulls")
prof = pd.DataFrame({"dtype": df.dtypes.astype(str), "nulls": df.isna().sum(),
                     "null_pct": (df.isna().mean() * 100).round(2), "distinct": df.nunique()})
out(prof.to_string()); out()

if "event" in df.columns:
    out("## Event types")
    out(df["event"].value_counts(dropna=False).to_string())
    unknown = set(df["event"].dropna().unique()) - EVENT_TYPES
    out(f"Event values outside the expected set {sorted(EVENT_TYPES)}: {sorted(unknown) or 'none'}\n")

if "timestamp" in df.columns:
    ts = pd.to_datetime(df["timestamp"], unit="ms", utc=True, errors="coerce")
    out("## Time range (UTC)")
    out(f"min: {ts.min()}   max: {ts.max()}   unparseable: {int(ts.isna().sum())}")
    per_day = ts.dt.date.value_counts().sort_index()
    out(f"days with data: {len(per_day)}   events/day  min={per_day.min():,}  median={int(per_day.median()):,}  max={per_day.max():,}")
    first, last = per_day.index.min(), per_day.index.max()
    all_days = pd.date_range(first, last).date
    gaps = [d for d in all_days if d not in set(per_day.index)]
    out(f"calendar days with zero events inside the range: {len(gaps)} {gaps[:10]}\n")

out("## Duplicates")
out(f"fully identical rows: {int(df.duplicated().sum()):,}\n")

if {"event", "transactionid"} <= set(df.columns):
    out("## transactionid presence by event type")
    out(df.assign(has_txn=df["transactionid"].notna()).groupby("event")["has_txn"]
        .agg(rows="size", with_transaction_id="sum").to_string())

REPORTS.mkdir(parents=True, exist_ok=True)
(REPORTS / "data_profile.md").write_text("\n".join(lines))
print(f"\nSaved {REPORTS / 'data_profile.md'}")
