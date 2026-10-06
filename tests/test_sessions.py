"""Sessionization and mart SQL tested on hand-built cases (DuckDB)."""
import datetime as dt
import pandas as pd
import pytest
duckdb = pytest.importorskip("duckdb")
from sqlutil import statements

MIN = 60 * 1000

def ms(y, m, d, h, mi):
    return int(dt.datetime(y, m, d, h, mi, tzinfo=dt.timezone.utc).timestamp() * 1000)

def build_events():
    rows = []
    def add(v, t, typ, item, txn=None):
        d = dt.datetime.fromtimestamp(t / 1000, dt.timezone.utc).replace(tzinfo=None)
        rows.append(dict(event_id=f"{v}-{t}-{typ}-{item}", event_ts_ms=t, event_ts=d, event_date=d.date(),
                         visitor_id=v, event_type=typ, item_id=item, transaction_id=txn))
    b = ms(2015, 6, 1, 10, 0)
    add(1, b, "view", 1); add(1, b + 10 * MIN, "view", 2); add(1, b + 20 * MIN, "addtocart", 2)   # session 1-1
    add(1, b + 51 * MIN, "view", 3); add(1, b + 55 * MIN, "transaction", 3, 900)                   # 31 min gap -> 1-2
    add(2, b, "view", 5); add(2, b + 30 * MIN, "view", 6)                                          # exactly 30 min -> same session
    t = ms(2015, 6, 1, 23, 50); add(3, t, "view", 7); add(3, t + 20 * MIN, "addtocart", 7)         # crosses midnight
    return pd.DataFrame(rows)

@pytest.fixture()
def con():
    c = duckdb.connect()
    df = build_events(); c.register("df", df)
    c.execute("CREATE TABLE clean_events AS SELECT * FROM df")
    for s in statements("03_sessions.sql", gap_min=30, gap_ms=30 * 60 * 1000): c.execute(s)
    for s in statements("04_marts.sql"): c.execute(s)
    return c

def test_session_boundaries(con):
    sessions = dict(con.execute("SELECT session_id, n_events FROM sessions").fetchall())
    assert sessions == {"1-1": 3, "1-2": 2, "2-1": 2, "3-1": 2}

def test_midnight_session_is_counted_on_first_day(con):
    d = con.execute("SELECT session_date FROM sessions WHERE session_id = '3-1'").fetchone()[0]
    assert d == dt.date(2015, 6, 1)

def test_marts_tie_out_to_events(con):
    assert con.execute("SELECT SUM(events) FROM daily_metrics").fetchone()[0] == 9
    assert con.execute("SELECT SUM(sessions) FROM daily_metrics").fetchone()[0] == 4
    assert con.execute("SELECT SUM(is_partial_day) FROM daily_metrics").fetchone()[0] == 2

def test_funnel_counts(con):
    cart, purchase = con.execute("""SELECT cart_sessions, purchase_sessions FROM daily_metrics
                                    WHERE event_date = DATE '2015-06-01'""").fetchone()
    assert (cart, purchase) == (2, 1)
