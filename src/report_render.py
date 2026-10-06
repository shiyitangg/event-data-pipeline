"""Step 6: rendering only (no database): turns DataFrames into the dashboard image and the summary text."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

STATUS_COLORS = {"pass": "#2e7d32", "warn": "#f9a825", "fail": "#c62828", "skip": "#9e9e9e"}

def kpis(dm: pd.DataFrame, funnel: dict) -> dict:
    full = dm[dm.is_partial_day == 0]
    return dict(
        events=int(dm.events.sum()), sessions=int(dm.sessions.sum()), days=len(dm), full_days=len(full),
        median_dau=int(full.dau.median()),
        purchase_session_rate=dm.purchase_sessions.sum() / dm.sessions.sum(),
        cart_to_purchase=dm.cart_then_purchase_sessions.sum() / dm.cart_sessions.sum(),
        funnel=funnel)

def summary_text(k: dict, dq: pd.DataFrame, runs: pd.DataFrame) -> str:
    f = k["funnel"]
    lines = ["# Pipeline summary", "",
             f"- Events (clean layer): {k['events']:,} over {k['days']} UTC days ({k['full_days']} full days; first and last day are partial)",
             f"- Sessions: {k['sessions']:,} (30-minute inactivity rule)",
             f"- Median daily active visitors (full days): {k['median_dau']:,}",
             f"- Sessions with a purchase: {k['purchase_session_rate']:.2%}",
             f"- Among sessions with an add-to-cart, share that also have a purchase: {k['cart_to_purchase']:.1%}",
             f"- Sessions with a view / cart / purchase: {f['with_view']:,} / {f['with_cart']:,} / {f['with_purchase']:,}", ""]
    counts = dq.groupby("status")["n"].sum().to_dict()
    lines += ["## Data-quality monitoring (latest run)", "",
              "- " + ", ".join(f"{s}: {counts.get(s, 0):,}" for s in ("pass", "warn", "fail", "skip")), "",
              "## Latest pipeline runs", "", runs.to_string(index=False), ""]
    return "\n".join(lines)

def render_dashboard(dm: pd.DataFrame, funnel: dict, dq: pd.DataFrame, runs: pd.DataFrame, out_path) -> None:
    dm = dm.sort_values("event_date").copy()
    dm["event_date"] = pd.to_datetime(dm["event_date"])
    k = kpis(dm, funnel)
    full = dm[dm.is_partial_day == 0]
    partial = dm[dm.is_partial_day == 1]

    fig = plt.figure(figsize=(15, 11))
    gs = fig.add_gridspec(4, 2, height_ratios=[0.5, 2.2, 2.2, 1.5], hspace=0.55, wspace=0.22, top=0.93)
    fig.suptitle("Product Event Pipeline: metrics and data-quality monitoring (RetailRocket e-commerce clickstream)",
                 fontsize=14, fontweight="bold", y=0.975)

    # KPI strip
    ax = fig.add_subplot(gs[0, :]); ax.axis("off")
    tiles = [("Events", f"{k['events']:,}"), ("Sessions", f"{k['sessions']:,}"),
             ("Median daily visitors", f"{k['median_dau']:,}"),
             ("Sessions with purchase", f"{k['purchase_session_rate']:.2%}"),
             ("Cart sessions that purchase", f"{k['cart_to_purchase']:.1%}")]
    for i, (label, value) in enumerate(tiles):
        x = 0.01 + i * 0.2
        ax.text(x, 0.62, value, fontsize=19, fontweight="bold", transform=ax.transAxes)
        ax.text(x, 0.12, label, fontsize=10, color="#555", transform=ax.transAxes)

    # Daily events and visitors
    ax = fig.add_subplot(gs[1, 0])
    ax.plot(full.event_date, full.events, label="events", color="#1565c0", lw=1.4)
    ax.plot(full.event_date, full.dau, label="daily visitors", color="#ef6c00", lw=1.4)
    ax.scatter(partial.event_date, partial.events, marker="x", color="#c62828", zorder=3, label="partial day (excluded from line)")
    ax.set_title("Daily events and visitors (UTC days)"); ax.legend(fontsize=8); ax.grid(alpha=0.25)
    ax.tick_params(axis="x", labelrotation=30, labelsize=8)

    # Purchase rate with smoothing
    ax = fig.add_subplot(gs[1, 1])
    pr = full.set_index("event_date").purchase_session_rate * 100
    ax.plot(pr.index, pr.values, color="#bdbdbd", lw=0.9, label="daily")
    ax.plot(pr.index, pr.rolling(7, min_periods=3).mean().values, color="#2e7d32", lw=2, label="7-day average")
    ax.set_title("Share of sessions with a purchase (%)"); ax.legend(fontsize=8); ax.grid(alpha=0.25)
    ax.tick_params(axis="x", labelrotation=30, labelsize=8)

    # Funnel
    ax = fig.add_subplot(gs[2, 0])
    f = funnel
    labels = ["All sessions", "with a view", "with add-to-cart", "with purchase"]
    vals = [f["sessions"], f["with_view"], f["with_cart"], f["with_purchase"]]
    bars = ax.barh(labels[::-1], vals[::-1], color=["#2e7d32", "#ef6c00", "#1565c0", "#546e7a"])
    for b, v in zip(bars, vals[::-1]):
        ax.text(v, b.get_y() + b.get_height() / 2, f"  {v:,} ({v / f['sessions']:.1%})", va="center", fontsize=9)
    ax.set_xlim(0, max(vals) * 1.35); ax.set_title("Session funnel (sessions that contain each event type)")
    ax.grid(axis="x", alpha=0.25)

    # Data-quality status by check
    ax = fig.add_subplot(gs[2, 1])
    piv = dq.pivot_table(index="check_name", columns="status", values="n", aggfunc="sum", fill_value=0)
    for s in STATUS_COLORS:
        if s not in piv: piv[s] = 0
    piv = piv[list(STATUS_COLORS)]
    piv = piv.loc[piv.sum(axis=1).sort_values().index]
    left = pd.Series(0, index=piv.index)
    for s, c in STATUS_COLORS.items():
        ax.barh(piv.index, piv[s], left=left, color=c, label=s); left += piv[s]
    ax.set_title("Data-quality checks by result (latest run)"); ax.legend(fontsize=8, ncol=4, loc="lower right")
    ax.tick_params(axis="y", labelsize=8)

    # Pipeline run table
    ax = fig.add_subplot(gs[3, :]); ax.axis("off")
    ax.set_title("Latest pipeline runs", loc="left", fontsize=11)
    tbl = ax.table(cellText=runs.astype(str).values, colLabels=list(runs.columns), loc="upper left", cellLoc="left")
    tbl.auto_set_font_size(False); tbl.set_fontsize(9); tbl.scale(1, 1.4)

    fig.savefig(out_path, dpi=130, bbox_inches="tight")
    plt.close(fig)
