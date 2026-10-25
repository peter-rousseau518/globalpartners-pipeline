import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from lib.data import table, top_discounted_items
from lib.ui import SERIES, location_labels, money, page, show_table, style

page("Pricing and Discount Effectiveness",
     "How are discounts affecting order volume, gross revenue and net revenue?")
st.caption("A discount is an item sold at least $0.05 below its usual price — the most common price for that item "
           "at that location in that month. Gross = revenue at usual prices; net = what customers paid.")

dm = table("mart_discount_monthly")
dm["month_dt"] = pd.to_datetime(dm["month"] + "-01")
tot = dm.groupby("order_type")[["orders", "gross_revenue", "discount_amt", "net_revenue"]].sum()
disc = tot.loc["Discounted"] if "Discounted" in tot.index else pd.Series(0, index=tot.columns)

c = st.columns(4)
c[0].metric("Orders with a discount", f"{disc['orders'] / tot['orders'].sum():.1%}")
c[0].caption(f"{int(disc['orders']):,} orders")
c[1].metric("Total discounts given", money(disc["discount_amt"]))
c[2].metric("Discount as % of gross revenue", f"{tot['discount_amt'].sum() / tot['gross_revenue'].sum():.2%}")
c[3].metric("Avg discount per discounted order", money(disc["discount_amt"] / max(disc["orders"], 1), 2))

left, right = st.columns(2)
with left:
    t = tot.reindex(["Full price", "Discounted"])
    fig = go.Figure()
    for i, (field, label) in enumerate([("gross_revenue", "Gross (usual prices)"), ("net_revenue", "Net (paid)")]):
        fig.add_trace(go.Bar(x=t.index, y=t[field], name=label, marker_color=SERIES[i],
                             hovertemplate=f"{label}<br>%{{x}}: $%{{y:,.0f}}<extra></extra>"))
    fig.update_layout(title="Gross vs net revenue: full-price vs discounted orders", barmode="group")
    fig.update_yaxes(tickprefix="$", tickformat=",.0f")
    st.plotly_chart(style(fig), use_container_width=True)
with right:
    t2 = tot.reindex(["Full price", "Discounted"])
    aov_g = t2["gross_revenue"] / t2["orders"]
    aov_n = t2["net_revenue"] / t2["orders"]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=t2.index, y=aov_g, name="Gross per order", marker_color=SERIES[0],
                         hovertemplate="%{x}: $%{y:.2f} gross per order<extra></extra>"))
    fig.add_trace(go.Bar(x=t2.index, y=aov_n, name="Net per order", marker_color=SERIES[1],
                         hovertemplate="%{x}: $%{y:.2f} net per order<extra></extra>"))
    fig.update_layout(title="Order value: full-price vs discounted orders", barmode="group")
    fig.update_yaxes(tickprefix="$")
    st.plotly_chart(style(fig), use_container_width=True)

left, right = st.columns(2)
with left:
    m = dm.groupby("month_dt").agg(disc=("discount_amt", "sum"), gross=("gross_revenue", "sum")).reset_index()
    fig = go.Figure(go.Scatter(x=m["month_dt"], y=m["disc"], mode="lines", line_color=SERIES[0], name="Discounts",
                               hovertemplate="%{x|%b %Y}: $%{y:,.0f} in discounts<extra></extra>"))
    fig.update_layout(title="Discounts given per month")
    fig.update_yaxes(tickprefix="$", tickformat=",.0f")
    st.plotly_chart(style(fig, legend=False), use_container_width=True)
with right:
    share = dm.pivot_table(index="month_dt", columns="order_type", values="orders", aggfunc="sum").fillna(0)
    share["pct"] = share.get("Discounted", 0) / share.sum(axis=1)
    fig = go.Figure(go.Scatter(x=share.index, y=share["pct"], mode="lines", line_color=SERIES[0], name="Share",
                               hovertemplate="%{x|%b %Y}: %{y:.1%} of orders discounted<extra></extra>"))
    fig.update_layout(title="Share of orders with a discount")
    fig.update_yaxes(tickformat=".0%")
    st.plotly_chart(style(fig, legend=False), use_container_width=True)

st.subheader("Do customers order more after their first discount?")
eff = table("mart_discount_effect")
w = eff["customers"]
before = (eff["avg_orders_30d_before"] * w).sum() / w.sum()
after = (eff["avg_orders_30d_after"] * w).sum() / w.sum()
left, right = st.columns([1, 2])
with left:
    fig = go.Figure(go.Bar(x=["30 days before", "30 days after"], y=[before, after], marker_color=[SERIES[1], SERIES[0]],
                           text=[f"{before:.2f}", f"{after:.2f}"], textposition="outside",
                           hovertemplate="%{x}: %{y:.2f} orders per customer<extra></extra>"))
    fig.update_layout(title="Orders per customer around first discount")
    st.plotly_chart(style(fig, height=340, legend=False), use_container_width=True)
with right:
    items = top_discounted_items(12)[::-1]
    fig = go.Figure(go.Bar(y=items["item_name"], x=items["discount_amt"], orientation="h", marker_color=SERIES[0],
                           customdata=items["discounted_lines"],
                           hovertemplate="%{y}: $%{x:,.0f} over %{customdata:,} lines<extra></extra>"))
    fig.update_layout(title="Most-discounted items (total discount $)")
    fig.update_xaxes(tickprefix="$", tickformat=",.0f")
    st.plotly_chart(style(fig, height=340, legend=False), use_container_width=True)
st.caption(f"Based on {int(w.sum()):,} customers. Before/after compares the same customers in the 30 days on each side of their first discounted order; "
           "it shows association, not proof that the discount caused the change.")

bl = dm.groupby("restaurant_id").agg(disc=("discount_amt", "sum"), gross=("gross_revenue", "sum")).reset_index()
bl["pct"] = bl["disc"] / bl["gross"]
bl["location"] = bl["restaurant_id"].map(location_labels(bl["restaurant_id"].unique()))
show_table(bl.sort_values("pct", ascending=False)[["location", "disc", "gross", "pct"]].round(4),
           "Discount rate by location")
