import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from lib.data import table
from lib.ui import SERIES, money, page, pct, show_table, style

page("Loyalty Program Impact",
     "How does loyalty membership affect customer spending, order value and repeat ordering?")

cust = table("mart_customer_current")
cust["Member"] = cust["ever_loyalty"].map({True: "Loyalty member"}).fillna("Non-member")
g = cust.groupby("Member").agg(customers=("user_id", "count"), avg_clv=("clv_to_date", "mean"),
                               avg_orders=("orders_to_date", "mean"), aov=("aov_to_date", "mean"),
                               repeat=("orders_to_date", lambda s: (s >= 2).mean()))
g = g.reindex(["Loyalty member", "Non-member"])
colors = {"Loyalty member": SERIES[0], "Non-member": SERIES[1]}

c = st.columns(4)
m, n = g.loc["Loyalty member"], g.loc["Non-member"]
c[0].metric("Average CLV — members", money(m["avg_clv"], 2), f"{m['avg_clv'] / n['avg_clv'] - 1:+.0%} vs non-members")
c[1].metric("Repeat-customer rate — members", pct(m["repeat"]), f"{(m['repeat'] - n['repeat']) * 100:+.1f} pts")
c[2].metric("Average order value — members", money(m["aov"], 2), f"{m['aov'] / n['aov'] - 1:+.0%}")
c[3].metric("Loyalty members", f"{int(m['customers']):,}")
c[3].caption(f"{m['customers'] / g['customers'].sum():.0%} of customers")
lm_all = table("mart_loyalty_monthly")
first_member = lm_all.loc[lm_all["is_loyalty"] == True, "month"].min()     # noqa: E712
st.caption("A customer counts as a loyalty member if they used a loyalty card on any order. "
           f"Note: loyalty card use first appears in {pd.to_datetime(first_member + '-01'):%B %Y}, so members are "
           "customers active since then, and lifetime comparisons partly reflect when people were customers. "
           "The monthly view below compares like-for-like periods.")

cols = st.columns(3)
for col, (field, title, fmt, tick) in zip(cols, [
        ("avg_clv", "Average lifetime value", lambda v: money(v, 0), "$"),
        ("avg_orders", "Average lifetime orders", lambda v: f"{v:.1f}", ""),
        ("repeat", "Share of customers with 2+ orders", lambda v: f"{v:.0%}", "")]):
    fig = go.Figure(go.Bar(x=g.index, y=g[field], marker_color=[colors[k] for k in g.index],
                           text=[fmt(v) for v in g[field]], textposition="outside",
                           hovertemplate="%{x}: %{text}<extra></extra>"))
    fig.update_layout(title=title)
    if field == "repeat":
        fig.update_yaxes(tickformat=".0%")
    else:
        fig.update_yaxes(tickprefix=tick)
    col.plotly_chart(style(fig, height=320, legend=False), use_container_width=True)

st.subheader("Month by month")
lm = table("mart_loyalty_monthly")
lm["month_dt"] = pd.to_datetime(lm["month"] + "-01")
left, right = st.columns(2)
for col, (field, title, yfmt) in zip([left, right], [
        ("avg_order_value", "Average order value: member vs non-member orders", "$"),
        ("repeat_rate", "Customers ordering 2+ times in the month", "%")]):
    fig = go.Figure()
    for label, flag in [("Loyalty member", True), ("Non-member", False)]:
        d = lm[lm["is_loyalty"] == flag].sort_values("month_dt")
        fig.add_trace(go.Scatter(x=d["month_dt"], y=d[field], mode="lines", name=label, line_color=colors[label],
                                 hovertemplate=f"{label}<br>%{{x|%b %Y}}: %{{y:{',.2f' if yfmt == '$' else '.1%'}}}"
                                               "<extra></extra>"))
    fig.update_layout(title=title)
    fig.update_yaxes(tickprefix="$" if yfmt == "$" else "", tickformat=".0%" if yfmt == "%" else None)
    col.plotly_chart(style(fig), use_container_width=True)
st.caption("Monthly view: an order is a member order when a loyalty card was used on it.")

show_table(g.reset_index().round(2), "Member vs non-member table")
