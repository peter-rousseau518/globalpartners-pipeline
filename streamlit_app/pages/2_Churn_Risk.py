import plotly.graph_objects as go
import streamlit as st

from lib.data import churn_trend, table
from lib.ui import STATUS, STATUS_ORDER, TIER_ORDER, money, page, show_table, style

page("Churn Risk Indicators",
     "Which signals — days since last order, average gap between orders, spend trend — go with higher churn risk?")

cust = table("mart_customer_current")
counts = cust.groupby("churn_status").agg(customers=("user_id", "count"), clv=("clv_to_date", "sum")) \
             .reindex(STATUS_ORDER).fillna(0)

c = st.columns(4)
for i, s in enumerate(STATUS_ORDER):
    c[i].metric(f"{s} customers", f"{int(counts.at[s, 'customers']):,}")
    c[i].caption(f"{money(counts.at[s, 'clv'])} lifetime value")
st.caption("Active ≤ 30 days since last order · Cooling 31–45 · At risk 46–90, or more than 2× the customer's usual "
           "gap between orders · Lapsed > 90 days.")

left, right = st.columns(2)
with left:
    fig = go.Figure(go.Bar(x=STATUS_ORDER, y=counts["customers"], marker_color=[STATUS[s] for s in STATUS_ORDER],
                           text=counts["customers"].map("{:,.0f}".format), textposition="outside",
                           hovertemplate="%{x}: %{y:,} customers<extra></extra>"))
    fig.update_layout(title="Customers by churn status today")
    st.plotly_chart(style(fig, legend=False), use_container_width=True)
with right:
    trend = churn_trend()
    fig = go.Figure()
    for s in STATUS_ORDER:
        d = trend[trend["churn_status"] == s]
        fig.add_trace(go.Scatter(x=d["snapshot_date"], y=d["customers"], mode="lines", name=s,
                                 line_color=STATUS[s],
                                 hovertemplate=f"{s}<br>%{{x|%b %Y}}: %{{y:,}} customers<extra></extra>"))
    fig.update_layout(title="Churn status over time (first day of each month)")
    st.plotly_chart(style(fig), use_container_width=True)

st.subheader("Which signals go with churn?")
sig = (cust.groupby("churn_status")
           .agg(median_gap=("avg_gap_days", "median"), median_orders=("orders_to_date", "median"),
                median_clv=("clv_to_date", "median"))
           .reindex(STATUS_ORDER).reset_index())
cols = st.columns(3)
for col, (field, title, fmt) in zip(cols, [
        ("median_orders", "Median lifetime orders", "{:,.0f}"),
        ("median_gap", "Median days between orders", "{:,.1f}"),
        ("median_clv", "Median CLV", "${:,.0f}")]):
    fig = go.Figure(go.Bar(x=sig["churn_status"], y=sig[field], marker_color=[STATUS[s] for s in sig["churn_status"]],
                           text=sig[field].map(lambda v: fmt.format(v) if v == v else "—"), textposition="outside",
                           hovertemplate="%{x}: %{y:,.1f}<extra></extra>"))
    fig.update_layout(title=title)
    col.plotly_chart(style(fig, height=300, legend=False), use_container_width=True)
st.caption("Customers who churn typically placed few orders: one-time buyers have no gap between orders and "
           "drift straight to Lapsed.")

st.subheader("Re-engagement list")
f1, f2, f3 = st.columns(3)
statuses = f1.multiselect("Status", STATUS_ORDER, default=["At risk"])
tiers = f2.multiselect("CLV tier", TIER_ORDER, default=["High", "Medium"])
min_days = f3.slider("Minimum days since last order", 0, 365, 45)
alert = (cust[cust["churn_status"].isin(statuses) & cust["clv_tier"].isin(tiers)
              & (cust["days_since_last_order"] >= min_days)]
         .sort_values("clv_to_date", ascending=False)
         [["user_id", "churn_status", "clv_tier", "clv_to_date", "orders_to_date", "last_order_date",
           "days_since_last_order", "avg_gap_days", "spend_change_pct", "ever_loyalty"]])
st.markdown(f"**{len(alert):,} customers** match · **{money(alert['clv_to_date'].sum())}** lifetime value")
alert = alert.assign(last_order_date=alert["last_order_date"].dt.date, clv_to_date=alert["clv_to_date"].round(2))
st.dataframe(alert.head(500), use_container_width=True, hide_index=True)
st.download_button("Download full list (CSV)", alert.to_csv(index=False), "re_engagement_list.csv", "text/csv")
show_table(sig, "Signal summary table")
