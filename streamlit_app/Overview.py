"""GlobalPartners Business Insights - overview page."""
import plotly.graph_objects as go
import streamlit as st

from lib.data import table
from lib.ui import SERIES, money, page, pct, style

page("GlobalPartners Business Insights",
     "Customer value, churn, sales, loyalty, locations and discounts across all restaurants. "
     "Data refreshed daily from SQL Server through the AWS pipeline.")

cust = table("mart_customer_current")
sales = table("mart_sales_daily")
as_of = cust["snapshot_date"].max()

net = sales["net_revenue"].sum()
high_share = cust.loc[cust["clv_tier"] == "High", "clv_to_date"].sum() / cust["clv_to_date"].sum()
at_risk_value = cust.loc[cust["is_at_risk"] & (cust["clv_tier"] == "High"), "clv_to_date"].sum()

c = st.columns(4)
c[0].metric("Net revenue (all time)", money(net))
c[1].metric("Identified customers", f"{len(cust):,}")
c[2].metric("CLV held by top 20% of customers", pct(high_share, 0))
c[3].metric("High-value CLV at risk or lapsed", money(at_risk_value))
st.caption(f"Data as of {as_of:%B %d, %Y}. Guest orders are included in revenue but not in customer counts.")

monthly = (sales.assign(month=sales["order_date"].dt.to_period("M").dt.to_timestamp())
                .groupby("month", as_index=False)["net_revenue"].sum())
fig = go.Figure(go.Scatter(x=monthly["month"], y=monthly["net_revenue"], mode="lines",
                           line_color=SERIES[0], name="Net revenue",
                           hovertemplate="%{x|%b %Y}<br>$%{y:,.0f}<extra></extra>"))
fig.update_layout(title="Net revenue by month")
fig.update_yaxes(tickprefix="$", tickformat=",.0f")
st.plotly_chart(style(fig, legend=False), use_container_width=True)

st.subheader("Dashboards")
st.markdown("""
| Page | Question it answers |
| --- | --- |
| **Customer Segmentation** | What customer segments emerge from spend, frequency, recency and loyalty? |
| **Churn Risk** | Which customers are at risk, and which signals go with churn? |
| **Sales Trends** | How do sales move by month, season, holiday, category and location? |
| **Loyalty Impact** | Do loyalty members spend more and come back more? |
| **Location Performance** | Which locations lead, and what sets them apart? |
| **Pricing & Discounts** | How do discounts affect order volume and net revenue? |
""")
st.caption("Use the sidebar to open a page.")
