import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from lib.data import table
from lib.ui import NEUTRAL, SERIES, location_labels, money, page, show_table, style

page("Location Performance",
     "Which locations generate the most revenue, and what distinguishes the top performers?")

loc = table("mart_location_monthly")
loc["location"] = loc["restaurant_id"].map(location_labels(loc["restaurant_id"].unique()))
months = sorted(loc["month"].unique())
period = st.selectbox("Period", ["All time", "Last 12 months"] + months[::-1], index=1)
if period == "All time":
    d = loc
elif period == "Last 12 months":
    d = loc[loc["month"].isin(months[-12:])]
else:
    d = loc[loc["month"] == period]

agg = (d.groupby("location").agg(net_revenue=("net_revenue", "sum"), orders=("orders", "sum"),
                                 trading_days=("trading_days", "sum"), customers=("customers", "sum"),
                                 returning=("returning_customers", "sum"))
         .reset_index())
agg["avg_order_value"] = agg["net_revenue"] / agg["orders"]
agg["orders_per_day"] = agg["orders"] / agg["trading_days"]
agg["returning_rate"] = agg["returning"] / agg["customers"]
agg = agg.sort_values("net_revenue", ascending=False).reset_index(drop=True)
agg["rank"] = agg.index + 1
n = len(agg)
agg["group"] = ["Top 5" if r <= 5 else ("Bottom 5" if r > n - 5 else "Middle") for r in agg["rank"]]
gcol = {"Top 5": SERIES[0], "Bottom 5": SERIES[1], "Middle": NEUTRAL}

c = st.columns(4)
c[0].metric("Active locations", f"{n}")
c[1].metric(f"Top location: {agg['location'].iloc[0].split(' ')[0]}", money(agg["net_revenue"].iloc[0]))
top5_share = agg.loc[agg["group"] == "Top 5", "net_revenue"].sum() / agg["net_revenue"].sum()
c[2].metric("Revenue share of top 5", f"{top5_share:.0%}")
c[3].metric("Average order value (all)", money(agg["net_revenue"].sum() / agg["orders"].sum(), 2))

r = agg.iloc[::-1]
fig = go.Figure(go.Bar(y=r["location"], x=r["net_revenue"], orientation="h", marker_color=[gcol[g] for g in r["group"]],
                       customdata=r[["rank", "group"]],
                       hovertemplate="#%{customdata[0]} %{y}: $%{x:,.0f} (%{customdata[1]})<extra></extra>"))
fig.update_layout(title=f"Net revenue ranking — {period.lower()}  (blue = top 5, orange = bottom 5)")
fig.update_xaxes(tickprefix="$", tickformat=",.0f")
st.plotly_chart(style(fig, height=max(360, 22 * n), legend=False), use_container_width=True)

left, right = st.columns(2)
with left:
    fig = go.Figure()
    for g in ["Middle", "Bottom 5", "Top 5"]:
        s = agg[agg["group"] == g]
        fig.add_trace(go.Scatter(x=s["orders_per_day"], y=s["avg_order_value"], mode="markers", name=g,
                                 marker=dict(size=11, color=gcol[g], line=dict(color="white", width=2)),
                                 text=s["location"],
                                 hovertemplate="%{text}<br>%{x:.1f} orders/day · AOV $%{y:.2f}<extra></extra>"))
    fig.update_layout(title="Orders per trading day (across) vs average order value (up)")
    fig.update_yaxes(tickprefix="$")
    st.plotly_chart(style(fig), use_container_width=True)
with right:
    rr = agg.sort_values("returning_rate")
    fig = go.Figure(go.Bar(y=rr["location"], x=rr["returning_rate"], orientation="h",
                           marker_color=[gcol[g] for g in rr["group"]],
                           hovertemplate="%{y}: %{x:.0%} returning customers<extra></extra>"))
    fig.update_layout(title="Customer retention: share of monthly customers who came back")
    fig.update_xaxes(tickformat=".0%")
    st.plotly_chart(style(fig, height=max(360, 22 * n), legend=False), use_container_width=True)

st.subheader("Top 5 locations over time")
top5 = agg.loc[agg["group"] == "Top 5", "location"].tolist()
fig = go.Figure()
for i, name in enumerate(top5):
    s = loc[loc["location"] == name].sort_values("month")
    fig.add_trace(go.Scatter(x=pd.to_datetime(s["month"] + "-01"), y=s["net_revenue"], mode="lines", name=name,
                             line_color=SERIES[i], hovertemplate=f"{name}<br>%{{x|%b %Y}}: $%{{y:,.0f}}<extra></extra>"))
fig.update_layout(title="Monthly net revenue, top 5 locations")
fig.update_yaxes(tickprefix="$", tickformat=",.0f")
st.plotly_chart(style(fig), use_container_width=True)

show_table(agg[["rank", "location", "net_revenue", "orders", "avg_order_value", "orders_per_day",
                "returning_rate", "group"]].round(3), "Location table")
