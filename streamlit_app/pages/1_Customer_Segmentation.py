import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from lib.data import customer_curve, table
from lib.ui import (SEGMENT_COLOR, SEGMENT_ORDER, SEQUENTIAL, SERIES, TIER_COLOR, TIER_ORDER,
                    money, page, pct, show_table, style)

page("Customer Segmentation",
     "What distinct customer segments emerge from total spend, frequency, recency and loyalty status?")

cust = table("mart_customer_current")
loyal = st.radio("Customers", ["All", "Loyalty members", "Non-members"], horizontal=True)
if loyal == "Loyalty members":
    cust = cust[cust["ever_loyalty"] == True]          # noqa: E712
elif loyal == "Non-members":
    cust = cust[cust["ever_loyalty"] != True]          # noqa: E712

seg = (cust.groupby("rfm_segment")
           .agg(customers=("user_id", "count"), clv=("clv_to_date", "sum"), avg_clv=("clv_to_date", "mean"))
           .reindex(SEGMENT_ORDER).fillna(0).reset_index())
seg["clv_share"] = seg["clv"] / seg["clv"].sum()

c = st.columns(4)
c[0].metric("Customers", f"{len(cust):,}")
c[1].metric("VIP customers", f"{int(seg.loc[seg.rfm_segment == 'VIP', 'customers'].sum()):,}")
c[2].metric("Total CLV", money(cust["clv_to_date"].sum()))
c[3].metric("Average CLV", money(cust["clv_to_date"].mean(), 2))

left, right = st.columns(2)
with left:
    fig = go.Figure(go.Bar(y=seg["rfm_segment"], x=seg["customers"], orientation="h",
                           marker_color=[SEGMENT_COLOR[s] for s in seg["rfm_segment"]],
                           text=seg["customers"].map("{:,.0f}".format), textposition="outside",
                           hovertemplate="%{y}: %{x:,} customers<extra></extra>"))
    fig.update_layout(title="Customers by RFM segment")
    fig.update_yaxes(categoryorder="array", categoryarray=SEGMENT_ORDER[::-1])
    st.plotly_chart(style(fig, legend=False), use_container_width=True)
with right:
    fig = go.Figure(go.Bar(y=seg["rfm_segment"], x=seg["clv_share"], orientation="h",
                           marker_color=[SEGMENT_COLOR[s] for s in seg["rfm_segment"]],
                           text=seg["clv_share"].map(lambda v: f"{v:.0%}"), textposition="outside",
                           customdata=seg["clv"],
                           hovertemplate="%{y}: %{x:.1%} of CLV ($%{customdata:,.0f})<extra></extra>"))
    fig.update_layout(title="Share of total customer lifetime value")
    fig.update_xaxes(tickformat=".0%")
    fig.update_yaxes(categoryorder="array", categoryarray=SEGMENT_ORDER[::-1])
    st.plotly_chart(style(fig, legend=False), use_container_width=True)

left, right = st.columns(2)
with left:
    act = cust[cust["rfm_active"] == True]              # noqa: E712
    grid = act.pivot_table(index="f_score", columns="r_score", values="user_id", aggfunc="count").reindex(
        index=[5, 4, 3, 2, 1], columns=[1, 2, 3, 4, 5]).fillna(0)
    fig = go.Figure(go.Heatmap(z=grid.values, x=[f"R{r}" for r in grid.columns], y=[f"F{f}" for f in grid.index],
                               colorscale=SEQUENTIAL, text=grid.values.astype(int), texttemplate="%{text:,}",
                               xgap=2, ygap=2, colorbar=dict(title="Customers"),
                               hovertemplate="%{x} · %{y}: %{z:,.0f} customers<extra></extra>"))
    fig.update_layout(title="Recency × frequency (customers who ordered in the last 6 months)")
    st.plotly_chart(style(fig, legend=False), use_container_width=True)
with right:
    tl = (cust.assign(member=cust["ever_loyalty"].map({True: "Loyalty member"}).fillna("Non-member"))
              .groupby(["clv_tier", "member"]).size().reset_index(name="customers"))
    fig = px.bar(tl, x="clv_tier", y="customers", color="member", barmode="group",
                 category_orders={"clv_tier": TIER_ORDER, "member": ["Loyalty member", "Non-member"]},
                 color_discrete_sequence=SERIES, title="CLV tier by loyalty status")
    fig.update_traces(hovertemplate="%{x} tier: %{y:,} customers<extra></extra>")
    fig.update_xaxes(title=None)
    fig.update_yaxes(title="Customers")
    st.plotly_chart(style(fig), use_container_width=True)

st.subheader("How one customer's lifetime value evolved")
top = cust.sort_values("clv_to_date", ascending=False).head(200)
choice = st.selectbox("Customer (top 200 by CLV)", top["user_id"],
                      format_func=lambda u: f"{u}  ·  CLV {money(top.set_index('user_id').at[u, 'clv_to_date'])}")
curve = customer_curve(choice)
if len(curve):
    fig = go.Figure(go.Scatter(x=curve["snapshot_date"], y=curve["clv_to_date"], mode="lines",
                               line_color=SERIES[0], name="CLV to date",
                               customdata=curve[["clv_tier", "churn_status"]],
                               hovertemplate="%{x|%b %d, %Y}<br>CLV $%{y:,.2f}<br>Tier %{customdata[0]} · "
                                             "%{customdata[1]}<extra></extra>"))
    fig.update_layout(title="Daily CLV snapshot")
    fig.update_yaxes(tickprefix="$")
    st.plotly_chart(style(fig, legend=False), use_container_width=True)

show_table(seg.assign(clv=seg["clv"].round(0), avg_clv=seg["avg_clv"].round(2),
                      clv_share=(seg["clv_share"] * 100).round(1)), "Segment summary table")
st.caption("Segments: VIP = recency, frequency and spend scores all 4–5 · New = first order in the last 30 days · "
           "Churn risk = low recency and frequency · Regular = everyone else. Scores rank customers who ordered "
           "in the last 6 months; others score 1. CLV tier: High = top 20%, Medium = middle 60%, Low = bottom 20%.")
