import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from lib.data import table
from lib.ui import NEUTRAL, SEQUENTIAL, SERIES, location_labels, money, page, show_table, style

page("Sales Trends and Seasonality",
     "What are the monthly and seasonal sales trends, and how do they vary by category and location?")

sales = table("mart_sales_daily")
dates = table("dim_date")[["date_key", "is_holiday", "holiday_name", "day_of_week", "day_of_week_num"]]
labels = location_labels(sales["restaurant_id"].unique())
sales["location"] = sales["restaurant_id"].map(labels)

f = st.columns([2, 2, 2, 1])
lo, hi = sales["order_date"].min().date(), sales["order_date"].max().date()
rng = f[0].date_input("Date range", (lo, hi), min_value=lo, max_value=hi)
locs = f[1].multiselect("Locations", sorted(sales["location"].unique()), placeholder="All locations")
cats = f[2].multiselect("Categories", sorted(sales["item_category"].dropna().unique()), placeholder="All categories")
grain = f[3].selectbox("Grain", ["Month", "Week", "Day"])

df = sales
if isinstance(rng, tuple) and len(rng) == 2:
    df = df[(df["order_date"] >= pd.Timestamp(rng[0])) & (df["order_date"] <= pd.Timestamp(rng[1]))]
if locs:
    df = df[df["location"].isin(locs)]
if cats:
    df = df[df["item_category"].isin(cats)]

days = df["order_date"].nunique()
c = st.columns(4)
c[0].metric("Net revenue", money(df["net_revenue"].sum()))
c[1].metric("Items sold", f"{df['items_sold'].sum():,.0f}")
c[2].metric("Average net revenue per trading day", money(df["net_revenue"].sum() / max(days, 1)))
c[3].metric("Discounts given", money(df["discount_amt"].sum()))

freq = {"Month": "MS", "Week": "W-MON", "Day": "D"}[grain]
ts = df.groupby(pd.Grouper(key="order_date", freq=freq))["net_revenue"].sum().reset_index()
fig = go.Figure(go.Scatter(x=ts["order_date"], y=ts["net_revenue"], mode="lines", line_color=SERIES[0],
                           name="Net revenue", hovertemplate="%{x|%b %d, %Y}: $%{y:,.0f}<extra></extra>"))
if grain == "Day":
    hol = ts.merge(dates[dates["is_holiday"]], left_on="order_date", right_on="date_key")
    fig.add_trace(go.Scatter(x=hol["order_date"], y=hol["net_revenue"], mode="markers", name="Holiday",
                             marker=dict(size=9, color=SERIES[1], line=dict(color="white", width=2)),
                             customdata=hol["holiday_name"],
                             hovertemplate="%{customdata}<br>%{x|%b %d, %Y}: $%{y:,.0f}<extra></extra>"))
fig.update_layout(title=f"Net revenue by {grain.lower()}")
fig.update_yaxes(tickprefix="$", tickformat=",.0f")
st.plotly_chart(style(fig, legend=(grain == "Day")), use_container_width=True)

left, right = st.columns(2)
with left:
    m = df.assign(year=df["order_date"].dt.year, month=df["order_date"].dt.month)
    by = m.groupby(["year", "month"]).agg(net=("net_revenue", "sum"), days=("order_date", "nunique")).reset_index()
    by["per_day"] = by["net"] / by["days"]
    fig = go.Figure()
    for i, y in enumerate(sorted(by["year"].unique())):
        d = by[by["year"] == y]
        fig.add_trace(go.Scatter(x=d["month"], y=d["per_day"], mode="lines+markers", name=str(y),
                                 line_color=SERIES[i % len(SERIES)], marker_size=8,
                                 hovertemplate=f"{y} · month %{{x}}: $%{{y:,.0f}} per day<extra></extra>"))
    fig.update_layout(title="Seasonality: average daily net revenue by month")
    fig.update_xaxes(tickmode="array", tickvals=list(range(1, 13)),
                     ticktext=["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    fig.update_yaxes(tickprefix="$")
    st.plotly_chart(style(fig), use_container_width=True)
with right:
    cat = df.groupby("item_category")["net_revenue"].sum().sort_values(ascending=False)
    top = cat.head(9)
    if len(cat) > 9:
        top = pd.concat([top, pd.Series({"Other": cat.iloc[9:].sum()})])
    top = top[::-1]
    fig = go.Figure(go.Bar(y=top.index, x=top.values, orientation="h",
                           marker_color=[NEUTRAL if k == "Other" else SERIES[0] for k in top.index],
                           hovertemplate="%{y}: $%{x:,.0f}<extra></extra>"))
    fig.update_layout(title="Net revenue by menu category")
    fig.update_xaxes(tickprefix="$", tickformat=",.0f")
    st.plotly_chart(style(fig, legend=False), use_container_width=True)

left, right = st.columns(2)
with left:
    hm = df.merge(dates, left_on="order_date", right_on="date_key")
    order_dp = ["Breakfast", "Lunch", "Afternoon", "Dinner", "Late"]
    order_dw = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    piv = hm.pivot_table(index="daypart", columns="day_of_week", values="net_revenue", aggfunc="sum") \
            .reindex(index=order_dp, columns=order_dw).fillna(0)
    fig = go.Figure(go.Heatmap(z=piv.values, x=[d[:3] for d in piv.columns], y=piv.index, colorscale=SEQUENTIAL,
                               xgap=2, ygap=2, colorbar=dict(title="Net $"),
                               hovertemplate="%{y} · %{x}: $%{z:,.0f}<extra></extra>"))
    fig.update_layout(title="When sales happen: daypart × weekday (US Eastern time)")
    fig.update_yaxes(autorange="reversed")
    st.plotly_chart(style(fig, legend=False), use_container_width=True)
with right:
    daily = df.groupby("order_date")["net_revenue"].sum().reset_index() \
              .merge(dates, left_on="order_date", right_on="date_key")
    daily["Day type"] = daily["is_holiday"].map({True: "Holiday"}).fillna(
        daily["day_of_week_num"].isin([1, 7]).map({True: "Weekend", False: "Weekday"}))
    avg = daily.groupby("Day type")["net_revenue"].mean().reindex(["Weekday", "Weekend", "Holiday"])
    fig = go.Figure(go.Bar(x=avg.index, y=avg.values, marker_color=[SERIES[0], SERIES[0], SERIES[1]],
                           text=[money(v) for v in avg.values], textposition="outside",
                           hovertemplate="%{x}: $%{y:,.0f} average per day<extra></extra>"))
    fig.update_layout(title="Average daily net revenue: weekday, weekend, holiday")
    fig.update_yaxes(tickprefix="$")
    st.plotly_chart(style(fig, legend=False), use_container_width=True)

show_table(ts.rename(columns={"order_date": grain.lower(), "net_revenue": "net revenue"}), "Revenue table")
