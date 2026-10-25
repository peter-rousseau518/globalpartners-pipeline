"""Shared look and helpers for every dashboard page."""
import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# Validated categorical palette, used in fixed order (never cycled)
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
NEUTRAL = "#c3c2b7"
SEQUENTIAL = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
# Status colours are reserved for churn state and always shown with their label
STATUS = {"Active": "#0ca30c", "Cooling": "#fab219", "At risk": "#ec835a", "Lapsed": "#d03b3b"}
STATUS_ORDER = ["Active", "Cooling", "At risk", "Lapsed"]
SEGMENT_ORDER = ["VIP", "Regular", "New", "Churn risk"]
SEGMENT_COLOR = dict(zip(SEGMENT_ORDER, SERIES))
TIER_ORDER = ["High", "Medium", "Low"]
TIER_COLOR = {"High": SEQUENTIAL[5], "Medium": SEQUENTIAL[3], "Low": SEQUENTIAL[1]}
INK, INK_2, MUTED, GRID, AXIS, SURFACE = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7", "#fcfcfb"


def page(title: str, question: str):
    st.set_page_config(page_title=f"{title} | GlobalPartners", layout="wide")
    _password_gate()
    st.title(title)
    st.caption(question)


def _password_gate():
    pw = os.environ.get("APP_PASSWORD")
    if not pw or st.session_state.get("authed"):
        return
    entered = st.text_input("Password", type="password")
    if entered == pw:
        st.session_state["authed"] = True
        st.rerun()
    st.stop()


def style(fig: go.Figure, height: int = 360, legend: bool = True) -> go.Figure:
    fig.update_layout(
        height=height, margin=dict(l=8, r=56, t=44, b=8),
        paper_bgcolor=SURFACE, plot_bgcolor=SURFACE,
        font=dict(family="system-ui, -apple-system, Segoe UI, sans-serif", color=INK_2, size=13),
        title_font=dict(color=INK, size=15),
        hoverlabel=dict(bgcolor="white", font_color=INK),
        legend=dict(orientation="h", yanchor="top", y=-0.12, x=0, title_text="") if legend else None,
        showlegend=legend, bargap=0.25,
    )
    fig.update_xaxes(gridcolor=GRID, linecolor=AXIS, tickfont_color=MUTED, zeroline=False)
    fig.update_yaxes(gridcolor=GRID, linecolor=AXIS, tickfont_color=MUTED, zeroline=False)
    fig.update_traces(selector=dict(type="bar"), marker_line_width=0, cliponaxis=False)
    fig.update_traces(selector=dict(type="scatter", mode="lines"), line_width=2)
    return fig


def money(x, decimals=0) -> str:
    return "—" if pd.isna(x) else f"${x:,.{decimals}f}"


def pct(x, decimals=1) -> str:
    return "—" if pd.isna(x) else f"{x * 100:.{decimals}f}%"


def location_labels(restaurant_ids) -> dict:
    """Short readable names: L01, L02 ... by order of first appearance in the data."""
    from lib.data import table
    dim = table("dim_restaurant").sort_values(["first_order_date", "restaurant_id"])
    names = {rid: f"L{i:02d} · …{rid[-4:]}" for i, rid in enumerate(dim["restaurant_id"], start=1)}
    return {r: names.get(r, r) for r in restaurant_ids}


def show_table(df: pd.DataFrame, label: str = "Show data table"):
    with st.expander(label):
        st.dataframe(df, use_container_width=True, hide_index=True)
