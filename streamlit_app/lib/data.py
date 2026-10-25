"""Data access for the GlobalPartners dashboard.

DATA_MODE=athena (default on EC2): queries the Gold Iceberg tables through Amazon Athena.
DATA_MODE=local: reads Parquet copies of the same tables with DuckDB (for testing).

Environment variables
    DATA_MODE            athena | local
    AWS_REGION           default us-east-1
    GOLD_DB              default gp_gold
    ATHENA_S3_STAGING    s3:// path for Athena query results (required for athena mode)
    ATHENA_WORKGROUP     default primary
    LOCAL_DATA_DIR       folder with <table>.parquet files (local mode)
"""
import os
import re

import pandas as pd
import streamlit as st

MODE = os.environ.get("DATA_MODE", "athena").lower()
DB = os.environ.get("GOLD_DB", "gp_gold")
REGION = os.environ.get("AWS_REGION", "us-east-1")

SMALL_TABLES = [
    "mart_customer_current", "mart_sales_daily", "mart_loyalty_monthly",
    "mart_location_monthly", "mart_discount_monthly", "mart_discount_effect",
    "dim_date", "dim_restaurant",
]


@st.cache_resource
def _connection():
    if MODE == "local":
        import duckdb
        con = duckdb.connect()
        folder = os.environ.get("LOCAL_DATA_DIR", "local_data")
        for f in os.listdir(folder):
            if f.endswith(".parquet") or os.path.isdir(os.path.join(folder, f)):
                name = f.replace(".parquet", "")
                path = os.path.join(folder, f)
                src = f"'{path}/*.parquet'" if os.path.isdir(path) else f"'{path}'"
                con.execute(f'CREATE VIEW "{name}" AS SELECT * FROM read_parquet({src})')
        return con
    from pyathena import connect
    from pyathena.pandas.cursor import PandasCursor
    return connect(s3_staging_dir=os.environ["ATHENA_S3_STAGING"], region_name=REGION,
                   work_group=os.environ.get("ATHENA_WORKGROUP", "primary"),
                   schema_name=DB, cursor_class=PandasCursor)


def _table(name: str) -> str:
    return f'"{name}"' if MODE == "local" else f"{DB}.{name}"


@st.cache_data(ttl=3600, show_spinner="Loading data...")
def query(sql: str) -> pd.DataFrame:
    """Run SQL. Write table names as {t:table_name}; they are qualified for the active mode."""
    sql = re.sub(r"\{t:(\w+)\}", lambda m: _table(m.group(1)), sql)
    con = _connection()
    if MODE == "local":
        return con.execute(sql).df()
    return con.cursor().execute(sql).as_pandas()


def table(name: str) -> pd.DataFrame:
    if name not in SMALL_TABLES:
        raise ValueError(f"{name} is not a small table; query it with filters instead")
    df = query(f"SELECT * FROM {{t:{name}}}")
    for c in df.columns:
        if (c.endswith("_date") and not c.endswith("_to_date")) or c == "date_key":
            df[c] = pd.to_datetime(df[c])
    for c in df.select_dtypes(include="object").columns:
        sample = df[c].dropna().head(1)
        if len(sample) and hasattr(sample.iloc[0], "as_tuple"):          # Decimal -> float
            df[c] = df[c].astype(float)
    return df


def customer_curve(user_id: str) -> pd.DataFrame:
    if not re.fullmatch(r"[0-9a-fA-F]{24}", user_id or ""):
        return pd.DataFrame()
    df = query(f"""SELECT snapshot_date, clv_to_date, clv_tier, churn_status, rfm_segment
                   FROM {{t:fact_customer_clv_daily}} WHERE user_id = '{user_id}'
                   ORDER BY snapshot_date""")
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    df["clv_to_date"] = df["clv_to_date"].astype(float)
    return df


def churn_trend() -> pd.DataFrame:
    """Customer counts by churn status on the first day of each month."""
    df = query("""SELECT snapshot_date, churn_status, COUNT(*) AS customers,
                         SUM(clv_to_date) AS clv
                  FROM {t:fact_customer_clv_daily}
                  WHERE EXTRACT(DAY FROM snapshot_date) = 1
                  GROUP BY snapshot_date, churn_status ORDER BY snapshot_date""")
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    df["clv"] = df["clv"].astype(float)
    return df


def top_discounted_items(limit: int = 15) -> pd.DataFrame:
    df = query(f"""SELECT item_name, item_category, COUNT(*) AS discounted_lines,
                          SUM(item_discount_amt) AS discount_amt
                   FROM {{t:fact_order_line}}
                   WHERE is_discounted AND NOT is_outlier
                   GROUP BY item_name, item_category
                   ORDER BY discount_amt DESC LIMIT {int(limit)}""")
    df["discount_amt"] = df["discount_amt"].astype(float)
    return df
