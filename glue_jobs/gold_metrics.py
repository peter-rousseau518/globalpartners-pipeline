"""GlobalPartners - Gold metrics (AWS Glue 5.0, PySpark, Apache Iceberg).

Reads the Gold star schema (gp_gold) and writes the metric tables the
dashboards use:

    fact_customer_clv_daily   one row per customer per day since first order:
                              CLV to date, CLV tier, RFM scores + segment,
                              churn indicators              (PRIMARY GOAL)
    mart_customer_current     the latest snapshot row per customer
    mart_sales_daily          date x restaurant x category x channel x daypart
    mart_loyalty_monthly      month x loyalty member flag
    mart_location_monthly     month x restaurant, with revenue rank
    mart_discount_monthly     month x restaurant x discounted / full price
    mart_discount_effect      orders 30 days before vs after a customer's first discount

Bulk-quantity outlier lines (is_outlier) are excluded from every metric.
Guest orders (no user_id) count in sales, location and discount marts but not
in customer-level metrics.

Job parameters
    --datalake-formats  iceberg
    --LAKE_BUCKET       optional; defaults to gp-lake-<account id>
"""
import sys

from pyspark.sql import DataFrame
from pyspark.sql import Window as W
from pyspark.sql import functions as F

RFM_WINDOW_DAYS = 180          # N months for frequency / monetary (~6 months)
SPEND_WINDOW_DAYS = 30         # spend change: last 30 days vs prior 30 days
CHURN_BANDS = [(30, "Active"), (45, "Cooling"), (90, "At risk")]   # else Lapsed
GAP_MULTIPLIER = 2.0           # also At risk if days since last order > 2 x average gap
GAP_MIN_DAYS = 14              # ...but only once at least this many days have passed


# --------------------------------------------------------------------------
# Customer daily snapshot
# --------------------------------------------------------------------------
def customer_daily_orders(lines: DataFrame) -> DataFrame:
    """Net revenue and order count per customer per day (outliers and guests excluded)."""
    return (lines.where(~F.col("is_outlier") & F.col("user_id").isNotNull())
            .groupBy("user_id", "order_date")
            .agg(F.sum("line_net").alias("revenue_on_day"),
                 F.countDistinct("order_id").alias("orders_on_day")))


def score_1_to_5(col_name, descending=False):
    """Quintile score 1-5 (5 = best), ranked only among customers active in the RFM window.

    Customers with no order in the last RFM_WINDOW_DAYS get 1. Scoring them against
    the whole base would let any recent buyer look like a VIP, because most of the
    base is long lapsed.
    """
    order = F.col(col_name).desc() if descending else F.col(col_name).asc()
    pr = F.percent_rank().over(W.partitionBy("snapshot_date", "rfm_active").orderBy(order))
    score = F.least(F.lit(5), (F.floor(pr * 5) + 1)).cast("int")
    return F.when(F.col("rfm_active"), score).otherwise(F.lit(1))


def build_clv_daily(daily: DataFrame, dim_date: DataFrame, as_of) -> DataFrame:
    first = daily.groupBy("user_id").agg(F.min("order_date").alias("first_order_date"))
    days = dim_date.select(F.col("date_key").alias("snapshot_date")).where(F.col("snapshot_date") <= F.lit(as_of))
    grid = first.join(days, days.snapshot_date >= first.first_order_date)

    df = (grid.join(daily.withColumnRenamed("order_date", "snapshot_date"), ["user_id", "snapshot_date"], "left")
              .fillna(0, ["revenue_on_day", "orders_on_day"]))

    by_user = W.partitionBy("user_id").orderBy("snapshot_date")
    cum = by_user.rowsBetween(W.unboundedPreceding, 0)
    def last_n(n):                                    # rows are contiguous days
        return by_user.rowsBetween(-(n - 1), 0)
    prior_30 = by_user.rowsBetween(-(2 * SPEND_WINDOW_DAYS - 1), -SPEND_WINDOW_DAYS)

    df = (df
          .withColumn("clv_to_date", F.sum("revenue_on_day").over(cum).cast("decimal(14,2)"))
          .withColumn("orders_to_date", F.sum("orders_on_day").over(cum))
          .withColumn("last_order_date",
                      F.last(F.when(F.col("orders_on_day") > 0, F.col("snapshot_date")), ignorenulls=True).over(cum))
          .withColumn("days_since_last_order", F.datediff("snapshot_date", "last_order_date"))
          .withColumn("customer_age_days", F.datediff("snapshot_date", "first_order_date"))
          .withColumn("aov_to_date", (F.col("clv_to_date") / F.col("orders_to_date")).cast("decimal(12,2)"))
          .withColumn("avg_gap_days",
                      F.when(F.col("orders_to_date") > 1,
                             F.round(F.datediff("last_order_date", "first_order_date") / (F.col("orders_to_date") - 1), 1)))
          .withColumn("spend_last_30", F.sum("revenue_on_day").over(last_n(SPEND_WINDOW_DAYS)).cast("decimal(12,2)"))
          .withColumn("spend_prior_30", F.coalesce(F.sum("revenue_on_day").over(prior_30), F.lit(0)).cast("decimal(12,2)"))
          .withColumn("spend_change_pct",
                      F.when(F.col("spend_prior_30") > 0,
                             F.round((F.col("spend_last_30") - F.col("spend_prior_30")) / F.col("spend_prior_30") * 100, 1)))
          .withColumn("frequency_n", F.sum("orders_on_day").over(last_n(RFM_WINDOW_DAYS)))
          .withColumn("monetary_n", F.sum("revenue_on_day").over(last_n(RFM_WINDOW_DAYS)).cast("decimal(12,2)")))

    # CLV tier: percentile of CLV across all customers on the same day
    df = (df.withColumn("clv_percentile",
                        F.round(F.percent_rank().over(W.partitionBy("snapshot_date").orderBy("clv_to_date")), 4))
            .withColumn("clv_tier", F.when(F.col("clv_percentile") >= 0.8, "High")
                                     .when(F.col("clv_percentile") < 0.2, "Low").otherwise("Medium")))

    # RFM scores 1-5 (5 = best) and segment
    df = (df.withColumn("rfm_active", F.col("frequency_n") > 0)
            .withColumn("r_score", score_1_to_5("days_since_last_order", descending=True))
            .withColumn("f_score", score_1_to_5("frequency_n"))
            .withColumn("m_score", score_1_to_5("monetary_n")))
    r, f, m = F.col("r_score"), F.col("f_score"), F.col("m_score")
    df = df.withColumn("rfm_segment",
                       F.when((r >= 4) & (f >= 4) & (m >= 4), "VIP")
                        .when((F.col("customer_age_days") <= 30) & (f <= 2) & (r >= 4), "New")
                        .when((r <= 2) & (f <= 2), "Churn risk")
                        .otherwise("Regular"))

    # Churn status from inactivity thresholds
    d = F.col("days_since_last_order")
    status = F.when(d <= CHURN_BANDS[0][0], CHURN_BANDS[0][1])
    for limit, name in CHURN_BANDS[1:]:
        status = status.when(d <= limit, name)
    status = status.otherwise("Lapsed")
    gap_flag = ((F.col("avg_gap_days").isNotNull()) & (d >= GAP_MIN_DAYS)
                & (d > F.col("avg_gap_days") * GAP_MULTIPLIER))
    df = (df.withColumn("churn_status_base", status)
            .withColumn("churn_status",
                        F.when(F.col("churn_status_base").isin("Active", "Cooling") & gap_flag, "At risk")
                         .otherwise(F.col("churn_status_base")))
            .withColumn("is_at_risk", F.col("churn_status").isin("At risk", "Lapsed"))
            .drop("churn_status_base"))

    return df.withColumn("snapshot_month", F.date_format("snapshot_date", "yyyy-MM")).select(
        "snapshot_date", "snapshot_month", "user_id", "first_order_date", "last_order_date",
        "revenue_on_day", "orders_on_day", "orders_to_date", "clv_to_date", "aov_to_date",
        "clv_percentile", "clv_tier", "days_since_last_order", "customer_age_days", "avg_gap_days",
        "spend_last_30", "spend_prior_30", "spend_change_pct", "frequency_n", "monetary_n",
        "rfm_active", "r_score", "f_score", "m_score", "rfm_segment", "churn_status", "is_at_risk")


def build_customer_current(clv: DataFrame, dim_customer: DataFrame, as_of) -> DataFrame:
    cur = dim_customer.where("is_current").select("user_id", F.col("is_loyalty").alias("is_loyalty_current"),
                                                  F.col("version").alias("loyalty_versions"))
    ever = dim_customer.groupBy("user_id").agg(F.max("is_loyalty").alias("ever_loyalty"))
    return (clv.where(F.col("snapshot_date") == F.lit(as_of))
               .join(cur, "user_id", "left").join(ever, "user_id", "left"))


# --------------------------------------------------------------------------
# Marts
# --------------------------------------------------------------------------
def build_sales_daily(lines: DataFrame) -> DataFrame:
    return (lines.where(~F.col("is_outlier"))
            .groupBy("order_date", "date_sk", "restaurant_id", "item_category", "channel", "daypart")
            .agg(F.countDistinct("order_id").alias("orders"),
                 F.sum("item_quantity").alias("items_sold"),
                 F.sum("line_gross").alias("gross_revenue"),
                 F.sum("item_discount_amt").alias("discount_amt"),
                 F.sum("line_net").alias("net_revenue")))


def order_level(lines: DataFrame) -> DataFrame:
    """Orders rebuilt from non-outlier lines so every mart uses the same revenue."""
    return (lines.where(~F.col("is_outlier"))
            .groupBy("order_id")
            .agg(F.first("order_date").alias("order_date"), F.first("user_id", ignorenulls=True).alias("user_id"),
                 F.max("is_loyalty").alias("is_loyalty"), F.first("restaurant_id").alias("restaurant_id"),
                 F.first("channel").alias("channel"),
                 F.sum("line_gross").alias("gross"), F.sum("item_discount_amt").alias("discount"),
                 F.sum("line_net").alias("net"), F.max("is_discounted").alias("has_discount"))
            .withColumn("month", F.date_format("order_date", "yyyy-MM")))


def build_loyalty_monthly(orders: DataFrame, clv: DataFrame) -> DataFrame:
    cust = orders.where(F.col("user_id").isNotNull())
    per_cust = (cust.groupBy("month", "is_loyalty", "user_id")
                .agg(F.count("*").alias("n_orders"), F.sum("net").alias("spend"),
                     F.max("order_date").alias("last_date")))
    month_end_clv = (clv.groupBy("user_id", "snapshot_month")
                     .agg(F.max_by("clv_to_date", "snapshot_date").alias("clv_month_end"))
                     .withColumnRenamed("snapshot_month", "month"))
    per_cust = per_cust.join(month_end_clv, ["user_id", "month"], "left")
    return (per_cust.groupBy("month", "is_loyalty")
            .agg(F.countDistinct("user_id").alias("customers"),
                 F.sum("n_orders").alias("orders"),
                 F.sum("spend").alias("net_revenue"),
                 F.round(F.sum("spend") / F.sum("n_orders"), 2).alias("avg_order_value"),
                 F.round(F.avg("spend"), 2).alias("avg_spend_per_customer"),
                 F.round(F.avg(F.when(F.col("n_orders") >= 2, 1).otherwise(0)), 4).alias("repeat_rate"),
                 F.round(F.avg("clv_month_end"), 2).alias("avg_clv"))
            .withColumn("member_label", F.when(F.col("is_loyalty"), "Loyalty member").otherwise("Non-member")))


def build_location_monthly(orders: DataFrame) -> DataFrame:
    first_visit = (orders.where(F.col("user_id").isNotNull())
                   .groupBy("restaurant_id", "user_id").agg(F.min("month").alias("first_month")))
    cust_month = (orders.where(F.col("user_id").isNotNull())
                  .select("restaurant_id", "user_id", "month").distinct()
                  .join(first_visit, ["restaurant_id", "user_id"])
                  .groupBy("restaurant_id", "month")
                  .agg(F.count("*").alias("customers"),
                       F.sum(F.when(F.col("first_month") < F.col("month"), 1).otherwise(0)).alias("returning_customers")))
    base = (orders.groupBy("restaurant_id", "month")
            .agg(F.count("*").alias("orders"), F.sum("net").alias("net_revenue"),
                 F.countDistinct("order_date").alias("trading_days")))
    df = (base.join(cust_month, ["restaurant_id", "month"], "left")
          .withColumn("avg_order_value", F.round(F.col("net_revenue") / F.col("orders"), 2))
          .withColumn("orders_per_day", F.round(F.col("orders") / F.col("trading_days"), 1))
          .withColumn("returning_customer_rate", F.round(F.col("returning_customers") / F.col("customers"), 4)))
    w = W.partitionBy("month").orderBy(F.desc("net_revenue"))
    w_all = W.partitionBy("month")
    return (df.withColumn("revenue_rank", F.dense_rank().over(w))
              .withColumn("locations_in_month", F.count("*").over(w_all))
              .withColumn("rank_group", F.when(F.col("revenue_rank") <= 5, "Top 5")
                          .when(F.col("revenue_rank") > F.col("locations_in_month") - 5, "Bottom 5")
                          .otherwise("Middle")))


def build_discount_monthly(orders: DataFrame) -> DataFrame:
    return (orders.groupBy("month", "restaurant_id", "has_discount")
            .agg(F.count("*").alias("orders"), F.sum("gross").alias("gross_revenue"),
                 F.sum("discount").alias("discount_amt"), F.sum("net").alias("net_revenue"))
            .withColumn("avg_discount_per_order", F.round(F.col("discount_amt") / F.col("orders"), 2))
            .withColumn("discount_pct_of_gross",
                        F.round(F.when(F.col("gross_revenue") > 0, F.col("discount_amt") / F.col("gross_revenue") * 100), 2))
            .withColumn("order_type", F.when(F.col("has_discount"), "Discounted").otherwise("Full price")))


def build_discount_effect(orders: DataFrame) -> DataFrame:
    """Per month of a customer's first discounted order: avg orders 30 days before vs after."""
    cust = orders.where(F.col("user_id").isNotNull())
    first_disc = (cust.where("has_discount").groupBy("user_id")
                  .agg(F.min("order_date").alias("first_discount_date")))
    j = cust.join(first_disc, "user_id")
    before = F.when((F.col("order_date") < F.col("first_discount_date"))
                    & (F.col("order_date") >= F.date_sub("first_discount_date", 30)), 1).otherwise(0)
    after = F.when((F.col("order_date") > F.col("first_discount_date"))
                   & (F.col("order_date") <= F.date_add("first_discount_date", 30)), 1).otherwise(0)
    per = (j.groupBy("user_id", "first_discount_date")
           .agg(F.sum(before).alias("orders_30d_before"), F.sum(after).alias("orders_30d_after")))
    return (per.withColumn("month", F.date_format("first_discount_date", "yyyy-MM"))
               .groupBy("month")
               .agg(F.count("*").alias("customers"),
                    F.round(F.avg("orders_30d_before"), 3).alias("avg_orders_30d_before"),
                    F.round(F.avg("orders_30d_after"), 3).alias("avg_orders_30d_after"))
               .withColumn("change_pct",
                           F.round(F.when(F.col("avg_orders_30d_before") > 0,
                                          (F.col("avg_orders_30d_after") - F.col("avg_orders_30d_before"))
                                          / F.col("avg_orders_30d_before") * 100), 1)))


# --------------------------------------------------------------------------
# Glue I/O
# --------------------------------------------------------------------------
def main():
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark import SparkConf
    from pyspark.context import SparkContext

    wanted = ["JOB_NAME"] + (["LAKE_BUCKET"] if "--LAKE_BUCKET" in sys.argv else [])
    args = getResolvedOptions(sys.argv, wanted)
    lake = args.get("LAKE_BUCKET") or "gp-lake-" + boto3.client("sts").get_caller_identity()["Account"]
    conf = (SparkConf()
            .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{lake}/gold/")
            .set("spark.sql.session.timeZone", "UTC")
            .set("spark.sql.shuffle.partitions", "64"))
    sc = SparkContext(conf=conf)
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    print(f"METRICS  lake bucket {lake}")

    g = "glue_catalog.gp_gold"
    lines = spark.table(f"{g}.fact_order_line").cache()
    dim_date = spark.table(f"{g}.dim_date")
    dim_customer = spark.table(f"{g}.dim_customer")
    as_of = lines.agg(F.max("order_date")).collect()[0][0]
    print(f"METRICS  as-of date {as_of}")

    clv = build_clv_daily(customer_daily_orders(lines), dim_date, as_of)
    (clv.writeTo(f"{g}.fact_customer_clv_daily").using("iceberg")
        .tableProperty("format-version", "2").partitionedBy(F.col("snapshot_month")).createOrReplace())
    clv = spark.table(f"{g}.fact_customer_clv_daily")

    orders = order_level(lines).cache()
    marts = {
        "mart_customer_current": build_customer_current(clv, dim_customer, as_of),
        "mart_sales_daily": build_sales_daily(lines),
        "mart_loyalty_monthly": build_loyalty_monthly(orders, clv),
        "mart_location_monthly": build_location_monthly(orders),
        "mart_discount_monthly": build_discount_monthly(orders),
        "mart_discount_effect": build_discount_effect(orders),
    }
    for name, df in marts.items():
        df.writeTo(f"{g}.{name}").using("iceberg").tableProperty("format-version", "2").createOrReplace()

    for name in ["fact_customer_clv_daily"] + list(marts):
        print(f"METRICS  {name:26} rows={spark.table(f'{g}.{name}').count():>11,}")
    cur = spark.table(f"{g}.mart_customer_current")
    for col in ["clv_tier", "rfm_segment", "churn_status"]:
        print(f"METRICS  {col}: " + ", ".join(f"{r[col]}={r['count']}" for r in cur.groupBy(col).count().collect()))
    job.commit()


if __name__ == "__main__":
    main()
