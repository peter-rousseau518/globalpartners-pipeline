"""GlobalPartners - Gold star schema (AWS Glue 5.0, PySpark, Apache Iceberg).

Reads gp_silver and rebuilds the Gold dimensional model in database gp_gold:

    dim_date          one row per calendar day covering all orders (+ holidays)
    dim_customer      SCD Type 2 on loyalty status, one row per user per version
    dim_restaurant    one row per restaurant
    dim_item          one row per item name + category
    dim_channel       one row per ordering app
    fact_order_line   one row per order line, with list price and discount logic
    fact_order        one row per order

Every run rebuilds the tables from Silver (idempotent: re-running gives the same
result). At ~200K lines a full rebuild takes a couple of minutes.

Job parameters
    --LAKE_BUCKET          S3 lake bucket name
    --datalake-formats     iceberg
"""
import sys
from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import Window as W
from pyspark.sql import functions as F

MIN_GROUP_LINES = 5          # list price: month group needs >= 5 lines, else all-time
DISCOUNT_THRESHOLD = 0.05    # $ below list price that counts as a discount
DAYPARTS = [(11, "Breakfast"), (14, "Lunch"), (17, "Afternoon"), (21, "Dinner"), (24, "Late")]


# --------------------------------------------------------------------------
# Calendar and holidays
# --------------------------------------------------------------------------
def _nth_weekday(year, month, weekday, n):
    d = date(year, month, 1)
    d += timedelta(days=(weekday - d.weekday()) % 7)
    return d + timedelta(weeks=n - 1)


def _last_weekday(year, month, weekday):
    d = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
    return d - timedelta(days=(d.weekday() - weekday) % 7)


def us_federal_holidays(year):
    """US federal holidays named like the source date_dim; weekend dates add an observed day."""
    fixed = [(1, 1, "New Year's Day"), (7, 4, "Indep. Day"), (11, 11, "Veterans Day"), (12, 25, "Christmas")]
    if year >= 2021:
        fixed.append((6, 19, "Juneteenth"))
    out = {}
    for m, d, name in fixed:
        day = date(year, m, d)
        out[day] = name
        if day.weekday() == 5:
            out[day - timedelta(days=1)] = f"{name} Observed"
        elif day.weekday() == 6:
            out[day + timedelta(days=1)] = f"{name} Observed"
    out[_nth_weekday(year, 1, 0, 3)] = "Martin Luther King Jr."
    out[_nth_weekday(year, 2, 0, 3)] = "Washington's Birthday"
    out[_last_weekday(year, 5, 0)] = "Memorial Day"
    out[_nth_weekday(year, 9, 0, 1)] = "Labor Day"
    out[_nth_weekday(year, 10, 0, 2)] = "Columbus Day"
    out[_nth_weekday(year, 11, 3, 4)] = "Thanksgiving Day"
    return out


def build_dim_date(spark, start: date, end: date, source_dates: DataFrame):
    """Calendar for start..end. Source holiday flags win for the years the source covers."""
    src_years = {r["y"] for r in source_dates.select(F.year("date_key").alias("y")).distinct().collect()}
    hol_rows = []
    for y in range(start.year, end.year + 1):
        if y not in src_years:
            hol_rows += [(d, n) for d, n in us_federal_holidays(y).items()]
    src_hol = [(r["date_key"], r["holiday_name"]) for r in
               source_dates.where("is_holiday").select("date_key", "holiday_name").collect()]
    holidays = spark.createDataFrame(hol_rows + src_hol, "hol_date date, holiday_name string")

    days = (spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS date_key")
            .join(holidays, F.col("date_key") == F.col("hol_date"), "left").drop("hol_date"))
    m = F.month("date_key")
    return days.select(
        F.date_format("date_key", "yyyyMMdd").cast("int").alias("date_sk"),
        "date_key",
        F.date_format("date_key", "EEEE").alias("day_of_week"),
        F.dayofweek("date_key").alias("day_of_week_num"),          # 1 = Sunday
        F.weekofyear("date_key").alias("week"),
        m.alias("month"),
        F.date_format("date_key", "MMMM").alias("month_name"),
        F.quarter("date_key").alias("quarter"),
        F.year("date_key").alias("year"),
        F.date_format("date_key", "yyyy-MM").alias("year_month"),
        F.when(m.isin(12, 1, 2), "Winter").when(m.isin(3, 4, 5), "Spring")
         .when(m.isin(6, 7, 8), "Summer").otherwise("Fall").alias("season"),
        F.dayofweek("date_key").isin(1, 7).alias("is_weekend"),
        F.col("holiday_name").isNotNull().alias("is_holiday"),
        "holiday_name",
    )


# --------------------------------------------------------------------------
# Dimensions
# --------------------------------------------------------------------------
def sk(*cols):
    """Deterministic surrogate key (same input -> same key on every rebuild)."""
    return F.abs(F.xxhash64(*cols))


def build_dim_customer(items: DataFrame):
    """SCD Type 2: a new version each time a customer's loyalty flag changes."""
    orders = (items.where(F.col("user_id").isNotNull())
              .groupBy("user_id", "order_id")
              .agg(F.min("order_ts_utc").alias("ts"), F.min("order_date").alias("order_date"),
                   F.max("is_loyalty").alias("is_loyalty"), F.max("card_hash").alias("card_hash")))
    w = W.partitionBy("user_id").orderBy("ts", "order_id")
    v = (orders.withColumn("prev", F.lag("is_loyalty").over(w))
               .withColumn("new_version",
                           F.when(F.col("prev").isNull() | (F.col("prev") != F.col("is_loyalty")), 1).otherwise(0))
               .withColumn("version", F.sum("new_version").over(w.rowsBetween(W.unboundedPreceding, 0))))
    versions = (v.groupBy("user_id", "version")
                .agg(F.min("ts").alias("valid_from_ts"), F.first("is_loyalty").alias("is_loyalty"),
                     F.max("card_hash").alias("card_hash"), F.count("*").alias("orders_in_version")))
    wv = W.partitionBy("user_id").orderBy("version")
    first_order = orders.groupBy("user_id").agg(F.min("order_date").alias("first_order_date"))
    return (versions
            .withColumn("valid_to_ts", F.lead("valid_from_ts").over(wv))
            .withColumn("is_current", F.col("valid_to_ts").isNull())
            .join(first_order, "user_id")
            .select(sk("user_id", "version").alias("customer_sk"), "user_id", "version",
                    "is_loyalty", "card_hash", "first_order_date",
                    "valid_from_ts",
                    F.coalesce("valid_to_ts", F.lit("9999-12-31 00:00:00").cast("timestamp")).alias("valid_to_ts"),
                    "is_current", "orders_in_version"))


def build_dim_restaurant(items: DataFrame):
    return (items.groupBy("restaurant_id")
            .agg(F.min("order_date").alias("first_order_date"), F.max("order_date").alias("last_order_date"))
            .select(sk("restaurant_id").alias("restaurant_sk"), "restaurant_id",
                    "first_order_date", "last_order_date"))


def build_dim_item(items: DataFrame):
    return (items.select("item_name", "item_category").distinct()
            .select(sk("item_name", "item_category").alias("item_sk"), "item_name", "item_category"))


def build_dim_channel(items: DataFrame):
    return items.select("channel").distinct().select(sk("channel").alias("channel_sk"), "channel")


# --------------------------------------------------------------------------
# Facts
# --------------------------------------------------------------------------
def _mode_price(df: DataFrame, keys):
    """Most common unit price per key group; ties take the higher price."""
    counts = df.groupBy(*keys, "unit_price").agg(F.count("*").alias("n"))
    w = W.partitionBy(*keys).orderBy(F.desc("n"), F.desc("unit_price"))
    totals = df.groupBy(*keys).agg(F.count("*").alias("group_lines"))
    return (counts.withColumn("rk", F.row_number().over(w)).where("rk = 1")
            .select(*keys, F.col("unit_price").alias("mode_price")).join(totals, keys))


def add_list_price(items: DataFrame):
    """list_price = modal UNIT price for restaurant + item + month (>= 5 lines), else restaurant + item."""
    base = items.withColumn("price_month", F.date_format("order_date", "yyyy-MM"))
    normal = base.where(~F.col("is_outlier"))
    month_mode = (_mode_price(normal, ["restaurant_id", "item_name", "price_month"])
                  .withColumnRenamed("mode_price", "month_price").withColumnRenamed("group_lines", "month_lines"))
    all_mode = (_mode_price(normal, ["restaurant_id", "item_name"])
                .withColumnRenamed("mode_price", "all_price").drop("group_lines"))
    return (base.join(month_mode, ["restaurant_id", "item_name", "price_month"], "left")
                .join(all_mode, ["restaurant_id", "item_name"], "left")
                .withColumn("list_price",
                            F.when(F.col("month_lines") >= MIN_GROUP_LINES, F.col("month_price"))
                             .otherwise(F.coalesce("all_price", "unit_price")).cast("decimal(10,2)"))
                .drop("price_month", "month_price", "month_lines", "all_price"))


def daypart(hour_col):
    expr = None
    for upper, name in DAYPARTS:
        expr = F.when(hour_col < upper, name) if expr is None else expr.when(hour_col < upper, name)
    return expr


def build_fact_order_line(items, options, dim_customer, dim_restaurant, dim_item, dim_channel):
    opt = (options.groupBy("order_id", "lineitem_id")
           .agg(F.sum("option_amount").alias("option_addon_amt"), F.count("*").alias("option_count")))
    df = add_list_price(items).join(opt, ["order_id", "lineitem_id"], "left")

    # item_line_amount is what the customer paid for the item (source item_price = line total).
    gap = F.col("list_price") - F.col("unit_price")
    discounted = (gap >= DISCOUNT_THRESHOLD) & ~F.col("is_outlier")
    qty = F.col("item_quantity")
    addon = F.coalesce("option_addon_amt", F.lit(0)).cast("decimal(12,2)")
    df = (df.withColumn("option_addon_amt", addon)
            .withColumn("option_count", F.coalesce("option_count", F.lit(0)))
            .withColumn("is_discounted", discounted)
            .withColumn("item_gross", F.when(discounted, F.col("list_price") * qty)
                                       .otherwise(F.col("item_line_amount")).cast("decimal(12,2)"))
            .withColumn("item_discount_amt", (F.col("item_gross") - F.col("item_line_amount")).cast("decimal(12,2)"))
            .withColumn("line_gross", (F.col("item_gross") + addon).cast("decimal(12,2)"))
            .withColumn("line_net", (F.col("item_line_amount") + addon).cast("decimal(12,2)")))

    cust = dim_customer.select("customer_sk", F.col("user_id").alias("c_user"), "valid_from_ts", "valid_to_ts")
    df = df.join(cust, (df.user_id == cust.c_user) & (df.order_ts_utc >= cust.valid_from_ts)
                 & (df.order_ts_utc < cust.valid_to_ts), "left")

    return (df.join(dim_restaurant.select("restaurant_id", "restaurant_sk"), "restaurant_id", "left")
              .join(dim_item.select("item_name", "item_category", "item_sk"), ["item_name", "item_category"], "left")
              .join(dim_channel, "channel", "left")
              .select("order_id", "lineitem_id",
                      F.date_format("order_date", "yyyyMMdd").cast("int").alias("date_sk"),
                      "order_date", "order_ts_utc", "order_hour_local",
                      daypart(F.col("order_hour_local")).alias("daypart"),
                      "customer_sk", "user_id", "is_guest", "is_loyalty",
                      "restaurant_sk", "restaurant_id", "item_sk", "item_category", "item_name",
                      "channel_sk", "channel",
                      "item_quantity", "unit_price", "list_price", "item_line_amount", "item_gross", "item_discount_amt",
                      "option_count", "option_addon_amt", "line_gross", "line_net",
                      "is_discounted", "is_outlier"))


def build_fact_order(lines: DataFrame):
    return (lines.groupBy("order_id")
            .agg(F.first("date_sk").alias("date_sk"), F.first("order_date").alias("order_date"),
                 F.first("order_ts_utc").alias("order_ts_utc"), F.first("order_hour_local").alias("order_hour_local"),
                 F.first("daypart").alias("daypart"),
                 F.first("customer_sk", ignorenulls=True).alias("customer_sk"),
                 F.first("user_id", ignorenulls=True).alias("user_id"), F.first("is_guest").alias("is_guest"),
                 F.max("is_loyalty").alias("is_loyalty"),
                 F.first("restaurant_sk").alias("restaurant_sk"), F.first("restaurant_id").alias("restaurant_id"),
                 F.first("channel_sk").alias("channel_sk"), F.first("channel").alias("channel"),
                 F.count("*").alias("line_count"), F.sum("item_quantity").alias("item_quantity"),
                 F.sum("line_gross").alias("gross_revenue"), F.sum("item_discount_amt").alias("discount_amt"),
                 F.sum("line_net").alias("net_revenue"),
                 F.max("is_discounted").alias("has_discount"), F.max("is_outlier").alias("has_outlier")))


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
    # Default bucket follows the project naming convention: gp-lake-<account id>
    lake = args.get("LAKE_BUCKET") or "gp-lake-" + boto3.client("sts").get_caller_identity()["Account"]
    print(f"GOLD  using lake bucket {lake}; argv={sys.argv[1:]}")
    conf = (SparkConf()
            .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{lake}/gold/")
            .set("spark.sql.session.timeZone", "UTC"))
    sc = SparkContext(conf=conf)
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.gp_gold LOCATION 's3://{lake}/gold/'")
    items = spark.table("glue_catalog.gp_silver.order_items").cache()
    options = spark.table("glue_catalog.gp_silver.order_item_options")
    src_dates = spark.table("glue_catalog.gp_silver.date_dim_source")

    span = items.agg(F.min("order_date").alias("lo"), F.max("order_date").alias("hi")).collect()[0]
    dim_date = build_dim_date(spark, date(span["lo"].year, 1, 1), date(span["hi"].year, 12, 31), src_dates)
    dim_customer = build_dim_customer(items).cache()
    dim_restaurant = build_dim_restaurant(items)
    dim_item = build_dim_item(items)
    dim_channel = build_dim_channel(items)
    fact_line = build_fact_order_line(items, options, dim_customer, dim_restaurant, dim_item, dim_channel).cache()
    fact_order = build_fact_order(fact_line)

    tables = {"dim_date": dim_date, "dim_customer": dim_customer, "dim_restaurant": dim_restaurant,
              "dim_item": dim_item, "dim_channel": dim_channel,
              "fact_order_line": fact_line, "fact_order": fact_order}
    for name, df in tables.items():
        (df.writeTo(f"glue_catalog.gp_gold.{name}").using("iceberg")
           .tableProperty("format-version", "2").createOrReplace())
        print(f"GOLD  {name:16} rows={spark.table(f'glue_catalog.gp_gold.{name}').count():>9,}")

    lines = spark.table("glue_catalog.gp_gold.fact_order_line")
    if lines.count() != items.count():
        raise RuntimeError("fact_order_line row count differs from silver.order_items")
    t = spark.table("glue_catalog.gp_gold.fact_order").agg(
        F.sum("gross_revenue").alias("g"), F.sum("discount_amt").alias("d"), F.sum("net_revenue").alias("n")).collect()[0]
    print(f"GOLD  totals           gross={t['g']:,.2f} discount={t['d']:,.2f} net={t['n']:,.2f}")
    job.commit()


if __name__ == "__main__":
    main()
