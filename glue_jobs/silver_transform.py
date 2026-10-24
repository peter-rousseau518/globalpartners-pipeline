"""GlobalPartners - Silver layer (AWS Glue 5.0, PySpark, Apache Iceberg).

Reads the latest Bronze partition of each table, cleans and conforms it, and
writes Iceberg tables to the Glue Data Catalog database `gp_silver`:

    gp_silver.order_items          clean order lines (one row per order line)
    gp_silver.order_item_options   options whose parent line is clean
    gp_silver.date_dim_source      the source 2023 calendar, typed
    gp_silver.quarantine           rejected rows with the reason, by ingest_date
    gp_silver.dq_audit             one row per data-quality rule per run

Cleaning rules come from docs/data_profile.md.

Job parameters
    --LAKE_BUCKET      S3 lake bucket name (e.g. gp-lake-123456789012)
    --CARD_HASH_SALT   secret salt for hashing printed_card_number
    --INGEST_DATE      Bronze partition to process (default: latest available)
    --datalake-formats iceberg      (Glue setting that loads the Iceberg libraries)
"""
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, functions as F, Window as W

LOCAL_TZ = "America/New_York"          # assumption until store time zones are known
QUARANTINE_LIMIT = 0.01                # block the run if > 1% of order lines are rejected
OUTLIER_PRICE = 500                    # item_price >= this is flagged
OUTLIER_QTY = 100                      # item_quantity >= this is flagged
TEST_APPS = ["Alltown Fresh - DEVELOPMENT"]
TEST_CATEGORIES = ["Test Items"]
CATEGORY_FIXES = {                     # after URL junk is stripped
    "Bowls0": "Bowls",
    "Sandwiches`1": "Sandwiches",
    "Sqalads": "Salads",
    "Kid's": "Kids",
}


# --------------------------------------------------------------------------
# Pure transformations (no AWS calls) - unit-testable with local Spark
# --------------------------------------------------------------------------
def clean_category(col):
    """Strip pasted admin URLs (e.g. 'Drip Chttps://...#offee' -> 'Drip Coffee'), then fix typos."""
    stripped = F.trim(F.regexp_replace(col, r"https?://[^#]*#?", ""))
    expr = stripped
    for bad, good in CATEGORY_FIXES.items():
        expr = F.when(stripped == F.lit(bad), F.lit(good)).otherwise(expr)
    return expr


def build_order_items(bronze: DataFrame, salt: str):
    """Return (clean_items, rejected_items). Rejected rows carry a `reject_reason`."""
    w = W.partitionBy("order_id", "lineitem_id").orderBy(F.col("_ingest_ts_utc").desc(), F.col("row_id").desc())
    df = (bronze
          .withColumn("_rn", F.row_number().over(w))
          .where("_rn = 1").drop("_rn"))

    reason = (F.when(F.col("app_name").isin(TEST_APPS), "test_channel")
               .when(F.col("item_category").isin(TEST_CATEGORIES), "test_item")
               .when(F.col("lineitem_id").isNull(), "missing_lineitem_id")
               .when(F.col("item_name").isNull() | F.col("item_category").isNull(), "missing_item")
               .when(F.col("creation_time_utc").isNull(), "missing_timestamp")
               .when(F.col("item_quantity") <= 0, "non_positive_quantity")
               .when(F.col("item_price") < 0, "negative_price"))
    df = df.withColumn("reject_reason", reason)

    rejected = df.where(F.col("reject_reason").isNotNull())
    clean = df.where(F.col("reject_reason").isNull()).drop("reject_reason")

    local_ts = F.from_utc_timestamp(F.col("creation_time_utc"), LOCAL_TZ)
    clean = clean.select(
        "order_id",
        "lineitem_id",
        F.col("creation_time_utc").alias("order_ts_utc"),
        local_ts.alias("order_ts_local"),
        F.to_date(local_ts).alias("order_date"),
        F.hour(local_ts).alias("order_hour_local"),
        F.col("restaurant_id"),
        F.col("app_name").alias("channel"),
        F.col("user_id"),
        F.col("user_id").isNull().alias("is_guest"),
        F.col("is_loyalty"),
        F.when(F.col("printed_card_number").isNotNull(),
               F.sha2(F.concat(F.lit(salt), F.col("printed_card_number")), 256)).alias("card_hash"),
        F.col("currency"),
        F.col("item_category").alias("item_category_raw"),
        clean_category(F.col("item_category")).alias("item_category"),
        F.trim(F.col("item_name")).alias("item_name"),
        F.col("item_price").cast("decimal(10,2)").alias("item_price"),
        F.col("item_quantity").cast("int").alias("item_quantity"),
        ((F.col("item_price") >= OUTLIER_PRICE) | (F.col("item_quantity") >= OUTLIER_QTY)).alias("is_outlier"),
        F.col("_ingest_ts_utc"),
        F.col("ingest_date"),
    )
    return clean, rejected


def build_options(bronze: DataFrame, clean_items: DataFrame, rejected_items: DataFrame):
    """Keep options whose parent line is clean; reject orphans and children of rejected lines."""
    keys = clean_items.select("order_id", "lineitem_id").withColumn("_ok", F.lit(True))
    bad_parents = (rejected_items.where(F.col("lineitem_id").isNotNull())
                   .select("order_id", "lineitem_id").withColumn("_bad", F.lit(True)))
    df = (bronze.join(keys, ["order_id", "lineitem_id"], "left")
                .join(bad_parents, ["order_id", "lineitem_id"], "left"))
    reason = (F.when(F.col("_ok").isNotNull(), None)
               .when(F.col("_bad").isNotNull(), "parent_rejected")
               .otherwise("orphan_option"))
    df = df.withColumn("reject_reason", reason).drop("_ok", "_bad")

    rejected = df.where(F.col("reject_reason").isNotNull())
    clean = (df.where(F.col("reject_reason").isNull())
             .select("order_id", "lineitem_id",
                     F.trim("option_group_name").alias("option_group_name"),
                     F.trim("option_name").alias("option_name"),
                     F.col("option_price").cast("decimal(10,2)").alias("option_price"),
                     F.col("option_quantity").cast("int").alias("option_quantity"),
                     (F.col("option_price") * F.col("option_quantity")).cast("decimal(12,2)").alias("option_amount"),
                     "_ingest_ts_utc", "ingest_date"))
    return clean, rejected


def build_date_dim(bronze: DataFrame):
    return bronze.select(
        F.col("date_key").cast("date").alias("date_key"),
        F.col("year").cast("int").alias("year"),
        F.col("month").cast("int").alias("month"),
        F.col("week").cast("int").alias("week"),
        F.col("day_of_week"),
        F.col("is_weekend").cast("boolean").alias("is_weekend"),
        F.col("is_holiday").cast("boolean").alias("is_holiday"),
        F.col("holiday_name"),
        F.col("ingest_date"),
    )


def to_quarantine(df: DataFrame, table: str, run_id: str):
    payload = [c for c in df.columns if c not in ("reject_reason", "ingest_date")]
    return df.select(
        F.lit(run_id).alias("run_id"),
        F.lit(table).alias("source_table"),
        F.col("reject_reason"),
        F.to_json(F.struct(*payload)).alias("record_json"),
        F.col("ingest_date"),
    )


def dq_rows(spark, run_id, ingest_date, checks):
    """checks: list of (table, rule, failed, total, blocking)."""
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for table, rule, failed, total, blocking in checks:
        status = "FAIL" if (blocking and failed) else ("WARN" if failed else "PASS")
        rows.append((run_id, ingest_date, now, table, rule, int(failed), int(total), status))
    return spark.createDataFrame(rows, "run_id string, ingest_date string, checked_at_utc string, "
                                       "table_name string, rule string, failed_rows long, "
                                       "total_rows long, status string")


def run_quality_checks(bronze_items, bronze_opts, bronze_dates, clean_items, rej_items, rej_opts):
    total_items = bronze_items.count()
    by_reason = {r["reject_reason"]: r["count"] for r in rej_items.groupBy("reject_reason").count().collect()}
    rej_total = sum(by_reason.values())
    non_test = rej_total - by_reason.get("test_channel", 0) - by_reason.get("test_item", 0)

    order_dates = clean_items.select(F.min("order_date").alias("lo"), F.max("order_date").alias("hi")).collect()[0]
    dup_keys = (bronze_items.groupBy("order_id", "lineitem_id").count().where("count > 1").count())
    checks = [
        ("order_items", "rows_received", 0 if total_items > 0 else 1, total_items, True),
        ("order_items", "duplicate_order_line_keys", dup_keys, total_items, False),
        ("order_items", "test_rows_excluded", by_reason.get("test_channel", 0) + by_reason.get("test_item", 0), total_items, False),
        ("order_items", "invalid_rows_quarantined", non_test, total_items, False),
        ("order_items", "quarantine_under_1pct", 1 if non_test > QUARANTINE_LIMIT * total_items else 0, total_items, True),
        ("order_items", "price_or_qty_outliers_flagged", clean_items.where("is_outlier").count(), total_items, False),
        ("order_item_options", "orphan_or_rejected_parent", rej_opts.count(), bronze_opts.count(), False),
        ("date_dim", "rows_received", 0 if bronze_dates.count() > 0 else 1, bronze_dates.count(), True),
    ]
    return checks, by_reason, order_dates


# --------------------------------------------------------------------------
# Glue I/O
# --------------------------------------------------------------------------
def latest_partition(spark, path, wanted):
    df = spark.read.parquet(path)
    date = wanted or df.agg(F.max("ingest_date")).collect()[0][0]
    return df.where(F.col("ingest_date") == date), str(date)


def write_iceberg(df, table, mode):
    """mode: replace (full snapshot) | partition (overwrite this ingest_date) | append."""
    writer = df.writeTo(table).using("iceberg").tableProperty("format-version", "2")
    exists = df.sparkSession.catalog.tableExists(table)
    if mode == "replace" or not exists:
        if mode == "partition" and not exists:
            writer = writer.partitionedBy(F.col("ingest_date"))
        writer.createOrReplace()
    elif mode == "partition":
        df.writeTo(table).overwritePartitions()
    else:
        df.writeTo(table).append()


def main():
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark import SparkConf
    from pyspark.context import SparkContext

    base = ["JOB_NAME", "LAKE_BUCKET", "CARD_HASH_SALT"]
    opt = [k for k in ["INGEST_DATE"] if f"--{k}" in sys.argv]
    args = getResolvedOptions(sys.argv, base + opt)
    args.setdefault("INGEST_DATE", "")
    lake = args["LAKE_BUCKET"]

    conf = (SparkConf()
            .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{lake}/silver/")
            .set("spark.sql.session.timeZone", "UTC"))
    sc = SparkContext(conf=conf)
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    log = glue_context.get_logger()
    run_id = str(uuid.uuid4())

    spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.gp_silver LOCATION 's3://{lake}/silver/'")

    b_items, ingest_date = latest_partition(spark, f"s3://{lake}/bronze/order_items/", args["INGEST_DATE"])
    b_opts, _ = latest_partition(spark, f"s3://{lake}/bronze/order_item_options/", ingest_date)
    b_dates, _ = latest_partition(spark, f"s3://{lake}/bronze/date_dim/", ingest_date)
    for d in (b_items, b_opts, b_dates):
        d.cache()

    items, rej_items = build_order_items(b_items, args["CARD_HASH_SALT"])
    items.cache(); rej_items.cache()
    opts, rej_opts = build_options(b_opts, items, rej_items)
    dates = build_date_dim(b_dates)

    checks, by_reason, span = run_quality_checks(b_items, b_opts, b_dates, items, rej_items, rej_opts)
    write_iceberg(dq_rows(spark, run_id, ingest_date, checks), "glue_catalog.gp_silver.dq_audit", "append")
    failed = [c for c in checks if c[4] and c[2]]
    if failed:
        raise RuntimeError(f"Blocking data-quality failure(s): {failed}")

    quarantine = (to_quarantine(rej_items, "order_items", run_id)
                  .unionByName(to_quarantine(rej_opts, "order_item_options", run_id)))
    write_iceberg(quarantine, "glue_catalog.gp_silver.quarantine", "partition")
    write_iceberg(items, "glue_catalog.gp_silver.order_items", "replace")
    write_iceberg(opts, "glue_catalog.gp_silver.order_item_options", "replace")
    write_iceberg(dates, "glue_catalog.gp_silver.date_dim_source", "replace")

    summary = {
        "ingest_date": ingest_date,
        "bronze_order_items": b_items.count(),
        "silver_order_items": spark.table("glue_catalog.gp_silver.order_items").count(),
        "silver_order_item_options": spark.table("glue_catalog.gp_silver.order_item_options").count(),
        "rejected_by_reason": by_reason,
        "options_rejected": rej_opts.count(),
        "order_date_range": f"{span['lo']} to {span['hi']}",
    }
    for k, v in summary.items():
        print(f"SILVER  {k:28} {v}")
        log.info(f"[silver] {k}={v}")
    job.commit()


if __name__ == "__main__":
    main()
