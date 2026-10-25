"""GlobalPartners - Bronze ingestion from SQL Server (AWS Glue 5.0, PySpark).

Reads dbo.order_items, dbo.order_item_options and dbo.date_dim from RDS for
SQL Server through a Glue JDBC connection and writes them to S3 as Parquet:

    s3://<LAKE_BUCKET>/bronze/<table>/ingest_date=YYYY-MM-DD/

Job parameters
    --CONNECTION_NAME   Glue connection to SQL Server     (e.g. gp-sqlserver-conn)
    --LAKE_BUCKET       S3 lake bucket name               (e.g. gp-lake-123456789012)
    --MODE              full | incremental                (default full)
    --SINCE_UTC         incremental lower bound, ISO time (e.g. 2024-02-18T00:00:00)
    --INGEST_DATE       partition date, YYYY-MM-DD        (default: today UTC)

Re-running for the same INGEST_DATE overwrites only that partition, so a
failed or repeated run never duplicates data.
"""
import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

REQUIRED = ["JOB_NAME", "CONNECTION_NAME", "LAKE_BUCKET"]
OPTIONAL = {"MODE": "full", "SINCE_UTC": "", "INGEST_DATE": ""}

# Tables to ingest: name -> (incremental filter column or None, numeric split column)
TABLES = {
    "order_items": ("creation_time_utc", "row_id"),
    "order_item_options": (None, "row_id"),   # no timestamp: always full (small table)
    "date_dim": (None, None),
}


def resolve_args():
    present = [k for k in OPTIONAL if f"--{k}" in sys.argv]
    args = getResolvedOptions(sys.argv, REQUIRED + present)
    for k, default in OPTIONAL.items():
        args.setdefault(k, default)
    if not args["INGEST_DATE"]:
        args["INGEST_DATE"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if args["MODE"] not in ("full", "incremental"):
        raise ValueError("MODE must be 'full' or 'incremental'")
    if args["MODE"] == "incremental" and not args["SINCE_UTC"]:
        raise ValueError("incremental MODE requires --SINCE_UTC")
    return args


def jdbc_options(glue_context, connection_name):
    conf = glue_context.extract_jdbc_conf(connection_name)
    url = conf.get("fullUrl") or conf["url"]
    if "databaseName" not in url:
        url = url.rstrip(";") + ";databaseName=GlobalPartners"
    # Glue connections reject extra URL properties, so add encryption here.
    if "encrypt=" not in url:
        url = url.rstrip(";") + ";encrypt=true;trustServerCertificate=true"
    return {"url": url, "user": conf["user"], "password": conf["password"],
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}


def source_query(table, inc_col, args):
    q = f"SELECT * FROM dbo.{table}"
    if args["MODE"] == "incremental" and inc_col:
        q += f" WHERE {inc_col} >= '{args['SINCE_UTC']}'"
    return q


def read_table(spark, opts, table, inc_col, split_col, args):
    query = source_query(table, inc_col, args)

    def reader():
        # Fresh reader each time: DataFrameReader.option() mutates in place,
        # and Spark rejects 'query' and 'dbtable' set together.
        return spark.read.format("jdbc").options(**opts)

    # Count at source for reconciliation (same filter as the extract).
    src_count = (reader().option("query", f"SELECT COUNT_BIG(*) AS n FROM ({query}) q")
                 .load().collect()[0]["n"])

    if split_col and src_count > 50000:
        bounds = (reader().option("query", f"SELECT MIN({split_col}) AS lo, MAX({split_col}) AS hi FROM ({query}) q")
                  .load().collect()[0])
        df = (reader().option("dbtable", f"({query}) src")
              .option("partitionColumn", split_col)
              .option("lowerBound", str(bounds["lo"]))
              .option("upperBound", str(bounds["hi"]))
              .option("numPartitions", "4")
              .option("fetchsize", "10000")
              .load())
    else:
        df = reader().option("query", query).option("fetchsize", "10000").load()
    return df, src_count


def main():
    args = resolve_args()
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    log = glue_context.get_logger()

    opts = jdbc_options(glue_context, args["CONNECTION_NAME"])
    ingest_ts = datetime.now(timezone.utc).isoformat()
    results = []

    for table, (inc_col, split_col) in TABLES.items():
        df, src_count = read_table(spark, opts, table, inc_col, split_col, args)
        df = (df.withColumn("_ingest_ts_utc", F.lit(ingest_ts))
                .withColumn("_ingest_mode", F.lit(args["MODE"]))
                .withColumn("_source", F.lit(f"sqlserver.GlobalPartners.dbo.{table}"))
                .withColumn("ingest_date", F.lit(args["INGEST_DATE"])))

        path = f"s3://{args['LAKE_BUCKET']}/bronze/{table}/"
        df.write.mode("overwrite").partitionBy("ingest_date").parquet(path)

        written = (spark.read.parquet(path)
                   .where(F.col("ingest_date") == args["INGEST_DATE"]).count())
        results.append((table, src_count, written))
        log.info(f"[bronze] {table}: source={src_count} written={written} -> {path}")
        if written != src_count:
            raise RuntimeError(f"Row count mismatch for {table}: source={src_count}, bronze={written}")

    for t, s, w in results:
        print(f"BRONZE OK  {t:20} source={s:>8,}  written={w:>8,}")
    job.commit()


if __name__ == "__main__":
    main()
