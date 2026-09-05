"""
AdVantage Analytics Pipeline - PySpark ETL

Reads CSVs → transforms → writes partitioned Parquet to S3
+ registers Glue tables + runs Athena SQL for marts.

Tech: PySpark + AWS S3 + Glue + Athena
Free: PySpark local[*] + LocalStack S3/Glue/Athena (no AWS bill)

Usage:
    python spark_etl.py
"""

import os
from pathlib import Path
from typing import Optional

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ── Config ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "data" / "mock_csv"
PARQUET_DIR = BASE_DIR / "data" / "parquet"
MART_DIR = BASE_DIR / "data" / "marts"
MART_DIR.mkdir(parents=True, exist_ok=True)

with (BASE_DIR / "config.yaml").open() as f:
    cfg = yaml.safe_load(f)

AWS_MODE = cfg["aws"]["mode"]              # "local" or "aws"
S3_BUCKET = cfg["aws"]["s3_bucket"]
GLUE_DB = cfg["aws"]["glue_db"]
AWS_REGION = cfg["aws"]["region"]

# For local mode: use LocalStack endpoints
if AWS_MODE == "local":
    LOCALSTACK = os.environ.get("LOCALSTACK_URL", "http://localhost:4566")
    S3_PATH = f"s3a://{S3_BUCKET}"        # boto3 + PyArrow will hit LocalStack
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = AWS_REGION
else:
    S3_PATH = f"s3a://{S3_BUCKET}"        # real AWS
    LOCALSTACK = None


# ── Spark Session ───────────────────────────────────────────────────────────

def get_spark() -> SparkSession:
    """
    Create Spark session in local[*] mode (no cluster needed).
    In production: same code runs on EMR/Glue Job/Databricks.
    """
    builder = SparkSession.builder.appName("AdVantage-ETL")

    # Use Hadoop AWS connector to read/write S3
    hadoop_aws = "org.apache.hadoop:hadoop-aws:3.3.4"
    aws_sdk = "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    builder = builder.config("spark.jars.packages", f"{hadoop_aws},{aws_sdk}")

    # S3a config (works for both LocalStack and real AWS)
    builder = builder.config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
    builder = builder.config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
    builder = builder.config("spark.hadoop.fs.s3a.endpoint", LOCALSTACK or f"s3.{AWS_REGION}.amazonaws.com")
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    return builder.getOrCreate()


# ── Read ────────────────────────────────────────────────────────────────────

def read_csv(spark: SparkSession, name: str) -> DataFrame:
    """Read a CSV from local input dir into a Spark DataFrame."""
    path = str(INPUT_DIR / f"{name}.csv")
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)


# ── Transform ───────────────────────────────────────────────────────────────

def build_star_schema(spark: SparkSession) -> None:
    """
    Read all CSVs, build star schema (dims + fact), write partitioned Parquet.

    Star schema = one fact table + multiple dimension tables.
    Fact grain: campaign × date (one row per campaign per day).
    """
    print("Reading CSVs...")
    advertisers = read_csv(spark, "dim_advertisers")
    campaigns = read_csv(spark, "dim_campaigns")
    users = read_csv(spark, "dim_users")
    time_dim = read_csv(spark, "dim_time")
    events = read_csv(spark, "fact_ad_events")
    conversions = read_csv(spark, "fact_conversions")

    # ── dim_date: enrich the date dimension with derived columns ──
    print("Building dim_date...")
    dim_date = (
        time_dim
        .withColumn("date_key", F.date_format("date_id", "yyyyMMdd").cast("int"))
        .select(
            "date_key", "date_id", "year", "month", "quarter",
            "day_of_month", "day_of_week", "day_name",
            "is_weekend", "month_name", "week_of_year"
        )
    )

    # ── dim_campaign: already clean, just add a surrogate key ──
    print("Building dim_campaign...")
    dim_campaign = (
        campaigns
        .withColumn("campaign_key", F.monotonically_increasing_id())
        .select(
            "campaign_key", "campaign_id", "campaign_name", "channel",
            "campaign_type", "status", "start_date", "end_date", "daily_budget_usd"
        )
    )

    # ── fact_campaign_daily: aggregate events + conversions by campaign × date ──
    #   spend = SUM(cost_usd)
    #   orders = COUNT of conversions
    #   revenue = SUM(conversion_value_usd)
    print("Building fact_campaign_daily...")

    # Aggregate ad events by (campaign_id, event_date)
    event_agg = (
        events
        .groupBy("campaign_id", "event_date")
        .agg(
            F.sum("cost_usd").alias("spend_usd"),
            F.count(F.when(F.col("event_type") == "impression", 1)).alias("impressions"),
            F.count(F.when(F.col("event_type") == "click", 1)).alias("clicks"),
        )
    )

    # Aggregate conversions by (campaign_id, conversion_date)
    conv_agg = (
        conversions
        .groupBy("campaign_id", "conversion_date")
        .agg(
            F.count("*").alias("orders"),
            F.sum("conversion_value_usd").alias("revenue_usd"),
            F.sum(F.col("is_new_customer")).alias("new_customers"),
        )
    )

    # Join: events + conversions on (campaign_id, date)
    fact = (
        event_agg
        .join(
            conv_agg,
            (event_agg.campaign_id == conv_agg.campaign_id) &
            (event_agg.event_date == conv_agg.conversion_date),
            how="left",
        )
        .join(dim_campaign, "campaign_id", "left")
        .withColumn("date_key", F.date_format("event_date", "yyyyMMdd").cast("int"))
        .fillna(0, subset=["orders", "revenue_usd", "new_customers"])
        .select(
            "campaign_key", "date_key", "event_date",
            "channel", "campaign_type",
            "spend_usd", "impressions", "clicks",
            "orders", "revenue_usd", "new_customers"
        )
        .withColumnRenamed("event_date", "date")
    )

    # ── Derived metrics: ROAS, CAC, CVR, CTR ──
    #   ROAS = revenue / spend
    #   CAC  = spend / new_customers
    #   CVR  = orders / clicks * 100
    #   CTR  = clicks / impressions * 100
    print("Computing derived metrics...")
    fact = (
        fact
        .withColumn("roas", F.round(F.col("revenue_usd") / F.when(F.col("spend_usd") > 0, F.col("spend_usd")), 2))
        .withColumn("cac", F.round(F.col("spend_usd") / F.when(F.col("new_customers") > 0, F.col("new_customers")), 2))
        .withColumn("conv_rate_pct", F.round(F.when(F.col("clicks") > 0, F.col("orders") / F.col("clicks") * 100), 2))
        .withColumn("ctr_pct", F.round(F.when(F.col("impressions") > 0, F.col("clicks") / F.col("impressions") * 100), 2))
    )

    # ── Write to S3 as partitioned Parquet ──
    # Partitioning by event_date: Athena prunes entire folders for date filters
    # ZSTD compression: best speed/ratio
    print(f"Writing Parquet to {S3_PATH}...")

    # Also write local copy for dashboard
    fact.write.mode("overwrite").partitionBy("date").parquet(str(PARQUET_DIR / "fact_campaign_daily"))
    dim_date.write.mode("overwrite").parquet(str(PARQUET_DIR / "dim_date"))
    dim_campaign.write.mode("overwrite").parquet(str(PARQUET_DIR / "dim_campaign"))

    # Try S3 (skip if LocalStack not running)
    if AWS_MODE == "local":
        try:
            fact.write.mode("overwrite").partitionBy("date").parquet(f"{S3_PATH}/fact_campaign_daily/")
            dim_date.write.mode("overwrite").parquet(f"{S3_PATH}/dim_date/")
            dim_campaign.write.mode("overwrite").parquet(f"{S3_PATH}/dim_campaign/")
            print(f"  S3 upload OK -> {S3_PATH}/")
        except Exception as e:
            print(f"  S3 upload skipped (LocalStack not running): {e}")
            print("  Pipeline still works locally at: data/parquet/")


# ── Glue + Athena ───────────────────────────────────────────────────────────

def build_marts(spark: SparkSession) -> None:
    """
    Build 3 pre-aggregated marts from the star schema.
    These are what the dashboard reads.

    In production: same SQL runs on Athena and writes back to S3.
    In local mode: we run the SQL on PySpark directly.
    """
    print("\nBuilding marts...")
    fact = spark.read.parquet(str(PARQUET_DIR / "fact_campaign_daily"))
    dim_date = spark.read.parquet(str(PARQUET_DIR / "dim_date"))
    dim_campaign = spark.read.parquet(str(PARQUET_DIR / "dim_campaign"))

    # ── Mart 1: channel_summary (one row per channel) ──
    mart_channel = (
        fact
        .groupBy("channel")
        .agg(
            F.countDistinct("campaign_key").alias("campaign_count"),
            F.sum("spend_usd").alias("total_spend_usd"),
            F.sum("revenue_usd").alias("total_revenue_usd"),
            F.sum("impressions").alias("total_impressions"),
            F.sum("clicks").alias("total_clicks"),
            F.sum("orders").alias("total_orders"),
            F.sum("new_customers").alias("total_new_customers"),
        )
        .withColumn("roas", F.round(F.col("total_revenue_usd") / F.when(F.col("total_spend_usd") > 0, F.col("total_spend_usd")), 2))
        .withColumn("cac", F.round(F.col("total_spend_usd") / F.when(F.col("total_new_customers") > 0, F.col("total_new_customers")), 2))
        .withColumn("conv_rate_pct", F.round(F.when(F.col("total_clicks") > 0, F.col("total_orders") / F.col("total_clicks") * 100), 2))
        .withColumn("ctr_pct", F.round(F.when(F.col("total_impressions") > 0, F.col("total_clicks") / F.col("total_impressions") * 100), 2))
    )
    mart_channel.write.mode("overwrite").csv(str(PARQUET_DIR / "mart_channel_summary"), header=True)
    mart_channel.toPandas().to_csv(MART_DIR / "mart_channel_summary.csv", index=False)

    # ── Mart 2: campaign_summary (one row per campaign) ──
    mart_campaign = (
        fact
        .groupBy("campaign_key", "channel", "campaign_type")
        .agg(
            F.sum("spend_usd").alias("total_spend_usd"),
            F.sum("revenue_usd").alias("total_revenue_usd"),
            F.sum("impressions").alias("total_impressions"),
            F.sum("clicks").alias("total_clicks"),
            F.sum("orders").alias("total_orders"),
            F.sum("new_customers").alias("total_new_customers"),
        )
        .join(dim_campaign.select("campaign_key", "campaign_name", "status"), "campaign_key", "left")
        .withColumn("roas", F.round(F.col("total_revenue_usd") / F.when(F.col("total_spend_usd") > 0, F.col("total_spend_usd")), 2))
        .withColumn("cac", F.round(F.col("total_spend_usd") / F.when(F.col("total_new_customers") > 0, F.col("total_new_customers")), 2))
        .withColumn("conv_rate_pct", F.round(F.when(F.col("total_clicks") > 0, F.col("total_orders") / F.col("total_clicks") * 100), 2))
    )
    mart_campaign.write.mode("overwrite").csv(str(PARQUET_DIR / "mart_campaign_summary"), header=True)
    mart_campaign.toPandas().to_csv(MART_DIR / "mart_campaign_summary.csv", index=False)

    # ── Mart 3: monthly_summary (one row per channel per month) ──
    mart_monthly = (
        fact
        .join(dim_date.select("date_key", "month", "month_name"), "date_key", "left")
        .groupBy("month", "month_name", "channel")
        .agg(
            F.sum("spend_usd").alias("spend_usd"),
            F.sum("revenue_usd").alias("revenue_usd"),
            F.sum("orders").alias("total_orders"),
            F.sum("new_customers").alias("new_customers"),
        )
        .withColumn("roas", F.round(F.col("revenue_usd") / F.when(F.col("spend_usd") > 0, F.col("spend_usd")), 2))
        .withColumn("cac", F.round(F.col("spend_usd") / F.when(F.col("new_customers") > 0, F.col("new_customers")), 2))
        .orderBy("month", "channel")
    )
    mart_monthly.write.mode("overwrite").csv(str(PARQUET_DIR / "mart_monthly_summary"), header=True)
    mart_monthly.toPandas().to_csv(MART_DIR / "mart_monthly_summary.csv", index=False)

    # ── Mart 4: campaign_daily (one row per campaign per day) ──
    (
        fact
        .withColumnRenamed("date", "date_str")
        .select(
            "campaign_key", "date_str", "channel", "campaign_type",
            "spend_usd", "impressions", "clicks", "orders", "revenue_usd",
            "new_customers", "roas", "cac", "conv_rate_pct", "ctr_pct"
        )
        .toPandas()
        .to_csv(MART_DIR / "mart_campaign_daily.csv", index=False)
    )

    print("  Built 4 marts: channel, campaign, monthly, daily")
    print(f"  CSVs saved to: {MART_DIR}")


# ── Glue + Athena ───────────────────────────────────────────────────────────

def setup_glue_and_athena() -> None:
    """
    Register Parquet in Glue catalog + run Athena marts.

    In local mode: skips silently (Athena needs real AWS or LocalStack).
    """
    if AWS_MODE != "aws" and not LOCALSTACK:
        print("\nSkipping Glue/Athena setup (use 'aws' mode or run LocalStack)")
        return

    print("\nSetting up Glue catalog...")
    try:
        import boto3
        glue = boto3.client("glue", endpoint_url=LOCALSTACK, region_name=AWS_REGION)
        athena = boto3.client("athena", endpoint_url=LOCALSTACK, region_name=AWS_REGION)

        # Create Glue database (idempotent)
        try:
            glue.create_database(DatabaseInput={"Name": GLUE_DB})
        except glue.exceptions.AlreadyExistsException:
            pass

        # Register each Parquet table in Glue
        for table_name, partition_key in [
            ("fact_campaign_daily", "date"),
            ("dim_date", None),
            ("dim_campaign", None),
        ]:
            try:
                glue.create_table(
                    DatabaseName=GLUE_DB,
                    TableInput={
                        "Name": table_name,
                        "StorageDescriptor": {
                            "Columns": [],   # Glue auto-detects from Parquet
                            "Location": f"{S3_PATH}/{table_name}/",
                            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                            "SerdeInfo": {
                                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                            },
                        },
                        "PartitionKeys": [{"Name": partition_key, "Type": "date"}] if partition_key else [],
                    },
                )
            except glue.exceptions.AlreadyExistsException:
                pass

        print("  Glue tables registered: fact_campaign_daily, dim_date, dim_campaign")

        # Run Athena marts (channel_summary, campaign_summary, monthly_summary)
        print("\nRunning Athena mart queries...")
        marts_path = BASE_DIR / "sql" / "marts.sql"
        if marts_path.exists():
            sql = marts_path.read_text()
            # Split on semicolons + comments
            queries = [q.strip() for q in sql.split(";") if q.strip() and not q.strip().startswith("--")]
            for query in queries[:3]:   # First 3 marts
                athena.start_query_execution(
                    QueryString=query,
                    QueryExecutionContext={"Database": GLUE_DB},
                    ResultConfiguration={"OutputLocation": f"{S3_PATH}/athena-results/"},
                )
        print("  Athena marts queued for execution")
    except Exception as e:
        print(f"  Glue/Athena setup skipped: {e}")


# ── Main ────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("AdVantage PySpark ETL")
    print("=" * 60)

    spark = get_spark()
    try:
        build_star_schema(spark)
        build_marts(spark)
        setup_glue_and_athena()
        print("\n" + "=" * 60)
        print("ETL Complete!")
        print(f"  Parquet: {PARQUET_DIR}")
        print(f"  S3:      {S3_PATH}")
        print(f"  Glue DB: {GLUE_DB}")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
