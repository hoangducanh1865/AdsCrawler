# spark_consumer/speed_layer.py
"""
Speed Layer: Spark Structured Streaming
  Source  : Kafka raw topics (fad_ad_daily_report, fad_age_gender_detailed_report)
  Sink    : Kafka processed topics — one per dim/fact table (10 topics total)
  Trigger : every 30 seconds (configurable via SPEED_LAYER_TRIGGER env var)

  Output topics:
    processed_dim_account
    processed_dim_campaign
    processed_dim_adset
    processed_dim_ad
    processed_dim_creative
    processed_dim_date
    processed_fact_fb_ad_daily
    processed_fact_fb_ad_creative_daily
    processed_fad_ad_daily_report
    processed_fact_fb_ad_demographic_daily

  Run (inside Docker, from airflow-scheduler container which has spark-submit + JARs):
    spark-submit --master spark://spark-master:7077 \\
      --jars /opt/airflow/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,\\
/opt/airflow/jars/kafka-clients-3.5.1.jar,\\
/opt/airflow/jars/hadoop-aws.jar,\\
/opt/airflow/jars/aws-java-sdk-bundle.jar,\\
/opt/airflow/jars/commons-pool2.jar \\
      /opt/spark/work-dir/spark_consumer/speed_layer.py
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, IntegerType,
)

# ── Config ─────────────────────────────────────────────────────────────────────

KAFKA_SERVERS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS     = os.getenv("MINIO_ACCESS_KEY",  "admin")
MINIO_SECRET     = os.getenv("MINIO_SECRET_KEY",  "password123")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",      "marketing-datalake")
TRIGGER_INTERVAL = os.getenv("SPEED_LAYER_TRIGGER", "30 seconds")

# Raw input topics
TOPIC_AD_DAILY   = "fad_ad_daily_report"
TOPIC_AGE_GENDER = "fad_age_gender_detailed_report"

# Processed output topics (1 per ClickHouse table)
TOPIC_DIM_ACCOUNT      = "processed_dim_account"
TOPIC_DIM_CAMPAIGN     = "processed_dim_campaign"
TOPIC_DIM_ADSET        = "processed_dim_adset"
TOPIC_DIM_AD           = "processed_dim_ad"
TOPIC_DIM_CREATIVE     = "processed_dim_creative"
TOPIC_DIM_DATE         = "processed_dim_date"
TOPIC_FACT_AD_DAILY    = "processed_fact_fb_ad_daily"
TOPIC_FACT_AD_CREATIVE = "processed_fact_fb_ad_creative_daily"
TOPIC_FACT_DEMOGRAPHIC = "processed_fact_fb_ad_demographic_daily"
TOPIC_FAD_REPORT       = "processed_fad_ad_daily_report"

# ── Input schemas ──────────────────────────────────────────────────────────────

_AD_DAILY_FIELDS = [
    StructField("account_id",              StringType()),
    StructField("account_name",            StringType()),
    StructField("campaign_id",             StringType()),
    StructField("campaign_name",           StringType()),
    StructField("ad_set_id",               StringType()),
    StructField("ad_set_name",             StringType()),
    StructField("id",                      StringType()),
    StructField("name",                    StringType()),
    StructField("date_start",              StringType()),
    StructField("date_stop",               StringType()),
    StructField("user_id",                 StringType()),
    StructField("spend",                   FloatType()),
    StructField("impressions",             IntegerType()),
    StructField("reach",                   IntegerType()),
    StructField("clicks",                  IntegerType()),
    StructField("linkClicks",              IntegerType()),
    StructField("landingPageViews",        IntegerType()),
    StructField("messagingFirstReply",     IntegerType()),
    StructField("newMessagingConnections", IntegerType()),
    StructField("postComments",            IntegerType()),
    StructField("postShares",              IntegerType()),
    StructField("postSaves",               IntegerType()),
    StructField("postReactions",           IntegerType()),
    StructField("photoViews",              IntegerType()),
    StructField("pageLikes",               IntegerType()),
    StructField("postEngagements",         IntegerType()),
    StructField("creative_id",             StringType()),
    StructField("creative_name",           StringType()),
    StructField("thruPlay",                IntegerType()),
    StructField("videoViewsP25",           IntegerType()),
    StructField("videoViewsP50",           IntegerType()),
    StructField("videoViewsP75",           IntegerType()),
    StructField("videoViewsP95",           IntegerType()),
    StructField("videoViewsP100",          IntegerType()),
    StructField("daily_budget",            IntegerType()),
    StructField("year",                    StringType()),
    StructField("month",                   StringType()),
    StructField("day",                     StringType()),
]

AD_DAILY_SCHEMA  = StructType(_AD_DAILY_FIELDS)
AGE_GENDER_SCHEMA = StructType(_AD_DAILY_FIELDS + [
    StructField("age",    StringType()),
    StructField("gender", StringType()),
])

# ── SparkSession ───────────────────────────────────────────────────────────────

def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SpeedLayer_Kafka_Streaming")
        .config("spark.hadoop.fs.s3a.endpoint",        MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",      MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",      MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

# ── Kafka helpers ──────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession, topic: str):
    """Return a streaming DataFrame subscribed to one Kafka topic."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def produce_to_kafka(df: DataFrame, topic: str) -> None:
    """Serialize a batch DataFrame to JSON and write to a Kafka topic."""
    if df is None:
        return
    count = df.count()
    if count == 0:
        print(f"  [SKIP] {topic} — 0 rows")
        return
    (
        df.select(F.to_json(F.struct(*df.columns)).alias("value"))
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", KAFKA_SERVERS)
          .option("topic", topic)
          .save()
    )
    print(f"  [OK]   {topic} — {count} rows")

# ── Transform helpers (logic mirrored from minio_ingest.py) ───────────────────

def _normalize_base(df: DataFrame) -> DataFrame:
    """Rename ad_set_* fields and cast core metric columns — identical to minio_ingest."""
    return (
        df
        .withColumnRenamed("ad_set_id",   "adset_id")
        .withColumnRenamed("ad_set_name", "adset_name")
        .withColumn("date_start",  F.to_date(F.col("date_start"),  "yyyy-MM-dd"))
        .withColumn("date_stop",   F.to_date(F.col("date_stop"),   "yyyy-MM-dd"))
        .withColumn("spend",       F.col("spend").cast("float"))
        .withColumn("impressions", F.col("impressions").cast("int"))
        .withColumn("reach",       F.col("reach").cast("int"))
        .withColumn("clicks",      F.col("clicks").cast("int"))
    )


def _build_dim_date(dates_df: DataFrame) -> DataFrame:
    """
    Streaming version of populate_dim_date: derive calendar attributes only for
    the unique dates present in the current micro-batch (no full date spine).
    Vietnamese public holidays match MockGenerator and minio_ingest.py.
    """
    iso_dow = F.date_format(F.col("date"), "u").cast("int")

    holiday_name = (
        F.when((F.month("date") == 1) & (F.dayofmonth("date") == 1),
               F.lit("Tết Dương Lịch"))
         .when(((F.month("date") == 1) & (F.dayofmonth("date") >= 15)) |
               ((F.month("date") == 2) & (F.dayofmonth("date") <= 5)),
               F.lit("Tết Nguyên Đán"))
         .when((F.month("date") == 4) & (F.dayofmonth("date") == 30),
               F.lit("Ngày Giải Phóng Miền Nam"))
         .when((F.month("date") == 5) & (F.dayofmonth("date") == 1),
               F.lit("Ngày Quốc Tế Lao Động"))
         .when((F.month("date") == 9) & (F.dayofmonth("date") == 2),
               F.lit("Quốc Khánh"))
         .otherwise(F.lit(None).cast("string"))
    )

    return dates_df.select(
        F.col("date"),
        F.year("date").cast("short").alias("year"),
        F.quarter("date").cast("byte").alias("quarter"),
        F.month("date").cast("byte").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.weekofyear("date").cast("byte").alias("week"),
        F.dayofyear("date").cast("short").alias("day_of_year"),
        F.dayofmonth("date").cast("byte").alias("day_of_month"),
        iso_dow.cast("byte").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.when(iso_dow >= 6, F.lit(1)).otherwise(F.lit(0)).cast("byte").alias("is_weekend"),
        F.when(holiday_name.isNotNull(), F.lit(1)).otherwise(F.lit(0)).cast("byte").alias("is_holiday"),
        holiday_name.alias("holiday_name"),
    )

# ── foreachBatch handlers ──────────────────────────────────────────────────────

def process_ad_daily_batch(batch_df: DataFrame, epoch_id: int) -> None:
    """
    foreachBatch handler for fad_ad_daily_report.
    Applies the same transform as process_ad_daily() + populate_dim_date()
    in minio_ingest.py, then produces to 9 processed Kafka topics.
    """
    if batch_df.rdd.isEmpty():
        return

    print(f"\n[epoch={epoch_id}] fad_ad_daily_report — transforming...")
    base = _normalize_base(batch_df)
    base.persist()

    # dim_account
    produce_to_kafka(
        base.select(
            F.col("account_id"),
            F.coalesce(F.col("account_name"), F.lit("Unknown")).alias("account_name"),
        ).dropDuplicates(["account_id"]),
        TOPIC_DIM_ACCOUNT,
    )

    # dim_campaign
    produce_to_kafka(
        base.select(
            F.col("campaign_id"),
            F.col("account_id"),
            F.coalesce(F.col("campaign_name"), F.lit("Unknown")).alias("campaign_name"),
        ).filter(F.col("campaign_id").isNotNull())
         .dropDuplicates(["campaign_id"]),
        TOPIC_DIM_CAMPAIGN,
    )

    # dim_adset
    produce_to_kafka(
        base.select(
            F.col("adset_id"),
            F.col("campaign_id"),
            F.coalesce(F.col("adset_name"), F.lit("Unknown")).alias("adset_name"),
        ).filter(F.col("adset_id").isNotNull())
         .dropDuplicates(["adset_id"]),
        TOPIC_DIM_ADSET,
    )

    # dim_ad
    produce_to_kafka(
        base.select(
            F.col("id").alias("ad_id"),
            F.col("adset_id"),
            F.coalesce(F.col("name"), F.lit("Unknown")).alias("ad_name"),
            F.lit("ACTIVE").alias("status"),
            F.lit("ACTIVE").alias("effective_status"),
            F.lit(None).cast("timestamp").alias("created_time"),
        ).dropDuplicates(["ad_id"]),
        TOPIC_DIM_AD,
    )

    # dim_creative
    produce_to_kafka(
        base.filter(F.col("creative_id").isNotNull())
            .select(
                F.col("creative_id"),
                F.coalesce(F.col("creative_name"), F.lit("")).alias("creative_title"),
                F.lit("").alias("creative_body"),
                F.lit("").alias("creative_thumbnail_raw_url"),
                F.lit("").alias("creative_link"),
                F.lit("Unknown").alias("page_name"),
            ).dropDuplicates(["creative_id"]),
        TOPIC_DIM_CREATIVE,
    )

    # dim_date — only dates seen in this micro-batch (streaming version)
    dates_df = base.select(F.col("date_start").alias("date")).dropDuplicates(["date"])
    produce_to_kafka(_build_dim_date(dates_df), TOPIC_DIM_DATE)

    # fact_fb_ad_daily
    produce_to_kafka(
        base.select(
            F.col("date_start"),
            F.col("account_id"),
            F.col("id").alias("ad_id"),
            F.col("spend"),
            F.col("impressions"),
            F.col("reach"),
            F.col("clicks"),
            F.lit(0.0).cast("float").alias("ctr"),
            F.lit(0.0).cast("float").alias("cpc"),
            F.lit(0.0).cast("float").alias("cpm"),
            F.lit(0.0).cast("float").alias("frequency"),
            F.coalesce(F.col("newMessagingConnections").cast("int"), F.lit(0)).alias("new_messaging_connections"),
            F.lit(0.0).cast("float").alias("cost_per_new_messaging"),
            F.coalesce(F.col("linkClicks").cast("int"),       F.lit(0)).alias("link_clicks"),
            F.coalesce(F.col("landingPageViews").cast("int"), F.lit(0)).alias("landing_page_views"),
        ),
        TOPIC_FACT_AD_DAILY,
    )

    # fact_fb_ad_creative_daily
    produce_to_kafka(
        base.filter(F.col("creative_id").isNotNull())
            .select(
                F.col("date_start"),
                F.col("account_id"),
                F.col("id").alias("ad_id"),
                F.col("creative_id"),
                F.col("spend"),
                F.col("impressions"),
                F.col("reach"),
                F.col("clicks"),
                F.coalesce(F.col("newMessagingConnections").cast("int"), F.lit(0)).alias("new_messaging_connections"),
                F.coalesce(F.col("postEngagements").cast("int"),         F.lit(0)).alias("post_engagements"),
                F.coalesce(F.col("postReactions").cast("int"),           F.lit(0)).alias("post_reactions"),
                F.coalesce(F.col("postShares").cast("int"),              F.lit(0)).alias("post_shares"),
                F.coalesce(F.col("photoViews").cast("int"),              F.lit(0)).alias("photo_views"),
            ),
        TOPIC_FACT_AD_CREATIVE,
    )

    # fad_ad_daily_report (flat/denormalized)
    produce_to_kafka(
        base.select(
            F.col("id"),
            F.coalesce(F.col("name"),          F.lit("")).alias("name"),
            F.col("adset_id"),
            F.coalesce(F.col("adset_name"),    F.lit("")).alias("adset_name"),
            F.col("campaign_id"),
            F.coalesce(F.col("campaign_name"), F.lit("")).alias("campaign_name"),
            F.col("account_id"),
            F.coalesce(F.col("account_name"),  F.lit("")).alias("account_name"),
            F.col("date_start"),
            F.col("date_stop"),
            F.col("spend"),
            F.col("impressions"),
            F.col("reach"),
            F.col("clicks"),
            F.coalesce(F.col("messagingFirstReply").cast("int"),     F.lit(0)).alias("messaging_first_reply"),
            F.coalesce(F.col("newMessagingConnections").cast("int"), F.lit(0)).alias("new_messaging_connections"),
            F.coalesce(F.col("postComments").cast("int"),            F.lit(0)).alias("post_comments"),
            F.coalesce(F.col("linkClicks").cast("int"),              F.lit(0)).alias("link_clicks"),
            F.coalesce(F.col("landingPageViews").cast("int"),        F.lit(0)).alias("landing_page_views"),
            F.coalesce(F.col("pageLikes").cast("int"),               F.lit(0)).alias("page_likes"),
            F.coalesce(F.col("thruPlay").cast("int"),                F.lit(0)).alias("thru_play"),
        ),
        TOPIC_FAD_REPORT,
    )

    base.unpersist()
    print(f"[epoch={epoch_id}] Done.")


def process_age_gender_batch(batch_df: DataFrame, epoch_id: int) -> None:
    """
    foreachBatch handler for fad_age_gender_detailed_report.
    Mirrors process_age_gender() from minio_ingest.py.
    """
    if batch_df.rdd.isEmpty():
        return

    print(f"\n[epoch={epoch_id}] fad_age_gender_detailed_report — transforming...")

    produce_to_kafka(
        batch_df.filter(
            F.col("id").isNotNull()
            & F.col("date_start").isNotNull()
            & F.col("age").isNotNull()
        ).select(
            F.to_date(F.col("date_start"), "yyyy-MM-dd").alias("date_start"),
            F.col("account_id"),
            F.col("id").alias("ad_id"),
            F.col("age"),
            F.coalesce(F.col("gender"),                              F.lit("unknown")).alias("gender"),
            F.coalesce(F.col("spend").cast("float"),                 F.lit(0.0)).alias("spend"),
            F.coalesce(F.col("impressions").cast("int"),             F.lit(0)).alias("impressions"),
            F.coalesce(F.col("reach").cast("int"),                   F.lit(0)).alias("reach"),
            F.coalesce(F.col("clicks").cast("int"),                  F.lit(0)).alias("clicks"),
            F.coalesce(F.col("linkClicks").cast("int"),              F.lit(0)).alias("inline_link_clicks"),
            F.coalesce(F.col("newMessagingConnections").cast("int"), F.lit(0)).alias("new_messaging_connections"),
        ),
        TOPIC_FACT_DEMOGRAPHIC,
    )
    print(f"[epoch={epoch_id}] Done.")

# ── main ───────────────────────────────────────────────────────────────────────

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  Speed Layer — Kafka Structured Streaming")
    print(f"  Broker      : {KAFKA_SERVERS}")
    print(f"  Trigger     : {TRIGGER_INTERVAL}")
    print(f"  Checkpoints : s3a://{MINIO_BUCKET}/checkpoints/speed_layer/")
    print("=" * 60)

    # Stream 1: fad_ad_daily_report → 9 processed topics
    ad_daily_raw = read_kafka_stream(spark, TOPIC_AD_DAILY)
    parsed_ad_daily = ad_daily_raw.select(
        F.from_json(F.col("value").cast("string"), AD_DAILY_SCHEMA).alias("d")
    ).select("d.*")

    q_ad_daily = (
        parsed_ad_daily.writeStream
        .foreachBatch(process_ad_daily_batch)
        .option(
            "checkpointLocation",
            f"s3a://{MINIO_BUCKET}/checkpoints/speed_layer/ad_daily/",
        )
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    # Stream 2: fad_age_gender_detailed_report → processed_fact_fb_ad_demographic_daily
    age_gender_raw = read_kafka_stream(spark, TOPIC_AGE_GENDER)
    parsed_age_gender = age_gender_raw.select(
        F.from_json(F.col("value").cast("string"), AGE_GENDER_SCHEMA).alias("d")
    ).select("d.*")

    q_age_gender = (
        parsed_age_gender.writeStream
        .foreachBatch(process_age_gender_batch)
        .option(
            "checkpointLocation",
            f"s3a://{MINIO_BUCKET}/checkpoints/speed_layer/age_gender/",
        )
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print(f"\nStreaming queries active: {q_ad_daily.id}, {q_age_gender.id}")
    print("Waiting for termination (Ctrl+C to stop)...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
