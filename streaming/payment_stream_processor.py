"""
Payment Stream Processor
------------------------
Reads real-time payment events from Kinesis using PySpark Structured Streaming.
Applies schema, cleans data, and writes to S3 in bronze/silver layers.

In production this runs on EMR. Locally you can test with a mock JSON source.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, lit,
    unix_timestamp, window, count, avg, sum as spark_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, BooleanType, TimestampType
)
import os

# ── CONFIG ─────────────────────────────────────────────────────────────────
KINESIS_STREAM   = "payment-fraud-stream"
KINESIS_REGION   = "us-east-1"
S3_BUCKET        = "s3://your-bucket-name"          # replace with your bucket
BRONZE_PATH      = f"{S3_BUCKET}/bronze/payments/"  # raw parsed events
SILVER_PATH      = f"{S3_BUCKET}/silver/payments/"  # cleaned, validated events
FRAUD_PATH       = f"{S3_BUCKET}/silver/fraud/"     # flagged transactions only
CHECKPOINT_PATH  = f"{S3_BUCKET}/checkpoints/"      # streaming state
TRIGGER_INTERVAL = "30 seconds"                      # micro-batch interval


# ── SCHEMA ─────────────────────────────────────────────────────────────────
# defines the structure we expect from each Kinesis event
PAYMENT_SCHEMA = StructType([
    StructField("transaction_id",    StringType(),  True),
    StructField("user_id",           StringType(),  True),
    StructField("amount",            DoubleType(),  True),
    StructField("currency",          StringType(),  True),
    StructField("merchant_name",     StringType(),  True),
    StructField("merchant_category", StringType(),  True),
    StructField("user_country",      StringType(),  True),
    StructField("ip_country",        StringType(),  True),
    StructField("device",            StringType(),  True),
    StructField("timestamp",         StringType(),  True),
    StructField("is_fraud",          BooleanType(), True),
    StructField("fraud_type",        StringType(),  True),
])


# ── SPARK SESSION ───────────────────────────────────────────────────────────
def create_spark_session():
    return (
        SparkSession.builder
        .appName("PaymentFraudStreamProcessor")
        # Kinesis connector — included automatically on EMR
        # locally add: .config("spark.jars.packages", "com.qubole.spark:spark-sql-kinesis_2.12:1.2.0_spark-3.0")
        .config("spark.sql.shuffle.partitions", "4")       # keep low for dev
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


# ── READ FROM KINESIS ───────────────────────────────────────────────────────
def read_kinesis_stream(spark):
    """
    Reads raw bytes from Kinesis.
    Each record arrives as base64-encoded bytes in the 'data' column.
    """
    return (
        spark.readStream
        .format("kinesis")
        .option("streamName",         KINESIS_STREAM)
        .option("region",             KINESIS_REGION)
        .option("initialPosition",    "TRIM_HORIZON")   # read from beginning
        .option("awsAccessKeyId",     os.environ.get("AWS_ACCESS_KEY_ID", ""))
        .option("awsSecretKey",       os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
        .load()
    )


# ── BRONZE LAYER — raw parsed events ───────────────────────────────────────
def parse_to_bronze(raw_df):
    """
    Step 1: Parse raw Kinesis bytes into structured columns.
    No filtering or business logic — just schema enforcement.
    This is your source of truth — never delete bronze data.
    """
    return (
        raw_df
        # Kinesis data column is binary — cast to string first
        .selectExpr("CAST(data AS STRING) as json_str",
                    "approximateArrivalTimestamp as kinesis_arrival_time",
                    "shardId as shard_id")
        # parse the JSON string into columns using our schema
        .select(
            from_json(col("json_str"), PAYMENT_SCHEMA).alias("payload"),
            col("kinesis_arrival_time"),
            col("shard_id")
        )
        .select(
            "payload.*",
            "kinesis_arrival_time",
            "shard_id"
        )
        # parse timestamp string into proper timestamp type
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        # add ingestion metadata
        .withColumn("ingestion_layer", lit("bronze"))
    )


# ── SILVER LAYER — cleaned and validated ────────────────────────────────────
def transform_to_silver(bronze_df):
    """
    Step 2: Apply data quality rules and business transformations.
    Drop nulls, standardize values, add derived columns.
    """
    return (
        bronze_df
        # drop records missing critical fields
        .filter(
            col("transaction_id").isNotNull() &
            col("user_id").isNotNull() &
            col("amount").isNotNull() &
            col("amount") > 0                      # amount must be positive
        )
        # standardize country codes to uppercase
        .withColumn("user_country", col("user_country").cast(StringType()))
        .withColumn("ip_country",   col("ip_country").cast(StringType()))
        # flag country mismatch — IP country differs from user's home country
        .withColumn(
            "country_mismatch",
            when(col("user_country") != col("ip_country"), True).otherwise(False)
        )
        # bucket transaction amounts for easier analysis
        .withColumn(
            "amount_bucket",
            when(col("amount") < 10,   lit("micro"))
            .when(col("amount") < 100,  lit("small"))
            .when(col("amount") < 500,  lit("medium"))
            .when(col("amount") < 2000, lit("large"))
            .otherwise(lit("very_large"))
        )
        # extract date parts for partitioning
        .withColumn("event_date", col("event_timestamp").cast("date"))
        .withColumn("event_hour", col("event_timestamp").cast("string").substr(12, 2))
        # update layer tag
        .withColumn("ingestion_layer", lit("silver"))
    )


# ── FRAUD FILTER — suspicious transactions only ─────────────────────────────
def filter_fraud(silver_df):
    """
    Step 3: Isolate flagged transactions for the fraud analytics table.
    These go to a separate S3 path for faster downstream querying.
    """
    return silver_df.filter(col("is_fraud") == True)


# ── WRITE STREAMS ───────────────────────────────────────────────────────────
def write_bronze(bronze_df):
    return (
        bronze_df.writeStream
        .format("parquet")
        .option("path",             BRONZE_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/bronze/")
        .partitionBy("event_date")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .outputMode("append")
        .start()
    )


def write_silver(silver_df):
    return (
        silver_df.writeStream
        .format("parquet")
        .option("path",             SILVER_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/silver/")
        .partitionBy("event_date", "event_hour")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .outputMode("append")
        .start()
    )


def write_fraud(fraud_df):
    return (
        fraud_df.writeStream
        .format("parquet")
        .option("path",             FRAUD_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/fraud/")
        .partitionBy("event_date", "fraud_type")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .outputMode("append")
        .start()
    )


def write_console(silver_df):
    """For local testing — prints output to console instead of S3."""
    return (
        silver_df.writeStream
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .trigger(processingTime="10 seconds")
        .outputMode("append")
        .start()
    )


# ── MAIN ────────────────────────────────────────────────────────────────────
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Payment Stream Processor...")
    print(f"Reading from Kinesis stream: {KINESIS_STREAM}")
    print(f"Writing bronze → {BRONZE_PATH}")
    print(f"Writing silver → {SILVER_PATH}")
    print(f"Writing fraud  → {FRAUD_PATH}")

    # read raw stream
    raw_df = read_kinesis_stream(spark)

    # transform layers
    bronze_df = parse_to_bronze(raw_df)
    silver_df = transform_to_silver(bronze_df)
    fraud_df  = filter_fraud(silver_df)

    # start all three write streams simultaneously
    bronze_query = write_bronze(bronze_df)
    silver_query = write_silver(silver_df)
    fraud_query  = write_fraud(fraud_df)

    print("All streams running. Waiting for data...")

    # keep running until manually stopped
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()