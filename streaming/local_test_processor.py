"""
Local Test Version — Payment Stream Processor
----------------------------------------------
Use this to test the transformation logic locally WITHOUT needing
AWS Kinesis or EMR. Reads from local JSON files instead.

Run:
    pip install pyspark faker
    python generate_test_data.py     # generates sample_events.json
    python local_test_processor.py   # runs the transformations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, BooleanType
)
import json
import random
import uuid
from datetime import datetime, timezone

# ── GENERATE SAMPLE DATA ────────────────────────────────────────────────────
def generate_sample_events(n=100, output_file="sample_events.json"):
    """Creates a local JSON file with sample payment events for testing."""
    merchants   = ["Amazon", "Walmart", "Netflix", "Uber", "Apple Store"]
    countries   = ["US", "US", "US", "CA", "GB"]
    risky       = ["RU", "NG", "CN"]
    events      = []

    for i in range(n):
        is_fraud  = random.random() < 0.05
        amount    = round(random.uniform(5, 3000) if is_fraud else random.uniform(5, 300), 2)
        country   = random.choice(countries)
        ip        = random.choice(risky) if is_fraud else country

        events.append({
            "transaction_id"  : str(uuid.uuid4()),
            "user_id"         : f"USR-{str(random.randint(1,200)).zfill(4)}",
            "amount"          : amount,
            "currency"        : "USD",
            "merchant_name"   : random.choice(merchants),
            "merchant_category": "retail",
            "user_country"    : country,
            "ip_country"      : ip,
            "device"          : random.choice(["mobile", "desktop", "tablet"]),
            "timestamp"       : datetime.now(timezone.utc).isoformat(),
            "is_fraud"        : is_fraud,
            "fraud_type"      : "foreign_ip" if is_fraud else None,
        })

    with open(output_file, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

    print(f"Generated {n} events → {output_file}")
    return output_file


# ── SCHEMA ──────────────────────────────────────────────────────────────────
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


# ── TRANSFORMATIONS (same logic as production) ──────────────────────────────
def transform(df):
    return (
        df
        .filter(
            col("transaction_id").isNotNull() &
            col("user_id").isNotNull() &
            col("amount").isNotNull() &
            (col("amount") > 0)
        )
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .withColumn(
            "country_mismatch",
            when(col("user_country") != col("ip_country"), True).otherwise(False)
        )
        .withColumn(
            "amount_bucket",
            when(col("amount") < 10,   lit("micro"))
            .when(col("amount") < 100,  lit("small"))
            .when(col("amount") < 500,  lit("medium"))
            .when(col("amount") < 2000, lit("large"))
            .otherwise(lit("very_large"))
        )
        .withColumn("event_date", col("event_timestamp").cast("date"))
    )


# ── MAIN ────────────────────────────────────────────────────────────────────
def main():
    # generate test data
    input_file = generate_sample_events(n=200)

    # start local spark session
    spark = (
        SparkSession.builder
        .appName("LocalPaymentTest")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # read from local JSON (batch mode for local testing)
    df = spark.read.schema(PAYMENT_SCHEMA).json(input_file)

    print(f"\nTotal events loaded: {df.count()}")

    # apply transformations
    silver_df = transform(df)
    fraud_df  = silver_df.filter(col("is_fraud") == True)

    print(f"Valid events after cleaning: {silver_df.count()}")
    print(f"Fraud events detected: {fraud_df.count()}")

    print("\n── Sample silver records ──")
    silver_df.select(
        "transaction_id", "user_id", "amount",
        "amount_bucket", "country_mismatch", "is_fraud", "fraud_type"
    ).show(10, truncate=False)

    print("\n── Fraud events ──")
    fraud_df.select(
        "transaction_id", "user_id", "amount",
        "user_country", "ip_country", "fraud_type"
    ).show(20, truncate=False)

    print("\n── Fraud count by type ──")
    fraud_df.groupBy("fraud_type").count().show()

    print("\n── Amount distribution ──")
    silver_df.groupBy("amount_bucket").count().orderBy("count", ascending=False).show()

    spark.stop()
    print("Done. Check output above.")


if __name__ == "__main__":
    main()