"""
Local Test Version — Payment Stream Processor (Pandas)
-------------------------------------------------------
Tests all transformation and fraud detection logic locally
using Pandas — no Java, no PySpark, no AWS needed.

Run:
    pip install pandas faker
    python local_test_processor.py
"""

import pandas as pd
import json
import random
import uuid
from datetime import datetime, timezone

def generate_sample_events(n=200, output_file="sample_events.json"):
    merchants   = ["Amazon", "Walmart", "Netflix", "Uber", "Apple Store",
                   "Delta Airlines", "McDonald's", "Shell Gas"]
    countries   = ["US", "US", "US", "US", "CA", "GB"]
    risky       = ["RU", "NG", "CN", "UA", "PK"]
    events      = []

    for i in range(n):
        is_fraud   = random.random() < 0.05
        user_id    = f"USR-{str(random.randint(1, 200)).zfill(4)}"
        avg_spend  = random.uniform(20, 500)
        country    = random.choice(countries)

        if is_fraud:
            fraud_type = random.choice(["large_amount", "foreign_ip", "rapid_fire"])
            if fraud_type == "large_amount":
                amount = round(avg_spend * random.uniform(5, 20), 2)
                ip     = country
            elif fraud_type == "foreign_ip":
                amount = round(random.uniform(100, 2000), 2)
                ip     = random.choice(risky)
            else:
                amount = round(random.uniform(1, 10), 2)
                ip     = country
        else:
            fraud_type = None
            amount     = round(random.uniform(avg_spend * 0.5, avg_spend * 1.5), 2)
            ip         = country

        events.append({
            "transaction_id"   : str(uuid.uuid4()),
            "user_id"          : user_id,
            "amount"           : amount,
            "currency"         : "USD",
            "merchant_name"    : random.choice(merchants),
            "merchant_category": "retail",
            "user_country"     : country,
            "ip_country"       : ip,
            "device"           : random.choice(["mobile", "desktop", "tablet"]),
            "timestamp"        : datetime.now(timezone.utc).isoformat(),
            "is_fraud"         : is_fraud,
            "fraud_type"       : fraud_type,
        })

    with open(output_file, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

    print(f"Generated {n} events -> {output_file}")
    return output_file


def to_bronze(df):
    df = df.copy()
    df["event_timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["ingestion_layer"] = "bronze"
    return df


def to_silver(df):
    df = df.copy()
    df = df.dropna(subset=["transaction_id", "user_id", "amount"])
    df = df[df["amount"] > 0]
    df["country_mismatch"] = df["user_country"] != df["ip_country"]

    def amount_bucket(amount):
        if amount < 10:     return "micro"
        elif amount < 100:  return "small"
        elif amount < 500:  return "medium"
        elif amount < 2000: return "large"
        else:               return "very_large"

    df["amount_bucket"]   = df["amount"].apply(amount_bucket)
    df["event_date"]      = df["event_timestamp"].dt.date
    df["event_hour"]      = df["event_timestamp"].dt.hour
    df["ingestion_layer"] = "silver"
    return df


def filter_fraud(df):
    return df[df["is_fraud"] == True].copy()


def print_results(bronze_df, silver_df, fraud_df):
    print("\n" + "="*60)
    print("PIPELINE RESULTS")
    print("="*60)
    print(f"Bronze (raw events):       {len(bronze_df):>6} records")
    print(f"Silver (after cleaning):   {len(silver_df):>6} records")
    print(f"Fraud events detected:     {len(fraud_df):>6} records")
    print(f"Fraud rate:                {len(fraud_df)/len(silver_df)*100:>5.1f}%")

    print("\n-- Sample silver records (first 5) --")
    print(silver_df[["transaction_id", "user_id", "amount",
                      "amount_bucket", "country_mismatch", "is_fraud"]].head(5).to_string(index=False))

    print("\n-- Fraud events --")
    if len(fraud_df) > 0:
        print(fraud_df[["transaction_id", "user_id", "amount",
                         "user_country", "ip_country", "fraud_type"]].to_string(index=False))
    else:
        print("No fraud events in this batch.")

    print("\n-- Fraud count by type --")
    print(fraud_df["fraud_type"].value_counts().to_string())

    print("\n-- Amount distribution --")
    print(silver_df["amount_bucket"].value_counts().to_string())

    print("\n-- Top 5 merchants --")
    print(silver_df["merchant_name"].value_counts().head(5).to_string())

    print("\n" + "="*60)
    print("Pipeline completed successfully.")
    print("="*60)


def main():
    input_file = generate_sample_events(n=200)
    df         = pd.read_json(input_file, lines=True)
    print(f"Loaded {len(df)} raw events")

    bronze_df = to_bronze(df)
    silver_df = to_silver(bronze_df)
    fraud_df  = filter_fraud(silver_df)

    print_results(bronze_df, silver_df, fraud_df)

    silver_df.to_csv("output_silver.csv", index=False)
    fraud_df.to_csv("output_fraud.csv",   index=False)
    print("\nOutputs saved: output_silver.csv, output_fraud.csv")


if __name__ == "__main__":
    main()