# AWS Payment Fraud Detection Pipeline

A production-grade, real-time payment fraud detection system built on AWS — ingesting payment events via Kinesis, processing with PySpark Structured Streaming on EMR, detecting fraud through behavioral rules, and serving analytics via Redshift. Infrastructure provisioned with Terraform and deployed via GitHub Actions CI/CD.

![CI](https://github.com/Nagarjun21Baldha/aws-payment-fraud-detection/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.x-orange)
![AWS](https://img.shields.io/badge/AWS-Kinesis%20%7C%20EMR%20%7C%20Redshift%20%7C%20Lambda-yellow)
![Terraform](https://img.shields.io/badge/IaC-Terraform-purple)
![License](https://img.shields.io/badge/License-MIT-green)

---

## Architecture

```
Payment Events (Python Producer)
          │
          ▼
  ┌─────────────────┐
  │  Amazon Kinesis  │  ← real-time event ingestion
  │   Data Stream    │
  └────────┬────────┘
           │
           ▼
  ┌─────────────────────────────────────┐
  │     PySpark Structured Streaming     │  ← EMR cluster
  │                                      │
  │  Raw bytes → Bronze → Silver → Fraud │
  └──────┬──────────┬───────────┬────────┘
         │          │           │
         ▼          ▼           ▼
    S3 /bronze  S3 /silver  S3 /fraud
    (raw)       (cleaned)   (flagged)
         │          │           │
         └──────────┴───────────┘
                    │
                    ▼
           ┌─────────────┐          ┌─────────────┐
           │   Redshift   │          │   Lambda    │
           │  Analytics   │          │  + SNS      │
           │  Warehouse   │          │  Alerts     │
           └─────────────┘          └─────────────┘
                    │
                    ▼
             Athena (ad-hoc)
             Glue Catalog
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Event ingestion | Amazon Kinesis Data Streams |
| Stream processing | Apache Spark (PySpark) on Amazon EMR |
| Data lake storage | Amazon S3 (bronze / silver / fraud layers) |
| Data warehouse | Amazon Redshift |
| Metadata catalog | AWS Glue Data Catalog |
| Ad-hoc queries | Amazon Athena |
| Alerting | AWS Lambda + Amazon SNS |
| Infrastructure | Terraform |
| CI/CD | GitHub Actions |
| Language | Python 3.11, PySpark, SQL |

---

## Project Structure

```
aws-payment-fraud-detection/
│
├── producer/
│   ├── payment_producer.py        # simulates payment events → Kinesis
│   └── requirements.txt
│
├── streaming/
│   ├── payment_stream_processor.py  # PySpark Structured Streaming (EMR)
│   └── local_test_processor.py      # local Pandas test (no Java needed)
│
├── fraud_rules/
│   └── fraud_detector.py          # behavioral fraud detection rules engine
│
├── redshift/
│   ├── schema.sql                 # table definitions with DISTKEY/SORTKEY
│   ├── load_data.sql              # COPY commands from S3 + aggregations
│   └── analytics_queries.sql     # 7 production analytics queries
│
├── terraform/
│   ├── main.tf                    # all AWS resources as code
│   ├── variables.tf               # configurable parameters
│   └── outputs.tf                 # resource ARNs and names
│
├── tests/
│   └── README.md
│
├── .github/workflows/
│   └── ci.yml                     # GitHub Actions CI/CD pipeline
│
└── README.md
```

---

## Fraud Detection Rules

The rules engine detects fraud purely from **behavioral patterns** — no pre-labeled data required:

| Rule | Logic | Detects |
|---|---|---|
| Velocity check | 5+ transactions from same user in 10 minutes | Card testing / account takeover |
| Amount spike | Transaction is 3x the user's rolling average spend | Stolen card large purchase |
| Geography anomaly | IP country doesn't match user's home country | Remote account takeover |

Each transaction is tagged with which rules triggered (`flag_velocity`, `flag_spike`, `flag_geography`) and a human-readable `fraud_reason` field.

---

## Data Lake Layers

| Layer | S3 Path | Description |
|---|---|---|
| Bronze | `s3://bucket/bronze/payments/` | Raw parsed events — source of truth, never modified |
| Silver | `s3://bucket/silver/payments/` | Cleaned, validated, enriched with derived columns |
| Fraud | `s3://bucket/silver/fraud/` | Flagged transactions only, partitioned by fraud_type |

---

## Redshift Schema

Five tables optimized for fraud analytics workloads:

- `bronze.payment_events_raw` — raw events
- `silver.payment_events_clean` — cleaned events, primary analytics table (DISTKEY: user_id)
- `fraud.flagged_transactions` — fraud only with all rule flags
- `silver.user_profiles` — rolling spend averages per user
- `silver.merchant_summary` — pre-aggregated merchant stats

Analytics queries cover: daily fraud summary, fraud by merchant category, high-risk user profiles, hourly fraud rate, country mismatch analysis, real-time 15-minute alerts, and week-over-week trend with LAG window functions.

---

## CI/CD Pipeline

GitHub Actions runs automatically on every push:

| Job | What it checks |
|---|---|
| Python Lint & Tests | flake8 lint + unit tests on all 3 Python modules |
| Terraform Validate | validates all AWS infrastructure code |
| SQL Syntax Check | confirms all SQL files exist and are non-empty |
| Security Scan | fails build if AWS credentials are hardcoded |

---

## Running Locally

**Prerequisites:** Python 3.11+, pip

```bash
# clone the repo
git clone https://github.com/Nagarjun21Baldha/aws-payment-fraud-detection.git
cd aws-payment-fraud-detection

# install dependencies
pip install pandas faker boto3 pytest flake8

# run local pipeline test (no AWS or Java needed)
cd streaming
python local_test_processor.py

# run fraud rules engine
cd ../fraud_rules
python fraud_detector.py
```

Expected output:
```
Generated 200 events -> sample_events.json
Bronze (raw events):       200 records
Silver (after cleaning):   200 records
Fraud events detected:      12 records
Fraud rate:                6.0%
```

---

## AWS Deployment

**Prerequisites:** AWS account, Terraform 1.5+, AWS CLI configured

```bash
# provision all AWS infrastructure
cd terraform
terraform init
terraform plan
terraform apply

# upload PySpark job to S3
aws s3 cp streaming/payment_stream_processor.py s3://YOUR_BUCKET/scripts/

# start payment event producer
cd ../producer
python payment_producer.py

# submit PySpark streaming job to EMR
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  s3://YOUR_BUCKET/scripts/payment_stream_processor.py
```

---

## Key Design Decisions

**Why Kinesis over Kafka?**
Kinesis is fully managed on AWS — no cluster to maintain, auto-scales with demand, and integrates natively with Lambda, EMR, and Glue. For an AWS-native stack this eliminates operational overhead.

**Why bronze/silver/gold layers?**
Separating raw, cleaned, and consumption-ready data means failures at any stage don't corrupt source data. Bronze is immutable — if a transformation bug is found, reprocessing from bronze is always possible.

**Why DISTKEY on user_id in Redshift?**
Fraud queries are almost always user-centric — "show me all transactions for USR-0042", "flag users with 5+ fraud events". Distributing by user_id ensures all rows for the same user land on the same Redshift node, making these joins and aggregations extremely fast.

---

## Author

**Nagarjun Baldha**
Senior Data Engineer | AWS & Azure | PySpark · Databricks · Redshift

[LinkedIn](https://linkedin.com/in/nagarjunbaldha) · [GitHub](https://github.com/Nagarjun21Baldha)