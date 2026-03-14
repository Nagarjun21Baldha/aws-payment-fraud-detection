import boto3
import json
import random
import time
import uuid
from datetime import datetime, timezone
from faker import Faker

fake = Faker()

# ── CONFIG ─────────────────────────────────────────────────────────────────
STREAM_NAME    = "payment-fraud-stream"   # your Kinesis stream name
AWS_REGION     = "us-east-1"             # change to your region
EVENTS_PER_SEC = 10                       # how many payments to send per second
FRAUD_RATE     = 0.05                     # 5% of transactions will be fraudulent

# ── REFERENCE DATA ─────────────────────────────────────────────────────────
MERCHANTS = [
    {"name": "Amazon",         "category": "retail"},
    {"name": "Walmart",        "category": "retail"},
    {"name": "Netflix",        "category": "subscription"},
    {"name": "Uber",           "category": "transport"},
    {"name": "Delta Airlines", "category": "travel"},
    {"name": "Walgreens",      "category": "pharmacy"},
    {"name": "McDonald's",     "category": "food"},
    {"name": "Apple Store",    "category": "electronics"},
    {"name": "Shell Gas",      "category": "fuel"},
    {"name": "Marriott Hotel", "category": "hotel"},
]

COUNTRIES = ["US", "US", "US", "US", "US", "CA", "GB", "AU", "DE", "FR"]
HIGH_RISK_COUNTRIES = ["RU", "NG", "CN", "BR", "IN", "UA", "PK"]

# simulate a pool of users with their typical spending patterns
USER_POOL = [
    {"user_id": f"USR-{str(i).zfill(4)}", "avg_txn": random.uniform(20, 500)}
    for i in range(1, 201)   # 200 users
]


# ── FRAUD PATTERN GENERATORS ───────────────────────────────────────────────

def normal_transaction(user):
    """Generates a completely normal payment event."""
    merchant = random.choice(MERCHANTS)
    country  = random.choice(COUNTRIES)
    amount   = round(random.uniform(
        user["avg_txn"] * 0.5,
        user["avg_txn"] * 1.5
    ), 2)
    return {
        "transaction_id"  : str(uuid.uuid4()),
        "user_id"         : user["user_id"],
        "amount"          : amount,
        "currency"        : "USD",
        "merchant_name"   : merchant["name"],
        "merchant_category": merchant["category"],
        "user_country"    : country,
        "ip_country"      : country,          # ip matches user country — normal
        "device"          : random.choice(["mobile", "desktop", "tablet"]),
        "timestamp"       : datetime.now(timezone.utc).isoformat(),
        "is_fraud"        : False,
        "fraud_type"      : None,
    }


def fraud_large_amount(user):
    """Fraud pattern 1: unusually large transaction — 5x to 20x normal spending."""
    merchant = random.choice(MERCHANTS)
    amount   = round(user["avg_txn"] * random.uniform(5, 20), 2)
    country  = random.choice(COUNTRIES)
    return {
        "transaction_id"  : str(uuid.uuid4()),
        "user_id"         : user["user_id"],
        "amount"          : amount,
        "currency"        : "USD",
        "merchant_name"   : merchant["name"],
        "merchant_category": merchant["category"],
        "user_country"    : country,
        "ip_country"      : country,
        "device"          : random.choice(["mobile", "desktop"]),
        "timestamp"       : datetime.now(timezone.utc).isoformat(),
        "is_fraud"        : True,
        "fraud_type"      : "large_amount",
    }


def fraud_foreign_ip(user):
    """Fraud pattern 2: transaction from a high-risk country IP."""
    merchant   = random.choice(MERCHANTS)
    ip_country = random.choice(HIGH_RISK_COUNTRIES)
    amount     = round(random.uniform(100, 2000), 2)
    return {
        "transaction_id"  : str(uuid.uuid4()),
        "user_id"         : user["user_id"],
        "amount"          : amount,
        "currency"        : "USD",
        "merchant_name"   : merchant["name"],
        "merchant_category": merchant["category"],
        "user_country"    : "US",
        "ip_country"      : ip_country,       # IP doesn't match user country — suspicious
        "device"          : "desktop",
        "timestamp"       : datetime.now(timezone.utc).isoformat(),
        "is_fraud"        : True,
        "fraud_type"      : "foreign_ip",
    }


def fraud_rapid_fire(user):
    """Fraud pattern 3: rapid repeated transactions — card testing behavior."""
    merchant = random.choice(MERCHANTS)
    amount   = round(random.uniform(1, 10), 2)   # small amounts — typical card test
    country  = random.choice(COUNTRIES)
    return {
        "transaction_id"  : str(uuid.uuid4()),
        "user_id"         : user["user_id"],
        "amount"          : amount,
        "currency"        : "USD",
        "merchant_name"   : merchant["name"],
        "merchant_category": merchant["category"],
        "user_country"    : country,
        "ip_country"      : country,
        "device"          : "desktop",
        "timestamp"       : datetime.now(timezone.utc).isoformat(),
        "is_fraud"        : True,
        "fraud_type"      : "rapid_fire",
    }


FRAUD_PATTERNS = [fraud_large_amount, fraud_foreign_ip, fraud_rapid_fire]


# ── KINESIS PUBLISHER ──────────────────────────────────────────────────────

def get_kinesis_client():
    return boto3.client("kinesis", region_name=AWS_REGION)


def publish_event(client, event):
    response = client.put_record(
        StreamName   = STREAM_NAME,
        Data         = json.dumps(event).encode("utf-8"),
        PartitionKey = event["user_id"],   # partition by user so same user's events go to same shard
    )
    return response


# ── MAIN LOOP ──────────────────────────────────────────────────────────────

def run_producer():
    client = get_kinesis_client()
    total_sent  = 0
    total_fraud = 0

    print(f"Starting payment producer → stream: {STREAM_NAME}")
    print(f"Rate: {EVENTS_PER_SEC} events/sec | Fraud rate: {FRAUD_RATE*100}%")
    print("-" * 60)

    while True:
        user = random.choice(USER_POOL)

        # decide normal or fraud
        if random.random() < FRAUD_RATE:
            fraud_fn = random.choice(FRAUD_PATTERNS)
            event    = fraud_fn(user)
            total_fraud += 1
        else:
            event = normal_transaction(user)

        try:
            publish_event(client, event)
            total_sent += 1

            # print summary every 50 events
            if total_sent % 50 == 0:
                fraud_pct = round((total_fraud / total_sent) * 100, 1)
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] "
                    f"Sent: {total_sent} | Fraud: {total_fraud} ({fraud_pct}%) | "
                    f"Last: {event['fraud_type'] or 'normal'} — "
                    f"${event['amount']} @ {event['merchant_name']}"
                )

        except Exception as e:
            print(f"Error publishing event: {e}")

        time.sleep(1 / EVENTS_PER_SEC)


if __name__ == "__main__":
    run_producer()