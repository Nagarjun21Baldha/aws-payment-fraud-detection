"""
Fraud Rules Engine
------------------
Detects fraudulent transactions purely from behavioral patterns
without relying on pre-labeled data.

3 rules:
  1. Velocity check    — 5+ transactions from same user in 10 minutes
  2. Amount spike      — transaction is 3x the user's rolling average
  3. Geography anomaly — IP country doesn't match user's home country

Run:
    python fraud_detector.py
"""

import pandas as pd
import json
import random
import uuid
from datetime import datetime, timezone, timedelta


# ── GENERATE RICHER TEST DATA ───────────────────────────────────────────────
def generate_events(n=500, output_file="fraud_test_events.json"):
    """
    Generates 500 events with realistic fraud patterns baked in.
    Includes rapid-fire bursts, large spikes, and foreign IP events.
    """
    merchants  = ["Amazon", "Walmart", "Netflix", "Uber", "Apple Store",
                  "Delta Airlines", "McDonald's", "Shell Gas", "Marriott", "Target"]
    countries  = ["US", "US", "US", "US", "CA", "GB"]
    risky      = ["RU", "NG", "CN", "UA", "PK"]
    events     = []
    base_time  = datetime.now(timezone.utc)

    # build user spending profiles
    users = {
        f"USR-{str(i).zfill(4)}": round(random.uniform(30, 400), 2)
        for i in range(1, 101)
    }

    for i in range(n):
        user_id   = random.choice(list(users.keys()))
        avg_spend = users[user_id]
        country   = random.choice(countries)
        # spread events over last 2 hours
        timestamp = base_time - timedelta(minutes=random.randint(0, 120))

        events.append({
            "transaction_id"   : str(uuid.uuid4()),
            "user_id"          : user_id,
            "amount"           : round(random.uniform(avg_spend * 0.5, avg_spend * 1.5), 2),
            "currency"         : "USD",
            "merchant_name"    : random.choice(merchants),
            "merchant_category": "retail",
            "user_country"     : country,
            "ip_country"       : country,
            "device"           : random.choice(["mobile", "desktop", "tablet"]),
            "timestamp"        : timestamp.isoformat(),
            "avg_spend"        : avg_spend,
        })

    # ── inject fraud pattern 1: velocity burst ──────────────────────────────
    # pick 3 users and give them 6 rapid transactions within 5 minutes
    burst_users = random.sample(list(users.keys()), 3)
    for user_id in burst_users:
        burst_time = base_time - timedelta(minutes=random.randint(5, 60))
        for j in range(6):
            events.append({
                "transaction_id"   : str(uuid.uuid4()),
                "user_id"          : user_id,
                "amount"           : round(random.uniform(1, 15), 2),
                "currency"         : "USD",
                "merchant_name"    : random.choice(merchants),
                "merchant_category": "retail",
                "user_country"     : "US",
                "ip_country"       : "US",
                "device"           : "desktop",
                "timestamp"        : (burst_time + timedelta(seconds=j*30)).isoformat(),
                "avg_spend"        : users[user_id],
            })

    # ── inject fraud pattern 2: amount spike ────────────────────────────────
    # pick 5 users and give them one massive transaction
    spike_users = random.sample(list(users.keys()), 5)
    for user_id in spike_users:
        avg = users[user_id]
        events.append({
            "transaction_id"   : str(uuid.uuid4()),
            "user_id"          : user_id,
            "amount"           : round(avg * random.uniform(6, 15), 2),
            "currency"         : "USD",
            "merchant_name"    : "Apple Store",
            "merchant_category": "electronics",
            "user_country"     : "US",
            "ip_country"       : "US",
            "device"           : "desktop",
            "timestamp"        : (base_time - timedelta(minutes=random.randint(1, 30))).isoformat(),
            "avg_spend"        : avg,
        })

    # ── inject fraud pattern 3: foreign IP ──────────────────────────────────
    # pick 4 users and give them a transaction from a risky country
    foreign_users = random.sample(list(users.keys()), 4)
    for user_id in foreign_users:
        events.append({
            "transaction_id"   : str(uuid.uuid4()),
            "user_id"          : user_id,
            "amount"           : round(random.uniform(200, 2000), 2),
            "currency"         : "USD",
            "merchant_name"    : random.choice(merchants),
            "merchant_category": "retail",
            "user_country"     : "US",
            "ip_country"       : random.choice(risky),
            "device"           : "desktop",
            "timestamp"        : (base_time - timedelta(minutes=random.randint(1, 20))).isoformat(),
            "avg_spend"        : users[user_id],
        })

    # shuffle so fraud events are mixed in
    random.shuffle(events)

    with open(output_file, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

    print(f"Generated {len(events)} events -> {output_file}")
    print(f"  Injected: 3 velocity bursts, 5 amount spikes, 4 foreign IP events")
    return output_file, users


# ── RULE 1: VELOCITY CHECK ──────────────────────────────────────────────────
def rule_velocity(df, window_minutes=10, max_txns=5):
    """
    Flags users who make more than max_txns transactions
    within any rolling window_minutes period.
    Classic card testing behavior.
    """
    df = df.sort_values("event_timestamp")
    flagged_ids = set()

    for user_id, group in df.groupby("user_id"):
        times = group["event_timestamp"].tolist()
        # sliding window check
        for i in range(len(times)):
            window_end   = times[i]
            window_start = window_end - pd.Timedelta(minutes=window_minutes)
            count = sum(1 for t in times if window_start <= t <= window_end)
            if count > max_txns:
                flagged_ids.add(user_id)
                break

    return flagged_ids


# ── RULE 2: AMOUNT SPIKE ────────────────────────────────────────────────────
def rule_amount_spike(df, spike_multiplier=3.0):
    """
    Flags transactions where the amount is more than
    spike_multiplier times the user's known average spend.
    """
    df = df.copy()
    df["spike_ratio"] = df["amount"] / df["avg_spend"]
    flagged = df[df["spike_ratio"] > spike_multiplier]["transaction_id"].tolist()
    return set(flagged)


# ── RULE 3: GEOGRAPHY ANOMALY ───────────────────────────────────────────────
def rule_geography(df):
    """
    Flags transactions where the IP country doesn't
    match the user's home country.
    """
    flagged = df[df["user_country"] != df["ip_country"]]["transaction_id"].tolist()
    return set(flagged)


# ── APPLY ALL RULES ─────────────────────────────────────────────────────────
def apply_fraud_rules(df):
    """
    Runs all three rules and combines results.
    A transaction is flagged if ANY rule triggers.
    """
    print("\nRunning fraud detection rules...")

    # run each rule
    velocity_users  = rule_velocity(df)
    spike_txns      = rule_amount_spike(df)
    geo_txns        = rule_geography(df)

    print(f"  Rule 1 - Velocity:  {len(velocity_users)} users flagged")
    print(f"  Rule 2 - Amount spike: {len(spike_txns)} transactions flagged")
    print(f"  Rule 3 - Foreign IP:   {len(geo_txns)} transactions flagged")

    # apply flags to dataframe
    df = df.copy()
    df["flag_velocity"]  = df["user_id"].isin(velocity_users)
    df["flag_spike"]     = df["transaction_id"].isin(spike_txns)
    df["flag_geography"] = df["transaction_id"].isin(geo_txns)

    # overall fraud flag — any rule triggered
    df["is_fraud_detected"] = (
        df["flag_velocity"] |
        df["flag_spike"]    |
        df["flag_geography"]
    )

    # fraud reason — which rules triggered
    def fraud_reason(row):
        reasons = []
        if row["flag_velocity"]:  reasons.append("velocity")
        if row["flag_spike"]:     reasons.append("amount_spike")
        if row["flag_geography"]: reasons.append("foreign_ip")
        return ", ".join(reasons) if reasons else None

    df["fraud_reason"] = df.apply(fraud_reason, axis=1)

    return df


# ── PRINT RESULTS ───────────────────────────────────────────────────────────
def print_results(df):
    fraud_df = df[df["is_fraud_detected"] == True]
    clean_df = df[df["is_fraud_detected"] == False]

    print("\n" + "="*60)
    print("FRAUD DETECTION RESULTS")
    print("="*60)
    print(f"Total transactions:        {len(df):>6}")
    print(f"Clean transactions:        {len(clean_df):>6}")
    print(f"Flagged as fraud:          {len(fraud_df):>6}")
    print(f"Detection rate:            {len(fraud_df)/len(df)*100:>5.1f}%")

    print("\n-- Fraud breakdown by rule --")
    print(f"  Velocity flag:    {df['flag_velocity'].sum():>4} transactions")
    print(f"  Amount spike:     {df['flag_spike'].sum():>4} transactions")
    print(f"  Foreign IP:       {df['flag_geography'].sum():>4} transactions")

    print("\n-- Sample flagged transactions --")
    cols = ["transaction_id", "user_id", "amount", "user_country",
            "ip_country", "fraud_reason"]
    print(fraud_df[cols].head(10).to_string(index=False))

    print("\n-- Fraud by reason --")
    print(fraud_df["fraud_reason"].value_counts().to_string())

    print("\n-- Top flagged users --")
    print(fraud_df["user_id"].value_counts().head(5).to_string())

    print("\n" + "="*60)
    print("Rules engine completed successfully.")
    print("="*60)

    return fraud_df


# ── MAIN ────────────────────────────────────────────────────────────────────
def main():
    # generate test data with injected fraud patterns
    input_file, users = generate_events(n=500)

    # load into dataframe
    df = pd.read_json(input_file, lines=True)
    df["event_timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    print(f"Loaded {len(df)} transactions")

    # run fraud rules
    df = apply_fraud_rules(df)

    # print results
    fraud_df = print_results(df)

    # save outputs
    df.to_csv("output_all_transactions.csv",  index=False)
    fraud_df.to_csv("output_fraud_detected.csv", index=False)
    print("\nOutputs saved:")
    print("  output_all_transactions.csv  — full dataset with fraud flags")
    print("  output_fraud_detected.csv    — flagged transactions only")


if __name__ == "__main__":
    main()