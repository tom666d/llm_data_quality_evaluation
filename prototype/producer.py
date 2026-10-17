"""
producer.py
Reads the existing Lending Club CSV in small batches and sends them
to a Kafka topic to simulate a real-time data stream.

Usage:
    python producer.py
    python producer.py --csv data/demo_lendingclub.csv --interval 2.0
"""

import argparse
import json
import random
import time

import pandas as pd
from kafka import KafkaProducer

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BROKER   = "localhost:9092"   # Change to your broker address on Databricks
TOPIC_NAME     = "loan-stream"
BATCH_SIZE     = 100                # Number of rows per batch
INTERVAL_SEC   = 2.0                # Seconds between batches (lower = faster)
CSV_PATH       = "data/demo_lendingclub.csv"

# Anomaly injection settings (for demo purposes)
INJECT_ANOMALY = True               # Set False to send clean data only
ANOMALY_RATIO  = 0.3                # 30% of batches will contain injected anomalies


# ── Anomaly Injection ─────────────────────────────────────────────────────────
def inject_anomaly(batch_df: pd.DataFrame) -> pd.DataFrame:
    """
    Corrupts a portion of the batch to simulate real-world data quality issues.
    Injects three types of anomalies:
      - Null Spike      : sets 40% of loan_amnt to null
      - Outlier         : sets 8% of annual_inc to an extreme value
      - Format Mismatch : changes 30% of issue_d to a different date format
    """
    df = batch_df.copy()

    # Null Spike — loan_amnt
    null_idx = df.sample(frac=0.4, random_state=42).index
    df.loc[null_idx, "loan_amnt"] = None

    # Statistical Outlier — annual_inc
    outlier_idx = df.sample(frac=0.08, random_state=99).index
    df.loc[outlier_idx, "annual_inc"] = 9_999_999

    # Format Inconsistency — issue_d
    if "issue_d" in df.columns:
        mix_idx = df.sample(frac=0.3, random_state=7).index
        df.loc[mix_idx, "issue_d"] = "01/2020"

    return df


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Kafka producer for loan data streaming demo")
    parser.add_argument("--csv",      default=CSV_PATH,     help="Path to source CSV file")
    parser.add_argument("--broker",   default=KAFKA_BROKER, help="Kafka broker address")
    parser.add_argument("--topic",    default=TOPIC_NAME,   help="Kafka topic name")
    parser.add_argument("--batch",    type=int,   default=BATCH_SIZE,   help="Rows per batch")
    parser.add_argument("--interval", type=float, default=INTERVAL_SEC, help="Seconds between batches")
    args = parser.parse_args()

    # ── Load data ─────────────────────────────────────────────────────────────
    print(f"[LOAD] Reading {args.csv} ...")
    df = pd.read_csv(args.csv)
    total = len(df)
    print(f"[LOAD] {total:,} rows loaded | batch_size={args.batch} | interval={args.interval}s\n")

    # ── Initialize producer ───────────────────────────────────────────────────
    producer = KafkaProducer(
        bootstrap_servers=args.broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # ── Stream loop ───────────────────────────────────────────────────────────
    batch_num = 0
    while True:
        for start in range(0, total, args.batch):
            batch = df.iloc[start: start + args.batch].copy()

            # Decide whether to inject anomalies into this batch
            if INJECT_ANOMALY and random.random() < ANOMALY_RATIO:
                batch = inject_anomaly(batch)
                tag = "⚠️  ANOMALY INJECTED"
            else:
                tag = "✅ clean"

            print(f"[Batch {batch_num:04d}] {tag} | rows={len(batch)}")

            # Send each row as a separate Kafka message
            for _, row in batch.iterrows():
                # Convert NaN to None so the record is JSON-serializable
                record = {k: (None if pd.isna(v) else v) for k, v in row.items()}
                producer.send(args.topic, value=record)

            producer.flush()
            batch_num += 1
            time.sleep(args.interval)

        # Loop back to the beginning after all rows are sent
        print("[LOOP] All rows sent — restarting from the beginning ...\n")
        time.sleep(args.interval * 2)


if __name__ == "__main__":
    main()