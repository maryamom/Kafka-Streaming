#!/usr/bin/env python3
"""
CSV Kafka Producer — streams CSV rows as one message per row.
Topic: transactions-csv (3 partitions). Streams gradually with a small delay.
"""
import argparse
import csv
import os
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Defaults (override with env or args)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_CSV_TOPIC", "transactions-csv")
DELAY_SEC = float(os.environ.get("CSV_PRODUCER_DELAY", "0.1"))
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_csv_path(dirty: bool) -> str:
    name = "transactions_dirty.csv" if dirty else "transactions.csv"
    return os.path.join(PROJECT_ROOT, name)


def main():
    parser = argparse.ArgumentParser(description="Stream CSV rows to Kafka (one row = one message)")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=TOPIC, help="Topic name")
    parser.add_argument("--delay", type=float, default=DELAY_SEC, help="Delay between messages (seconds)")
    parser.add_argument("--dirty", action="store_true", help="Use transactions_dirty.csv (Phase 2)")
    parser.add_argument("--no-delay", action="store_true", help="Send as fast as possible (no delay)")
    args = parser.parse_args()

    csv_path = get_csv_path(args.dirty)
    if not os.path.isfile(csv_path):
        print(f"Error: CSV file not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    delay = 0.0 if args.no_delay else args.delay

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap.split(","),
        value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
    )

    sent = 0
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                print("CSV has no rows (only header or empty).")
                return
            for row in reader:
                if not row or all(not c.strip() for c in row):
                    continue
                line = ",".join(row)
                # Optional: use transaction_id as key for ordering per key (if first column is id)
                key = row[0].strip() if row else None
                producer.send(args.topic, value=line, key=key)
                sent += 1
                if delay > 0:
                    time.sleep(delay)
        producer.flush()
        print(f"Sent {sent} CSV rows to topic '{args.topic}'.")
    except KafkaError as e:
        print(f"Kafka error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
