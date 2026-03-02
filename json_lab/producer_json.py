#!/usr/bin/env python3
"""
JSON Kafka Producer — converts CSV rows to JSON and streams to Kafka.
Sends to raw topic; optionally routes valid records to a filtered topic (see consumer or separate pipeline).
Handles malformed records by sending to raw with a 'malformed' flag or skipping and logging.
"""
import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime
from typing import Optional, Tuple

from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = os.environ.get("KAFKA_JSON_TOPIC_RAW", "transactions-raw")
TOPIC_FILTERED = os.environ.get("KAFKA_JSON_TOPIC_FILTERED", "transactions-filtered")
DELAY_SEC = float(os.environ.get("JSON_PRODUCER_DELAY", "0.1"))
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

HEADER = ["transaction_id", "user_id", "amount", "timestamp"]


def parse_amount(s: str):
    if s is None or (isinstance(s, str) and not s.strip()):
        return None
    s = str(s).strip()
    try:
        return float(s)
    except ValueError:
        return None


def parse_int(s: str):
    if s is None or (isinstance(s, str) and not s.strip()):
        return None
    s = str(s).strip()
    try:
        return int(s)
    except ValueError:
        return None


def parse_timestamp(s: str):
    if s is None or (isinstance(s, str) and not s.strip()):
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.isoformat()
        except ValueError:
            continue
    return None


def row_to_json(row: list, headers: list) -> Tuple[Optional[dict], bool]:
    """Convert a CSV row to a JSON-serializable dict. Returns (obj, is_valid)."""
    if len(row) < len(headers):
        return None, False
    d = {}
    valid = True
    for i, h in enumerate(headers):
        raw = row[i].strip() if i < len(row) and row[i] else ""
        if h == "transaction_id":
            v = parse_int(raw)
            if v is None and raw:
                valid = False
            d[h] = v if v is not None else raw
        elif h == "user_id":
            v = parse_int(raw)
            if v is None and raw:
                valid = False
            d[h] = v if v is not None else raw
        elif h == "amount":
            v = parse_amount(raw)
            if v is None and raw:
                valid = False
            d[h] = v if v is not None else raw
        elif h == "timestamp":
            v = parse_timestamp(raw)
            if v is None and raw:
                valid = False
            d[h] = v if v is not None else raw
        else:
            d[h] = raw
    d["_valid"] = valid
    return d, valid


def get_csv_path(dirty: bool) -> str:
    name = "transactions_dirty.csv" if dirty else "transactions.csv"
    return os.path.join(PROJECT_ROOT, name)


def main():
    parser = argparse.ArgumentParser(description="Stream CSV as JSON to Kafka")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP, help="Kafka bootstrap servers")
    parser.add_argument("--topic-raw", default=TOPIC_RAW, help="Raw events topic")
    parser.add_argument("--topic-filtered", default=TOPIC_FILTERED, help="Filtered (valid) events topic")
    parser.add_argument("--delay", type=float, default=DELAY_SEC, help="Delay between messages (seconds)")
    parser.add_argument("--dirty", action="store_true", help="Use transactions_dirty.csv")
    parser.add_argument("--no-delay", action="store_true", help="Send as fast as possible")
    parser.add_argument("--raw-only", action="store_true", help="Send only to raw topic (no filtered)")
    args = parser.parse_args()

    csv_path = get_csv_path(args.dirty)
    if not os.path.isfile(csv_path):
        print(f"Error: CSV file not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    delay = 0.0 if args.no_delay else args.delay

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap.split(","),
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
    )

    sent_raw = 0
    sent_filtered = 0
    malformed = 0
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                print("CSV has no rows.")
                return
            headers = [h.strip() for h in header]
            for row in reader:
                if not row or all(not (c or "").strip() for c in row):
                    continue
                obj, is_valid = row_to_json(row, headers)
                if obj is None:
                    malformed += 1
                    continue
                key = str(obj.get("transaction_id", ""))
                producer.send(args.topic_raw, value=obj, key=key)
                sent_raw += 1
                if is_valid and not args.raw_only:
                    producer.send(args.topic_filtered, value=obj, key=key)
                    sent_filtered += 1
                else:
                    malformed += 1
                if delay > 0:
                    time.sleep(delay)
        producer.flush()
        print(f"Sent {sent_raw} to '{args.topic_raw}', {sent_filtered} to '{args.topic_filtered}'. Malformed/invalid: {malformed}.")
    except KafkaError as e:
        print(f"Kafka error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
