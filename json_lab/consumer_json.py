#!/usr/bin/env python3
"""
JSON Kafka Consumer — parses JSON messages, extracts fields, detects malformed events.
Can consume from raw or filtered topic.
"""
import argparse
import json
import os
import sys

from kafka import KafkaConsumer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = os.environ.get("KAFKA_JSON_TOPIC_RAW", "transactions-raw")
GROUP_ID = os.environ.get("KAFKA_JSON_GROUP", "json-consumer-group")


def main():
    parser = argparse.ArgumentParser(description="Consume JSON transaction events from Kafka")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=TOPIC_RAW, help="Topic name (e.g. transactions-raw or transactions-filtered)")
    parser.add_argument("--group", default=GROUP_ID, help="Consumer group ID")
    parser.add_argument("--from-beginning", action="store_true", help="Read from earliest offset")
    parser.add_argument("--no-group", action="store_true", help="No consumer group")
    args = parser.parse_args()

    consumer_kw = {
        "bootstrap_servers": args.bootstrap.split(","),
        "topic": args.topic,
        "value_deserializer": lambda v: v.decode("utf-8") if v else "{}",
        "auto_offset_reset": "earliest" if args.from_beginning else "latest",
    }
    if not args.no_group:
        consumer_kw["group_id"] = args.group

    try:
        consumer = KafkaConsumer(**consumer_kw)
    except KafkaError as e:
        print(f"Kafka error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Consuming JSON from '{args.topic}' (group={args.group or 'none'}). Ctrl+C to stop.", file=sys.stderr)
    try:
        for msg in consumer:
            raw = msg.value
            partition = msg.partition
            offset = msg.offset
            try:
                data = json.loads(raw) if isinstance(raw, str) else raw
            except json.JSONDecodeError:
                print(f"[p={partition} o={offset}] MALFORMED (not JSON): {raw[:80]}...")
                continue
            if not isinstance(data, dict):
                print(f"[p={partition} o={offset}] MALFORMED (not object): {type(data)}")
                continue
            valid = data.pop("_valid", True)
            tid = data.get("transaction_id")
            uid = data.get("user_id")
            amount = data.get("amount")
            ts = data.get("timestamp")
            if valid and tid is not None:
                print(f"[p={partition} o={offset}] transaction_id={tid} user_id={uid} amount={amount} timestamp={ts}")
            else:
                print(f"[p={partition} o={offset}] MALFORMED/INVALID: {data}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
