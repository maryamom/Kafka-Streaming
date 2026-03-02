#!/usr/bin/env python3
"""
CSV Kafka Consumer — reads messages and prints raw CSV rows with offset tracking.
Supports consumer groups for Tasks 5–7.
"""
import argparse
import os
import sys

from kafka import KafkaConsumer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_CSV_TOPIC", "transactions-csv")
GROUP_ID = os.environ.get("KAFKA_CSV_GROUP", "csv-consumer-group")


def main():
    parser = argparse.ArgumentParser(description="Consume CSV rows from Kafka")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP, help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=TOPIC, help="Topic name")
    parser.add_argument("--group", default=GROUP_ID, help="Consumer group ID")
    parser.add_argument("--from-beginning", action="store_true", help="Read from earliest offset")
    parser.add_argument("--no-group", action="store_true", help="No consumer group (independent consumer)")
    args = parser.parse_args()

    consumer_kw = {
        "bootstrap_servers": args.bootstrap.split(","),
        "topic": args.topic,
        "value_deserializer": lambda v: v.decode("utf-8") if v else "",
        "auto_offset_reset": "earliest" if args.from_beginning else "latest",
    }
    if not args.no_group:
        consumer_kw["group_id"] = args.group

    try:
        consumer = KafkaConsumer(**consumer_kw)
    except KafkaError as e:
        print(f"Kafka error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Consuming from '{args.topic}' (group={args.group or 'none'}). Ctrl+C to stop.", file=sys.stderr)
    try:
        for msg in consumer:
            partition = msg.partition
            offset = msg.offset
            raw_row = msg.value or ""
            print(f"[p={partition} o={offset}] {raw_row}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
