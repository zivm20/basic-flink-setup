#!/usr/bin/env python3
"""Write sample JSON events to Kafka for the Flink DynamicIcebergSink lab."""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from envfile import load_env_file, require

try:
    from kafka import KafkaProducer
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency. Install with: pip install kafka-python\n" + str(exc)
    ) from exc


DATASETS = ("orders", "clicks")


def build_event(i: int, evolve_after: int) -> dict:
    dataset = DATASETS[i % 2]
    now = datetime.now(timezone.utc).isoformat()
    if dataset == "orders":
        event = {
            "dataset": "orders",
            "order_id": i,
            "amount": round(random.uniform(5.0, 250.0), 2),
            "status": random.choice(["NEW", "PAID", "SHIPPED"]),
            "event_ts": now,
        }
        # Schema evolution smoke test: add a new field after N messages
        if i >= evolve_after:
            event["priority"] = random.choice(["low", "normal", "high"])
        return event

    return {
        "dataset": "clicks",
        "click_id": i,
        "page": random.choice(["/home", "/search", "/checkout", "/help"]),
        "user_id": f"u-{random.randint(1, 50)}",
        "event_ts": now,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        default="../env/local.kafka.env",
        help="Path to Kafka producer env file",
    )
    parser.add_argument("--count", type=int, default=None, help="Override PRODUCE_COUNT")
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help="Override PRODUCE_INTERVAL_SEC",
    )
    parser.add_argument(
        "--evolve-after",
        type=int,
        default=10,
        help="After this many messages, orders payloads include an extra 'priority' field",
    )
    args = parser.parse_args()

    env = load_env_file(args.env_file)
    bootstrap = require(env, "KAFKA_BOOTSTRAP_SERVERS")
    topic = env.get("KAFKA_TOPIC", "events")
    count = args.count if args.count is not None else int(env.get("PRODUCE_COUNT", "50"))
    interval = (
        args.interval
        if args.interval is not None
        else float(env.get("PRODUCE_INTERVAL_SEC", "0.2"))
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if v is not None else None,
    )

    print(f"Producing {count} messages to {topic} @ {bootstrap}")
    for i in range(count):
        event = build_event(i, args.evolve_after)
        key = event["dataset"]
        fut = producer.send(topic, key=key, value=event)
        fut.get(timeout=30)
        print(f"[{i+1}/{count}] {json.dumps(event)}")
        if interval > 0 and i + 1 < count:
            time.sleep(interval)

    producer.flush()
    producer.close()
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
