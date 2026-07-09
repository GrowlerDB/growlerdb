"""Stage 1 of the streaming demo: synthesize a steady stream of IoT sensor-telemetry readings
as JSON and produce them to a Kafka/Redpanda topic. Tune the rate with RATE (readings/sec).

  generator.py --(JSON)--> Redpanda topic `telemetry` --> sink.py --> Iceberg --> Spark --> GrowlerDB
"""
import json
import os
import random
import time
import uuid

from kafka import KafkaProducer

BROKER = os.environ.get("KAFKA_BROKER", "redpanda:9092")
TOPIC = os.environ.get("TOPIC", "telemetry")
RATE = float(os.environ.get("RATE", "200"))  # readings/sec

DEVICES = [f"device-{i:03d}" for i in range(50)]
SITES = [f"site-{i:02d}" for i in range(6)]
METRICS = ["temperature", "humidity", "pressure", "vibration", "voltage", "current", "rpm", "flow"]
SUBSYSTEMS = ["hvac", "motor", "power", "network", "controller"]
# Weighted: mostly healthy, occasionally degraded — so the demo shows a realistic status mix.
STATUSES = ["ok", "ok", "ok", "ok", "info", "warning", "error", "critical"]


def reading():
    device = random.choice(DEVICES)
    metric = random.choice(METRICS)
    val = random.randint(0, 1000)
    status = random.choice(STATUSES)
    return {
        "id": uuid.uuid4().hex,                 # unique key per reading
        "ts": int(time.time() * 1000),          # epoch ms
        "device_id": device,
        "site": random.choice(SITES),
        "subsystem": random.choice(SUBSYSTEMS),
        "metric": metric,
        "status": status,
        "reading": val,
        "message": f"{metric} reading {val} on {device} status {status}",
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode(),
        linger_ms=50,
        retries=5,
    )
    print(f"generator → {BROKER} topic={TOPIC} rate={RATE}/s", flush=True)
    interval = 1.0 / RATE if RATE > 0 else 0.0
    sent = 0
    while True:
        producer.send(TOPIC, reading())
        sent += 1
        if sent % 1000 == 0:
            print(f"  produced {sent} readings", flush=True)
        if interval:
            time.sleep(interval)


if __name__ == "__main__":
    main()
