"""Shared Kafka settings and helpers used by every service in the fab demo.

Everything that talks to Kafka goes through the small helpers here, so the
individual services stay focused on their business logic.
"""
import json
import os
import signal
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer

# --------------------------------------------------------------------------
# Connection settings (override with environment variables if needed)
# --------------------------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
FAB_ID = "AIDP-FAB"

# --------------------------------------------------------------------------
# Topics: one topic per *kind* of business event
# --------------------------------------------------------------------------
TOPIC_SENSOR_READINGS = "fab.equipment.sensor-readings"
TOPIC_EQUIPMENT_COMMANDS = "fab.equipment.commands"
TOPIC_LOT_EVENTS = "fab.lot.events"
TOPIC_ALERTS = "fab.alerts"
TOPIC_WORK_ORDERS = "fab.maintenance.work-orders"
# Written by the AI root-cause assistant
TOPIC_AI_REPORTS = "fab.ai.incident-reports"
# Written by the Flink SQL jobs (flink/aidp-fab.sql)
TOPIC_FLINK_LINE_YIELD = "fab.analytics.line-yield"
TOPIC_FLINK_TOOL_THROUGHPUT = "fab.analytics.tool-throughput"
TOPIC_FLINK_ALERT_COUNTS = "fab.analytics.alert-counts"

DAY_MS = str(24 * 60 * 60 * 1000)

TOPICS = {
    # High-volume telemetry, keyed by tool_id -> 6 partitions so consumers can scale out.
    TOPIC_SENSOR_READINGS: {"partitions": 6, "config": {"retention.ms": DAY_MS}},
    # Operator / engineer commands to the equipment (e.g. inject a fault for the demo).
    TOPIC_EQUIPMENT_COMMANDS: {"partitions": 1, "config": {}},
    # Lot genealogy is a compliance record in a fab: keep it forever (-1).
    TOPIC_LOT_EVENTS: {"partitions": 3, "config": {"retention.ms": "-1"}},
    TOPIC_ALERTS: {"partitions": 3, "config": {}},
    TOPIC_WORK_ORDERS: {"partitions": 3, "config": {}},
    TOPIC_AI_REPORTS: {"partitions": 1, "config": {"retention.ms": "-1"}},
    TOPIC_FLINK_LINE_YIELD: {"partitions": 1, "config": {"retention.ms": DAY_MS}},
    TOPIC_FLINK_TOOL_THROUGHPUT: {"partitions": 1, "config": {"retention.ms": DAY_MS}},
    TOPIC_FLINK_ALERT_COUNTS: {"partitions": 1, "config": {"retention.ms": DAY_MS}},
}


# --------------------------------------------------------------------------
# Event envelope
# --------------------------------------------------------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime = None) -> str:
    return (dt or utc_now()).isoformat(timespec="milliseconds")


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


def age_seconds(event: dict) -> float:
    """How old an event is (event time vs. now)."""
    return (utc_now() - parse_iso(event["event_time"])).total_seconds()


def new_event(event_type: str, source: str, **payload) -> dict:
    """Every event shares the same envelope: id, type, time, source, fab."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": to_iso(),
        "source": source,
        "fab_id": FAB_ID,
        **payload,
    }


# --------------------------------------------------------------------------
# Producer helpers
# --------------------------------------------------------------------------
def create_producer(client_id: str) -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": client_id,
        "acks": "all",                 # wait for the broker to fully persist the event
        "enable.idempotence": True,    # no duplicates on retries
        "linger.ms": 10,               # tiny batching window for throughput
        "compression.type": "lz4",
    })


def _delivery_report(err, msg):
    if err is not None:
        print(f"[delivery-failed] topic={msg.topic()} key={msg.key()} error={err}", flush=True)


def publish(producer: Producer, topic: str, key: str, event: dict) -> None:
    """Send one event. The key decides the partition -> per-key ordering."""
    kwargs = dict(
        topic=topic,
        key=key,
        value=json.dumps(event),
        headers={"event_type": event["event_type"], "source": event["source"]},
        on_delivery=_delivery_report,
    )
    try:
        producer.produce(**kwargs)
    except BufferError:            # local queue full: let it drain, then retry once
        producer.poll(1.0)
        producer.produce(**kwargs)
    producer.poll(0)               # serve delivery callbacks


# --------------------------------------------------------------------------
# Consumer helpers
# --------------------------------------------------------------------------
def create_consumer(group_id: str, topics: list, client_id: str = None,
                    offset_reset: str = "earliest", verbose: bool = True) -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "client.id": client_id or group_id,
        "auto.offset.reset": offset_reset,
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,   # commit every second so consumer lag on the dashboard is accurate
    })

    def on_assign(_consumer, partitions):
        if verbose:
            parts = ", ".join(f"{p.topic}[{p.partition}]" for p in partitions) or "(none)"
            print(f"[{client_id or group_id}] partitions assigned: {parts}", flush=True)

    def on_revoke(_consumer, partitions):
        if verbose and partitions:
            parts = ", ".join(f"{p.topic}[{p.partition}]" for p in partitions)
            print(f"[{client_id or group_id}] partitions revoked (rebalance): {parts}", flush=True)

    consumer.subscribe(topics, on_assign=on_assign, on_revoke=on_revoke)
    return consumer


def poll_event(consumer: Consumer, timeout: float = 1.0):
    """Poll one message and decode it. Returns (msg, event) or (None, None)."""
    msg = consumer.poll(timeout)
    if msg is None:
        return None, None
    if msg.error():
        if msg.error().code() != KafkaError._PARTITION_EOF:
            print(f"[consumer-error] {msg.error()}", flush=True)
        return None, None
    try:
        return msg, json.loads(msg.value())
    except (TypeError, ValueError):
        print(f"[bad-message] {msg.topic()}[{msg.partition()}]@{msg.offset()} is not JSON", flush=True)
        return None, None


def drain(consumer: Consumer, max_messages: int = 500):
    """Yield all messages that are immediately available (non-blocking)."""
    for _ in range(max_messages):
        msg, event = poll_event(consumer, 0)
        if msg is None:
            return
        yield msg, event


# --------------------------------------------------------------------------
# Graceful shutdown on Ctrl+C / SIGTERM
# --------------------------------------------------------------------------
class Shutdown:
    def __init__(self):
        self.requested = False
        signal.signal(signal.SIGINT, self._handler)
        signal.signal(signal.SIGTERM, self._handler)

    def _handler(self, *_):
        self.requested = True
