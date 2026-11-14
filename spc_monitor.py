"""CONSUMER -> PRODUCER: Statistical Process Control (SPC) on live sensor data.

Reads fab.equipment.sensor-readings (consumer group "spc-monitor") and checks every
parameter against its control limits using two classic SPC rules:

  * OUT_OF_CONTROL  one point beyond 3 sigma                     -> CRITICAL
  * DRIFT           4 of the last 5 points beyond 1 sigma, same side -> WARNING (early warning)

(These are Western Electric rules 1 and 3, the classic SPC run rules.)

Violations are published as AlertRaised events to fab.alerts (key = tool_id).

Scaling demo: start a second copy with `--instance 2`. Kafka splits the 6 partitions
between the two instances. Because readings are keyed by tool_id, all readings of one
tool always go to the same instance, so the per-tool history kept here stays correct.
"""
import argparse
import time
from collections import defaultdict, deque

from common import (TOPIC_ALERTS, TOPIC_SENSOR_READINGS, Shutdown, age_seconds, create_consumer,
                    create_producer, new_event, parse_iso, poll_event, publish)
from equipment import TOOLS, control_limits, param_spec, z_score

SOURCE = "spc-monitor"
GROUP_ID = "spc-monitor"
DRIFT_WINDOW, DRIFT_HITS = 5, 4
ALERT_COOLDOWN_S = 20
STALE_AFTER_S = 60  # don't raise alarms for old readings (e.g. replaying a backlog)
RESTART_GAP_S = 5   # tool silent this long (e.g. down for maintenance) -> start a fresh SPC run


class SpcEngine:
    def __init__(self):
        self.history = defaultdict(lambda: deque(maxlen=DRIFT_WINDOW))  # (tool, param) -> recent z
        self.last_alert = {}  # (tool, param, rule) -> time
        self.last_seen = {}   # tool -> event time of its previous reading

    def check(self, reading: dict) -> list:
        tool_id = reading["tool_id"]
        if tool_id not in TOOLS:
            return []
        # After a gap (tool was down / repaired) old points must not count towards run rules.
        seen_at = parse_iso(reading["event_time"])
        previous = self.last_seen.get(tool_id)
        if previous is not None and (seen_at - previous).total_seconds() > RESTART_GAP_S:
            for param in reading["parameters"]:
                self.history[(tool_id, param)].clear()
        self.last_seen[tool_id] = seen_at

        violations = []
        for param, value in reading["parameters"].items():
            z = z_score(tool_id, param, value)
            hist = self.history[(tool_id, param)]
            hist.append(z)
            if abs(z) > 3:
                violations.append(("OUT_OF_CONTROL", "CRITICAL", param, value, z, "up" if z > 0 else "down"))
            elif sum(v > 1 for v in hist) >= DRIFT_HITS:
                violations.append(("DRIFT", "WARNING", param, value, z, "up"))
            elif sum(v < -1 for v in hist) >= DRIFT_HITS:
                violations.append(("DRIFT", "WARNING", param, value, z, "down"))
        return [v for v in violations if self._not_in_cooldown(tool_id, v[2], v[0])]

    def _not_in_cooldown(self, tool_id, param, rule):
        key, now = (tool_id, param, rule), time.time()
        if now - self.last_alert.get(key, 0) < ALERT_COOLDOWN_S:
            return False
        self.last_alert[key] = now
        return True


def build_alert(reading, rule, severity, param, value, z, direction):
    tool_id = reading["tool_id"]
    lcl, ucl = control_limits(tool_id, param)
    spec = param_spec(tool_id, param)
    if rule == "OUT_OF_CONTROL":
        message = f"{tool_id} {param}={value}{spec['unit']} outside control limits [{lcl:g}, {ucl:g}]"
    else:
        message = f"{tool_id} {param} drifting {direction}: {DRIFT_HITS} of last {DRIFT_WINDOW} points beyond 1 sigma"
    return new_event(
        "AlertRaised", SOURCE,
        alert_type="SPC_VIOLATION", severity=severity, rule=rule,
        tool_id=tool_id, recipe=reading.get("recipe"), parameter=param,
        value=value, target=spec["target"], lcl=round(lcl, 4), ucl=round(ucl, 4),
        z_score=round(z, 2), reading_event_id=reading["event_id"], message=message,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--instance", default="1", help="instance name, run several to see consumer-group scaling")
    args = parser.parse_args()

    client_id = f"{SOURCE}-{args.instance}"
    shutdown = Shutdown()
    consumer = create_consumer(GROUP_ID, [TOPIC_SENSOR_READINGS], client_id=client_id)
    producer = create_producer(client_id)
    engine = SpcEngine()
    processed, alerts = 0, 0
    print(f"{client_id} running in consumer group '{GROUP_ID}'. Ctrl+C to stop.", flush=True)

    while not shutdown.requested:
        msg, reading = poll_event(consumer, 1.0)
        if msg is None or reading.get("event_type") != "SensorReading":
            continue
        processed += 1
        violations = engine.check(reading)
        if violations and age_seconds(reading) > STALE_AFTER_S:
            continue  # history updated, but no alarms for stale data
        for violation in violations:
            alert = build_alert(reading, *violation)
            publish(producer, TOPIC_ALERTS, reading["tool_id"], alert)
            alerts += 1
            print(f"[{alert['severity']}] {alert['message']}  (partition {msg.partition()}, offset {msg.offset()})", flush=True)
        if processed % 300 == 0:
            print(f".. processed {processed} readings, raised {alerts} alerts", flush=True)

    producer.flush(10)
    consumer.close()  # leaves the group cleanly -> immediate rebalance for other instances


if __name__ == "__main__":
    main()
