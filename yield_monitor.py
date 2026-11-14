"""CONSUMER -> PRODUCER: yield monitoring with equipment correlation (a stream join).

Consumes two topics at once:
  * fab.lot.events : builds each lot's genealogy (which tool ran which step, when)
  * fab.alerts     : remembers recent SPC violations per tool

When a lot's InspectionCompleted event shows low yield, it puts the lot on HOLD and
names the suspect tools: tools in that lot's genealogy that had SPC alerts around the
time they processed it. The result is a YIELD_EXCURSION AlertRaised event on fab.alerts.
"""
from collections import defaultdict, deque
from datetime import timedelta

from common import (TOPIC_ALERTS, TOPIC_LOT_EVENTS, Shutdown, create_consumer, create_producer,
                    new_event, parse_iso, poll_event, publish)

SOURCE = "yield-monitor"
YIELD_LIMIT_PCT = 90.0
CORRELATION_BEFORE = timedelta(seconds=120)
CORRELATION_AFTER = timedelta(seconds=30)


class YieldMonitor:
    def __init__(self, producer):
        self.producer = producer
        self.genealogy = defaultdict(list)                    # lot_id -> [(step, tool, completed_at)]
        self.spc_alerts = defaultdict(lambda: deque(maxlen=50))  # tool_id -> recent SPC alerts

    def on_event(self, event):
        etype = event["event_type"]
        if etype == "AlertRaised" and event.get("alert_type") == "SPC_VIOLATION":
            self.spc_alerts[event["tool_id"]].append(event)
        elif etype == "StepCompleted":
            self.genealogy[event["lot_id"]].append(
                (event["step"], event["tool_id"], parse_iso(event["event_time"])))
        elif etype == "InspectionCompleted":
            self.evaluate(event)
        elif etype == "LotCompleted":
            self.genealogy.pop(event["lot_id"], None)

    def suspects_for(self, lot_id):
        suspects = []
        for step, tool_id, completed_at in self.genealogy.get(lot_id, []):
            matches = [a for a in self.spc_alerts[tool_id]
                       if completed_at - CORRELATION_BEFORE <= parse_iso(a["event_time"]) <= completed_at + CORRELATION_AFTER]
            if matches:
                suspects.append({
                    "tool_id": tool_id,
                    "step": step,
                    "parameters": sorted({a["parameter"] for a in matches}),
                    "worst_severity": "CRITICAL" if any(a["severity"] == "CRITICAL" for a in matches) else "WARNING",
                })
        return suspects

    def evaluate(self, inspection):
        lot_id, yield_pct = inspection["lot_id"], inspection["yield_pct"]
        if yield_pct >= YIELD_LIMIT_PCT:
            print(f"{lot_id}: yield {yield_pct}% OK", flush=True)
            return
        suspects = self.suspects_for(lot_id)
        culprit = ", ".join(f"{s['tool_id']} ({'/'.join(s['parameters'])})" for s in suspects) or "unknown - needs engineering review"
        alert = new_event(
            "AlertRaised", SOURCE,
            alert_type="YIELD_EXCURSION", severity="CRITICAL", action="LOT_HOLD",
            lot_id=lot_id, yield_pct=yield_pct, mean_defects=inspection["mean_defects"],
            wafers_passed=inspection["wafers_passed"], suspect_tools=suspects,
            tool_id=suspects[0]["tool_id"] if suspects else None,
            message=f"{lot_id} on HOLD: yield {yield_pct}% < {YIELD_LIMIT_PCT}%. Suspect: {culprit}",
        )
        publish(self.producer, TOPIC_ALERTS, lot_id, alert)
        print(f"[CRITICAL] {alert['message']}", flush=True)


def main():
    shutdown = Shutdown()
    producer = create_producer(SOURCE)
    consumer = create_consumer(SOURCE, [TOPIC_LOT_EVENTS, TOPIC_ALERTS], offset_reset="latest")
    monitor = YieldMonitor(producer)
    print(f"Yield monitor running (hold lots below {YIELD_LIMIT_PCT}% yield). Ctrl+C to stop.", flush=True)

    while not shutdown.requested:
        msg, event = poll_event(consumer, 1.0)
        if msg is not None:
            monitor.on_event(event)

    producer.flush(10)
    consumer.close()


if __name__ == "__main__":
    main()
