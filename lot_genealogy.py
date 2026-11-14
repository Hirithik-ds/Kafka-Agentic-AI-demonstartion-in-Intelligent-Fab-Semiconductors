"""REPLAY: rebuild the full history of one wafer lot straight from Kafka.

Kafka keeps events after they are consumed (fab.lot.events is kept forever), so any
new consumer can re-read history from offset 0. This script joins a brand-new
consumer group, reads from the beginning and prints the lot's genealogy, which is
what a yield engineer or auditor needs for traceability.

    python lot_genealogy.py              # list lots found
    python lot_genealogy.py LOT-112305   # timeline for one lot
"""
import argparse
import time
import uuid

from common import TOPIC_ALERTS, TOPIC_LOT_EVENTS, create_consumer, parse_iso, poll_event

IDLE_STOP_S = 4


def describe(e):
    t = e["event_type"]
    if t == "LotStarted":
        return f"Lot started: {e['product']}, {e['wafer_count']} wafers, route {' > '.join(e['route'])}"
    if t == "StepStarted":
        return f"{e['step']:<9} started on {e['tool_id']}"
    if t == "StepCompleted":
        return f"{e['step']:<9} completed on {e['tool_id']} ({e['duration_s']}s)"
    if t == "InspectionCompleted":
        return f"INSPECTION: yield {e['yield_pct']}%, {e['wafers_passed']}/25 wafers pass, mean defects {e['mean_defects']}"
    if t == "LotCompleted":
        return "Lot completed"
    if t == "AlertRaised":
        return f"ALERT {e['severity']}: {e['message']}"
    return t


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("lot_id", nargs="?")
    args = parser.parse_args()

    consumer = create_consumer(f"genealogy-{uuid.uuid4().hex[:8]}", [TOPIC_LOT_EVENTS, TOPIC_ALERTS],
                               offset_reset="earliest", verbose=False)
    print("Replaying fab.lot.events and fab.alerts from the beginning...")
    events, lots, scanned = [], {}, 0
    last_msg_at = time.time() + 6  # allow time for the group join
    while time.time() - last_msg_at < IDLE_STOP_S:
        msg, event = poll_event(consumer, 0.5)
        if msg is None:
            continue
        last_msg_at, scanned = time.time(), scanned + 1
        lot_id = event.get("lot_id")
        if not lot_id:
            continue
        if event["event_type"] == "LotStarted":
            lots[lot_id] = None
        if event["event_type"] == "LotCompleted":
            lots[lot_id] = event["yield_pct"]
        if args.lot_id and lot_id == args.lot_id:
            events.append((msg.topic(), msg.partition(), msg.offset(), event))
    consumer.close()
    print(f"Scanned {scanned} events.\n")

    if not args.lot_id:
        print(f"{'Lot':<14}Yield")
        for lot_id, y in lots.items():
            print(f"{lot_id:<14}{'in progress' if y is None else str(y) + '%'}")
        print("\nRun again with a lot id to see its genealogy.")
        return

    if not events:
        print(f"No events found for {args.lot_id}.")
        return
    print(f"Genealogy of {args.lot_id}")
    for topic, partition, offset, e in sorted(events, key=lambda x: parse_iso(x[3]["event_time"])):
        local = parse_iso(e["event_time"]).astimezone().strftime("%H:%M:%S")
        print(f"  {local}  {describe(e):<95} [{topic}[{partition}]@{offset}]")


if __name__ == "__main__":
    main()
