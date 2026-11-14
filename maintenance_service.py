"""CONSUMER -> PRODUCER: predictive maintenance automation.

Consumes fab.alerts (consumer group "maintenance-service"). For every CRITICAL SPC
violation on a tool without an open work order, it publishes WorkOrderOpened to
fab.maintenance.work-orders. A simulated technician then fixes the tool and the
service publishes WorkOrderCompleted.

Nobody calls this service directly: the equipment simulator and lot tracker simply
react to the work-order events (tool DOWN / back UP). That is event choreography.
"""
import argparse
import random
import time

from common import (TOPIC_ALERTS, TOPIC_WORK_ORDERS, Shutdown, age_seconds, create_consumer,
                    create_producer, new_event, poll_event, publish)

SOURCE = "maintenance-service"
STALE_AFTER_S = 600  # still act on alerts up to 10 min old (e.g. after this service was stopped)
TECHNICIANS = ["Mesfin", "Sushma", "Jesus", "Hirithik", "Rahul", "Ravi"]
REPAIR_ACTIONS = [
    ("temp", "Replaced thermocouple and re-tuned heater PID loop"),
    ("pressure", "Leak-checked chamber and replaced throttle valve"),
    ("rf_power", "Re-tuned RF match network and checked generator"),
    ("flow", "Recalibrated mass flow controller (MFC)"),
    ("he_backside", "Cleaned electrostatic chuck and replaced He seal"),
    ("dose", "Recalibrated exposure dose sensor"),
    ("focus", "Ran focus calibration on lens and stage"),
    ("overlay", "Ran stage alignment and overlay calibration"),
    ("platen", "Serviced platen motor drive"),
    ("down_force", "Recalibrated carrier head pressure zones"),
]


def repair_action(parameter: str) -> str:
    for keyword, action in REPAIR_ACTIONS:
        if keyword in parameter:
            return action
    return "Performed preventive maintenance and chamber requalification"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--repair-seconds", type=float, default=15.0, help="simulated repair time (default 15)")
    args = parser.parse_args()

    shutdown = Shutdown()
    consumer = create_consumer(SOURCE, [TOPIC_ALERTS])
    producer = create_producer(SOURCE)
    open_orders = {}  # tool_id -> work order dict (incl. due time)
    print("Maintenance service running. Ctrl+C to stop.", flush=True)

    while not shutdown.requested:
        msg, alert = poll_event(consumer, 0.5)
        if (msg is not None and alert.get("alert_type") == "SPC_VIOLATION" and alert.get("severity") == "CRITICAL"
                and alert["tool_id"] not in open_orders and age_seconds(alert) < STALE_AFTER_S):
            tool_id = alert["tool_id"]
            order = {
                "work_order_id": f"WO-{random.randint(10000, 99999)}",
                "tool_id": tool_id,
                "priority": "P1",
                "parameter": alert["parameter"],
                "reason": alert["message"],
                "triggered_by_alert_id": alert["event_id"],
                "assigned_to": random.choice(TECHNICIANS),
            }
            publish(producer, TOPIC_WORK_ORDERS, tool_id, new_event("WorkOrderOpened", SOURCE, **order))
            order["due"] = time.time() + args.repair_seconds
            open_orders[tool_id] = order
            print(f"OPENED   {order['work_order_id']} for {tool_id} -> {order['assigned_to']}: {order['reason']}", flush=True)

        # simulated technicians finishing their jobs
        for tool_id, order in list(open_orders.items()):
            if time.time() >= order["due"]:
                done = new_event("WorkOrderCompleted", SOURCE, work_order_id=order["work_order_id"],
                                 tool_id=tool_id, completed_by=order["assigned_to"],
                                 action_taken=repair_action(order["parameter"]),
                                 duration_s=args.repair_seconds)
                publish(producer, TOPIC_WORK_ORDERS, tool_id, done)
                del open_orders[tool_id]
                print(f"COMPLETED {order['work_order_id']} on {tool_id}: {done['action_taken']}", flush=True)

    producer.flush(10)
    consumer.close()


if __name__ == "__main__":
    main()
