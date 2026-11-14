"""PRODUCER: simulates fab process tools streaming sensor telemetry (FDC data).

* Every tool publishes one SensorReading per interval to fab.equipment.sensor-readings,
  keyed by tool_id (so each tool's readings stay in order on one partition).
* Now and then a tool starts to DRIFT on one parameter (e.g. an etch chamber heating up).
* It also *consumes* events, which closes the loop of the event-driven design:
    - fab.equipment.commands      InjectFault         -> start a drift on demand
    - fab.maintenance.work-orders WorkOrderOpened     -> tool goes DOWN (stops producing)
                                  WorkOrderCompleted  -> tool repaired, back UP
"""
import argparse
import random
import time

from common import (TOPIC_EQUIPMENT_COMMANDS, TOPIC_SENSOR_READINGS, TOPIC_WORK_ORDERS, Shutdown,
                    create_consumer, create_producer, drain, new_event, publish)
from equipment import TOOLS

SOURCE = "equipment-simulator"
NOISE = 0.6        # healthy process noise, in units of the SPC sigma (a capable process)
DRIFT_STEP = 0.07  # how fast a drifting parameter walks away from target (sigma per reading)
DRIFT_MAX = 5.0


class SimulatedTool:
    def __init__(self, tool_id, spec):
        self.tool_id = tool_id
        self.spec = spec
        self.status = "UP"
        self.sequence = 0
        self.drift_param = None
        self.drift_sigma = 0.0
        self.direction = 1

    def start_drift(self, param=None, direction=None):
        self.drift_param = param or random.choice(list(self.spec["params"]))
        self.direction = direction or random.choice([1, -1])
        self.drift_sigma = 0.0
        arrow = "UP" if self.direction > 0 else "DOWN"
        print(f"!! {self.tool_id}: {self.drift_param} starts drifting {arrow}", flush=True)

    def repair(self):
        self.status = "UP"
        self.drift_param = None
        self.drift_sigma = 0.0

    def read_sensors(self) -> dict:
        values = {}
        for name, p in self.spec["params"].items():
            offset = self.drift_sigma * self.direction if name == self.drift_param else 0.0
            z = offset + random.gauss(0, NOISE)
            values[name] = round(p["target"] + z * p["sigma"], 4)
        if self.drift_param:
            self.drift_sigma = min(self.drift_sigma + DRIFT_STEP, DRIFT_MAX)
        self.sequence += 1
        return values


def handle_event(event, tools):
    tool = tools.get(event.get("tool_id"))
    if tool is None:
        return
    etype = event["event_type"]
    if etype == "InjectFault":
        tool.start_drift(event.get("parameter"), event.get("direction"))
    elif etype == "WorkOrderOpened":
        tool.status = "DOWN"
        print(f"-- {tool.tool_id}: DOWN for maintenance ({event['work_order_id']})", flush=True)
    elif etype == "WorkOrderCompleted":
        tool.repair()
        print(f"++ {tool.tool_id}: repaired and back UP ({event['work_order_id']})", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between readings (default 1)")
    parser.add_argument("--fault-chance", type=float, default=0.03,
                        help="chance per tick that a random drift starts when no tool is drifting (default 0.03)")
    parser.add_argument("--first-fault-after", type=float, default=25, help="seconds of healthy running first")
    parser.add_argument("--no-random-faults", action="store_true", help="only drift when inject_fault.py is used")
    args = parser.parse_args()

    shutdown = Shutdown()
    producer = create_producer(SOURCE)
    consumer = create_consumer(SOURCE, [TOPIC_EQUIPMENT_COMMANDS, TOPIC_WORK_ORDERS], offset_reset="latest")
    tools = {tool_id: SimulatedTool(tool_id, spec) for tool_id, spec in TOOLS.items()}
    started, sent = time.time(), 0
    print(f"Simulating {len(tools)} tools, one reading each every {args.interval}s. Ctrl+C to stop.", flush=True)

    while not shutdown.requested:
        tick = time.time()

        # 1) react to events from other services
        for _msg, event in drain(consumer):
            handle_event(event, tools)

        # 2) maybe start a random drift (one at a time keeps the demo easy to follow)
        nobody_drifting = all(t.drift_param is None and t.status == "UP" for t in tools.values())
        if (not args.no_random_faults and nobody_drifting and tick - started > args.first_fault_after
                and random.random() < args.fault_chance):
            random.choice(list(tools.values())).start_drift()

        # 3) publish one reading per running tool
        for tool in tools.values():
            if tool.status == "DOWN":
                continue
            event = new_event(
                "SensorReading", SOURCE,
                tool_id=tool.tool_id,
                tool_type=tool.spec["tool_type"],
                chamber="CH-A",
                recipe=tool.spec["recipe"],
                sequence=tool.sequence,
                parameters=tool.read_sensors(),
            )
            publish(producer, TOPIC_SENSOR_READINGS, tool.tool_id, event)
            sent += 1

        if sent and sent % (len(tools) * 15) < len(tools):
            print(f".. {sent} readings published", flush=True)
        time.sleep(max(0.0, args.interval - (time.time() - tick)))

    print("Flushing and shutting down...")
    producer.flush(10)
    consumer.close()


if __name__ == "__main__":
    main()
