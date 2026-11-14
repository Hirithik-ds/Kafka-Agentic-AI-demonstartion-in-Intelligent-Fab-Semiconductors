"""PRODUCER: simulates the MES moving wafer lots through the process route.

Publishes lot lifecycle events to fab.lot.events, keyed by lot_id so every event
of one lot lands on the same partition in order (= a replayable lot genealogy):

  LotStarted -> StepStarted/StepCompleted (LITHO, ETCH, CVD, CMP) -> InspectionCompleted -> LotCompleted

It also listens to other services to behave like a real line:
  * fab.maintenance.work-orders : tools under maintenance are skipped when dispatching.
  * fab.alerts (SPC)            : a lot processed on a tool that was out of control
                                  comes out of inspection with many more defects.
"""
import argparse
import math
import random
import time

from common import (TOPIC_ALERTS, TOPIC_LOT_EVENTS, TOPIC_WORK_ORDERS, Shutdown, create_consumer,
                    create_producer, drain, new_event, publish, utc_now)
from equipment import METROLOGY_TOOL, PRODUCT, ROUTE, WAFERS_PER_LOT, tools_for_step

SOURCE = "lot-tracker"
STEP_SECONDS = (3.0, 6.0)
# A lot is hit by an excursion if its tool raised an SPC alert from 60 s before to 30 s
# after the lot left it (the drift was already under way before SPC caught it).
EXCURSION_BEFORE_S, EXCURSION_AFTER_S = 60, 30
MAX_DEFECTS_PER_WAFER = 5    # a wafer with more defects fails inspection
BASE_DEFECT_MEAN = 1.5
EXCURSION_DEFECT_MEAN = 12.0


def poisson(lam: float) -> int:
    limit, k, prod = math.exp(-lam), 0, random.random()
    while prod > limit:
        k += 1
        prod *= random.random()
    return k


class Lot:
    def __init__(self, lot_id):
        self.lot_id = lot_id
        self.step_index = -1
        self.tool_id = None
        self.step_started = 0.0
        self.step_ends_at = 0.0
        self.history = []      # (tool_id, completed_at) - simulation only


class LotTracker:
    def __init__(self, producer):
        self.producer = producer
        self.lots = []
        self.down_tools = set()
        self.spc_alert_times = {}  # tool_id -> [time, ...]

    def emit(self, event_type, lot, **payload):
        publish(self.producer, TOPIC_LOT_EVENTS, lot.lot_id,
                new_event(event_type, SOURCE, lot_id=lot.lot_id, **payload))

    # ----- reacting to other services -----
    def on_event(self, event):
        etype, tool_id = event["event_type"], event.get("tool_id")
        if etype == "AlertRaised" and event.get("alert_type") == "SPC_VIOLATION":
            self.spc_alert_times.setdefault(tool_id, []).append(time.time())
        elif etype == "WorkOrderOpened":
            self.down_tools.add(tool_id)
        elif etype == "WorkOrderCompleted":
            self.down_tools.discard(tool_id)

    # ----- lot flow -----
    def start_lot(self):
        lot = Lot(f"LOT-{utc_now():%H%M%S}")
        self.lots.append(lot)
        self.emit("LotStarted", lot, product=PRODUCT, wafer_count=WAFERS_PER_LOT, route=ROUTE)
        print(f"{lot.lot_id} started", flush=True)
        self.try_start_next_step(lot)

    def try_start_next_step(self, lot):
        step = ROUTE[lot.step_index + 1]
        if step == "METROLOGY":
            tool_id = METROLOGY_TOOL
        else:
            available = [t for t in tools_for_step(step) if t not in self.down_tools]
            if not available:
                lot.tool_id = None  # waiting for a tool to come back
                return
            tool_id = random.choice(available)
        lot.step_index += 1
        lot.tool_id = tool_id
        lot.step_started = time.time()
        lot.step_ends_at = lot.step_started + random.uniform(*STEP_SECONDS)
        self.emit("StepStarted", lot, step=step, tool_id=tool_id)

    def complete_step(self, lot):
        step = ROUTE[lot.step_index]
        if step == "METROLOGY":
            self.inspect_and_finish(lot)
            return
        lot.history.append((lot.tool_id, time.time()))
        self.emit("StepCompleted", lot, step=step, tool_id=lot.tool_id,
                  duration_s=round(time.time() - lot.step_started, 1))
        self.try_start_next_step(lot)

    def was_affected(self, lot):
        return any(done - EXCURSION_BEFORE_S <= t <= done + EXCURSION_AFTER_S
                   for tool_id, done in lot.history for t in self.spc_alert_times.get(tool_id, []))

    def inspect_and_finish(self, lot):
        mean = EXCURSION_DEFECT_MEAN if self.was_affected(lot) else BASE_DEFECT_MEAN
        defects = [poisson(mean) for _ in range(WAFERS_PER_LOT)]
        passed = sum(d <= MAX_DEFECTS_PER_WAFER for d in defects)
        yield_pct = round(100.0 * passed / WAFERS_PER_LOT, 1)
        self.emit("InspectionCompleted", lot, step="METROLOGY", tool_id=METROLOGY_TOOL,
                  defects_per_wafer=defects, mean_defects=round(sum(defects) / len(defects), 2),
                  wafers_passed=passed, yield_pct=yield_pct)
        self.emit("LotCompleted", lot, yield_pct=yield_pct)
        self.lots.remove(lot)
        print(f"{lot.lot_id} completed, yield {yield_pct}%", flush=True)

    def tick(self):
        now = time.time()
        for lot in list(self.lots):
            if lot.tool_id is None:            # waiting for equipment
                self.try_start_next_step(lot)
            elif now >= lot.step_ends_at:
                self.complete_step(lot)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--new-lot-every", type=float, default=6.0, help="seconds between lot starts (default 6)")
    parser.add_argument("--max-active-lots", type=int, default=8)
    args = parser.parse_args()

    shutdown = Shutdown()
    producer = create_producer(SOURCE)
    consumer = create_consumer(SOURCE, [TOPIC_ALERTS, TOPIC_WORK_ORDERS], offset_reset="latest")
    tracker = LotTracker(producer)
    next_lot_at = time.time() + 2
    print("Lot tracker running. Ctrl+C to stop.", flush=True)

    while not shutdown.requested:
        for _msg, event in drain(consumer):
            tracker.on_event(event)
        if time.time() >= next_lot_at and len(tracker.lots) < args.max_active_lots:
            tracker.start_lot()
            next_lot_at = time.time() + args.new_lot_every
        tracker.tick()
        producer.poll(0)
        time.sleep(0.25)

    producer.flush(10)
    consumer.close()


if __name__ == "__main__":
    main()
