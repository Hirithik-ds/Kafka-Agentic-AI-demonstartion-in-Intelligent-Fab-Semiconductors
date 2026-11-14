"""CONSUMER: live fab control-room dashboard in the terminal.

Subscribes to all four business topics with its own consumer group, so it receives
a full copy of every event without taking anything away from the other services
(Kafka fan-out). Pure read model: it builds its view only from events.
"""
import time
import uuid
from collections import Counter, OrderedDict, deque

from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from common import (TOPIC_ALERTS, TOPIC_LOT_EVENTS, TOPIC_SENSOR_READINGS, TOPIC_WORK_ORDERS, Shutdown,
                    create_consumer, drain, parse_iso)
from equipment import TOOLS, z_score

TOPICS = [TOPIC_SENSOR_READINGS, TOPIC_LOT_EVENTS, TOPIC_ALERTS, TOPIC_WORK_ORDERS]
SPARK = "▁▂▃▄▅▆▇█"
STATUS_STYLE = {"UP": "bold green", "WARNING": "bold yellow", "CRITICAL": "bold red", "DOWN": "bold magenta", "NO DATA": "dim"}
ALERT_HOLD_S = 15


def sparkline(values):
    return "".join(SPARK[min(len(SPARK) - 1, int(abs(v) / 4.0 * (len(SPARK) - 1)))] for v in values)


def hhmmss(iso_time):
    return parse_iso(iso_time).astimezone().strftime("%H:%M:%S")


class DashboardState:
    def __init__(self):
        self.tools = {t: {"worst": None, "trend": deque(maxlen=16), "count": 0, "alert": None,
                          "alert_at": 0.0, "down": False} for t in TOOLS}
        self.lots = OrderedDict()
        self.alerts = deque(maxlen=10)
        self.orders = OrderedDict()
        self.topic_counts = Counter()
        self.rate_window = deque()  # (time, topic)
        self.started = time.time()

    def apply(self, topic, event):
        now = time.time()
        self.topic_counts[topic] += 1
        self.rate_window.append(now)
        etype = event["event_type"]

        if etype == "SensorReading" and event["tool_id"] in self.tools:
            tool = self.tools[event["tool_id"]]
            zs = {p: z_score(event["tool_id"], p, v) for p, v in event["parameters"].items()}
            worst = max(zs, key=lambda p: abs(zs[p]))
            tool["worst"] = (worst, zs[worst])
            tool["trend"].append(zs[worst])
            tool["count"] += 1

        elif etype == "AlertRaised":
            self.alerts.appendleft(event)
            if event.get("alert_type") == "SPC_VIOLATION" and event["tool_id"] in self.tools:
                tool = self.tools[event["tool_id"]]
                if tool["alert"] != "CRITICAL" or now - tool["alert_at"] > ALERT_HOLD_S:
                    tool["alert"] = event["severity"]
                tool["alert_at"] = now
            if event.get("action") == "LOT_HOLD" and event["lot_id"] in self.lots:
                self.lots[event["lot_id"]]["status"] = "HOLD"

        elif etype in ("WorkOrderOpened", "WorkOrderCompleted"):
            opened = etype == "WorkOrderOpened"
            self.orders[event["work_order_id"]] = event if opened else {**self.orders.get(event["work_order_id"], {}), **event}
            self.orders[event["work_order_id"]]["status"] = "OPEN" if opened else "DONE"
            self.orders.move_to_end(event["work_order_id"])
            while len(self.orders) > 6:
                self.orders.popitem(last=False)
            if event["tool_id"] in self.tools:
                self.tools[event["tool_id"]]["down"] = opened
                if not opened:
                    self.tools[event["tool_id"]]["alert"] = None

        elif "lot_id" in event and topic == TOPIC_LOT_EVENTS:
            lot = self.lots.setdefault(event["lot_id"], {"step": "-", "tool": "-", "status": "RUN", "yield": None})
            if etype == "StepStarted":
                lot.update(step=event["step"], tool=event["tool_id"])
            elif etype == "InspectionCompleted":
                lot.update(step="DONE", tool=event["tool_id"], **{"yield": event["yield_pct"]})
                if lot["status"] != "HOLD":
                    lot["status"] = "PASS" if event["yield_pct"] >= 90 else "LOW"
            self.lots.move_to_end(event["lot_id"])
            while len(self.lots) > 9:
                self.lots.popitem(last=False)

    def tool_status(self, tool):
        if tool["down"]:
            return "DOWN"
        if tool["count"] == 0:
            return "NO DATA"
        if tool["alert"] and time.time() - tool["alert_at"] < ALERT_HOLD_S:
            return tool["alert"]
        return "UP"

    def rate(self):
        cutoff = time.time() - 5
        while self.rate_window and self.rate_window[0] < cutoff:
            self.rate_window.popleft()
        return len(self.rate_window) / 5


def render(state: DashboardState):
    header = Text.assemble(
        ("AIDP-FAB   ", "bold cyan"),
        (f"{state.rate():.1f} events/s   ", "green"),
        ("  ".join(f"{t.split('.', 1)[1]}={c}" for t, c in state.topic_counts.items()), "dim"),
    )

    tools = Table(title="Equipment health (SPC on live sensor stream)", title_style="bold")
    for col, width in (("Tool", 8), ("Status", 8), ("Worst parameter", 16), ("z", 5), ("Trend |z|", 16), ("#", 4)):
        tools.add_column(col, no_wrap=True, min_width=width, max_width=max(width, 16))
    for tool_id, tool in state.tools.items():
        status = state.tool_status(tool)
        param, z = tool["worst"] or ("-", 0.0)
        z_style = "red" if abs(z) > 3 else "yellow" if abs(z) > 2 else "green"
        tools.add_row(tool_id, Text(status, STATUS_STYLE[status]),
                      param, Text(f"{z:+.2f}", z_style), Text(sparkline(tool["trend"]), z_style), str(tool["count"]))

    lots = Table(title="Wafer lots (MES events)", expand=True, title_style="bold")
    for col in ("Lot", "Step", "Tool", "Yield", "Status"):
        lots.add_column(col, no_wrap=True)
    lot_style = {"RUN": "cyan", "PASS": "green", "LOW": "yellow", "HOLD": "bold red"}
    for lot_id, lot in reversed(state.lots.items()):
        y = f"{lot['yield']}%" if lot["yield"] is not None else "-"
        lots.add_row(lot_id, lot["step"], lot["tool"], y, Text(lot["status"], lot_style[lot["status"]]))

    alerts = Table(title="Alerts (fab.alerts)", expand=True, title_style="bold")
    for col in ("Time", "Severity", "From", "Message"):
        alerts.add_column(col, no_wrap=col != "Message")
    for a in state.alerts:
        sev_style = "bold red" if a["severity"] == "CRITICAL" else "yellow"
        alerts.add_row(hhmmss(a["event_time"]), Text(a["severity"], sev_style), a["source"], a["message"])

    orders = Table(title="Maintenance work orders", expand=True, title_style="bold")
    for col in ("WO", "Tool", "Status", "Technician", "Detail"):
        orders.add_column(col, no_wrap=col != "Detail")
    for wo_id, wo in reversed(state.orders.items()):
        detail = wo.get("action_taken") if wo["status"] == "DONE" else wo.get("reason", "")
        orders.add_row(wo_id, wo["tool_id"], Text(wo["status"], "magenta" if wo["status"] == "OPEN" else "green"),
                       wo.get("assigned_to", ""), detail or "")

    top = Table.grid(expand=True)
    top.add_column()            # equipment table at its natural width
    top.add_column(ratio=1)     # lots table takes the rest
    top.add_row(tools, lots)
    return Group(Panel(header), top, alerts, orders,
                 Text("Ctrl+C to exit  |  break a tool: python inject_fault.py ETCH-02", style="dim"))


def main():
    shutdown = Shutdown()
    # Unique group per run: the dashboard gets its own copy of the stream, starting "now".
    consumer = create_consumer(f"fab-dashboard-{uuid.uuid4().hex[:6]}", TOPICS, offset_reset="latest", verbose=False)
    state = DashboardState()
    with Live(render(state), refresh_per_second=4, screen=False) as live:
        while not shutdown.requested:
            for msg, event in drain(consumer, 1000):
                state.apply(msg.topic(), event)
            live.update(render(state))
            time.sleep(0.25)
    consumer.close()


if __name__ == "__main__":
    main()
