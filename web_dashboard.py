"""CONSUMER: browser dashboard for presenting the demo.

Reads every fab topic from Kafka (its own consumer group, like any other service)
and streams a live snapshot to a web page using Server-Sent Events. It also shows:
  * the consumer lag of every service (read from Kafka's committed offsets), and
  * Stop / Start buttons for each service, when started through run_demo.py.
The page's "Inject fault" buttons publish InjectFault commands to Kafka.

    python run_demo.py                   # normal way: starts services + this dashboard
    python web_dashboard.py              # dashboard only (no Stop/Start buttons)

Only uses the Python standard library plus confluent-kafka.
"""
import argparse
import json
import random
import re
import threading
import time
import uuid
import webbrowser
from collections import Counter, OrderedDict, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from common import (BOOTSTRAP_SERVERS, TOPIC_AI_REPORTS, TOPIC_ALERTS, TOPIC_EQUIPMENT_COMMANDS,
                    TOPIC_FLINK_ALERT_COUNTS, TOPIC_FLINK_LINE_YIELD, TOPIC_FLINK_TOOL_THROUGHPUT,
                    TOPIC_LOT_EVENTS, TOPIC_SENSOR_READINGS, TOPIC_WORK_ORDERS, create_consumer,
                    create_producer, drain, new_event, poll_event, publish)
from equipment import ROUTE, TOOLS, z_score
from services import EXTERNAL_GROUPS, SERVICES

INDEX_HTML = Path(__file__).resolve().parent / "web" / "index.html"
TOPICS = [TOPIC_SENSOR_READINGS, TOPIC_EQUIPMENT_COMMANDS, TOPIC_LOT_EVENTS, TOPIC_ALERTS, TOPIC_WORK_ORDERS,
          TOPIC_AI_REPORTS, TOPIC_FLINK_LINE_YIELD, TOPIC_FLINK_TOOL_THROUGHPUT, TOPIC_FLINK_ALERT_COUNTS]
HISTORY = 120        # readings kept per parameter for the control chart
ALERT_HOLD_S = 15    # how long a tool stays WARNING/CRITICAL after its last alert
MAX_LOTS = 14
FLINK_STALE_S = 25   # a Flink window older than this (by arrival) counts as "nothing in the last minute"


class FabState:
    """In-memory read model built purely from Kafka events."""

    def __init__(self):
        self.lock = threading.Lock()
        self.tools = {
            tool_id: {
                "count": 0, "down": False, "alert": None, "alert_at": 0.0, "alert_msg": None,
                "worst": (None, 0.0), "trend": deque(maxlen=40), "last": {},
                "z": {p: deque(maxlen=HISTORY) for p in spec["params"]},
            }
            for tool_id, spec in TOOLS.items()
        }
        self.lots = OrderedDict()
        self.alerts = deque(maxlen=40)
        self.orders = OrderedDict()
        self.topic_counts = Counter({t: 0 for t in TOPICS})
        self.rate_window = deque()
        self.yields = deque(maxlen=10)
        self.holds = 0
        self.lots_completed = 0
        self.connected = False
        self.ai_reports = deque(maxlen=8)
        self.flink_yield = OrderedDict()        # window_end -> row
        self.flink_tools = OrderedDict()        # window_end -> {"at": arrival, "rows": {tool: lots}}
        self.flink_alerts = OrderedDict()       # window_end -> {"at": arrival, "rows": {tool: alerts}}

    # ------------------------------------------------------------------ apply
    def apply(self, msg, event):
        with self.lock:
            self._apply(msg.topic(), msg.partition(), msg.offset(), event)

    @staticmethod
    def _windowed(store, e, key, value, keep=10):
        w = store.setdefault(e["window_end"], {"at": time.time(), "rows": Counter()})
        w["rows"][key] += value
        while len(store) > keep:
            store.popitem(last=False)

    def _apply(self, topic, partition, offset, e):
        now = time.time()
        self.topic_counts[topic] += 1
        self.rate_window.append(now)
        etype = e.get("event_type")
        kafka = {"topic": topic, "partition": partition, "offset": offset}

        # ---- Flink SQL results (plain rows, no event envelope)
        if topic == TOPIC_FLINK_LINE_YIELD:
            self.flink_yield[e["window_end"]] = {**e, "at": now}
            while len(self.flink_yield) > 36:
                self.flink_yield.popitem(last=False)
            return
        if topic == TOPIC_FLINK_TOOL_THROUGHPUT:
            self._windowed(self.flink_tools, e, e["tool_id"], e["lots"])
            return
        if topic == TOPIC_FLINK_ALERT_COUNTS:
            self._windowed(self.flink_alerts, e, e["tool_id"], e["alerts"])
            return

        if etype == "IncidentReport":
            self.ai_reports.appendleft({k: e.get(k) for k in (
                "event_id", "event_time", "incident_id", "suspect_tool", "step", "parameters", "lots_held",
                "avg_yield_pct", "wafers_lost", "work_order_id", "generated_by", "generation_seconds", "report")})

        elif etype == "SensorReading" and e.get("tool_id") in self.tools:
            tool = self.tools[e["tool_id"]]
            zs = {p: z_score(e["tool_id"], p, v) for p, v in e["parameters"].items()}
            for p, z in zs.items():
                tool["z"][p].append(round(z, 3))
            worst = max(zs, key=lambda p: abs(zs[p]))
            tool["worst"] = (worst, zs[worst])
            tool["trend"].append(round(abs(zs[worst]), 2))
            tool["last"] = e["parameters"]
            tool["count"] += 1

        elif etype == "InjectFault":
            self.alerts.appendleft({
                "id": e["event_id"], "time": e["event_time"], "severity": "INFO", "source": e["source"],
                "alert_type": "COMMAND", "tool_id": e.get("tool_id"),
                "message": f"InjectFault sent to {e.get('tool_id')} ({e.get('parameter') or 'random parameter'})",
                **kafka,
            })

        elif etype == "AlertRaised":
            self.alerts.appendleft({
                "id": e["event_id"], "time": e["event_time"], "severity": e["severity"], "source": e["source"],
                "alert_type": e.get("alert_type"), "tool_id": e.get("tool_id"), "message": e["message"], **kafka,
            })
            if e.get("alert_type") == "SPC_VIOLATION" and e.get("tool_id") in self.tools:
                tool = self.tools[e["tool_id"]]
                if tool["alert"] != "CRITICAL" or now - tool["alert_at"] > ALERT_HOLD_S:
                    tool["alert"] = e["severity"]
                    tool["alert_msg"] = e["message"]
                tool["alert_at"] = now
            if e.get("action") == "LOT_HOLD":
                self.holds += 1
                if e.get("lot_id") in self.lots:
                    self.lots[e["lot_id"]]["status"] = "HOLD"

        elif etype in ("WorkOrderOpened", "WorkOrderCompleted"):
            opened = etype == "WorkOrderOpened"
            wo = self.orders.get(e["work_order_id"], {})
            wo.update({
                "id": e["work_order_id"], "tool_id": e["tool_id"], "status": "OPEN" if opened else "DONE",
                "assigned_to": e.get("assigned_to") or e.get("completed_by") or wo.get("assigned_to"),
                "detail": e.get("reason") if opened else e.get("action_taken"), "time": e["event_time"],
            })
            self.orders[e["work_order_id"]] = wo
            self.orders.move_to_end(e["work_order_id"])
            while len(self.orders) > 8:
                self.orders.popitem(last=False)
            if e["tool_id"] in self.tools:
                self.tools[e["tool_id"]]["down"] = opened
                if not opened:
                    self.tools[e["tool_id"]]["alert"] = None

        elif topic == TOPIC_LOT_EVENTS and e.get("lot_id"):
            lot = self.lots.setdefault(e["lot_id"], {"lot_id": e["lot_id"], "step": "QUEUED", "step_index": -1,
                                                     "tool": "-", "yield": None, "status": "RUN"})
            if etype == "StepStarted":
                lot.update(step=e["step"], step_index=ROUTE.index(e["step"]), tool=e["tool_id"])
            elif etype == "InspectionCompleted":
                lot.update(step="DONE", step_index=len(ROUTE), tool=e["tool_id"])
                lot["yield"] = e["yield_pct"]
                self.yields.append(e["yield_pct"])
                self.lots_completed += 1
                if lot["status"] != "HOLD":
                    lot["status"] = "PASS" if e["yield_pct"] >= 90 else "LOW"
            self.lots.move_to_end(e["lot_id"])
            while len(self.lots) > MAX_LOTS:
                self.lots.popitem(last=False)

    # --------------------------------------------------------------- snapshot
    def _tool_status(self, tool, now):
        if tool["down"]:
            return "DOWN"
        if tool["count"] == 0:
            return "NO DATA"
        if tool["alert"] and now - tool["alert_at"] < ALERT_HOLD_S:
            return tool["alert"]
        return "UP"

    def _flink_snapshot(self, now):
        def latest(store):
            if not store:
                return None
            window_end, w = next(reversed(store.items()))
            fresh = now - w["at"] < FLINK_STALE_S
            return {"window_end": window_end, "fresh": fresh, "rows": dict(w["rows"]) if fresh else {}}
        return {
            "line_yield": [{k: v for k, v in r.items() if k != "at"} for r in self.flink_yield.values()],
            "throughput": latest(self.flink_tools),
            "alerts": latest(self.flink_alerts),
            "any": bool(self.flink_yield or self.flink_tools or self.flink_alerts),
        }

    def snapshot(self):
        now = time.time()
        with self.lock:
            while self.rate_window and self.rate_window[0] < now - 5:
                self.rate_window.popleft()
            tools = []
            for tool_id, t in self.tools.items():
                spec = TOOLS[tool_id]
                status = self._tool_status(t, now)
                tools.append({
                    "id": tool_id, "type": spec["tool_type"], "step": spec["step"], "recipe": spec["recipe"],
                    "status": status, "count": t["count"],
                    "worst_param": t["worst"][0], "worst_z": round(t["worst"][1], 2),
                    "trend": list(t["trend"]),
                    "alert_msg": t["alert_msg"] if status in ("WARNING", "CRITICAL") else None,
                    "params": [{"name": p, "unit": spec["params"][p]["unit"], "target": spec["params"][p]["target"],
                                "last": t["last"].get(p), "z": list(t["z"][p])} for p in spec["params"]],
                })
            return {
                "bootstrap": BOOTSTRAP_SERVERS,
                "connected": self.connected,
                "rate": round(len(self.rate_window) / 5, 1),
                "topic_counts": dict(self.topic_counts),
                "route": ROUTE,
                "kpi": {
                    "tools_available": sum(1 for t in tools if t["status"] != "DOWN"),
                    "tools_total": len(tools),
                    "open_orders": sum(1 for o in self.orders.values() if o["status"] == "OPEN"),
                    "lots_on_hold": self.holds,
                    "lots_completed": self.lots_completed,
                    "line_yield": round(sum(self.yields) / len(self.yields), 1) if self.yields else None,
                },
                "tools": tools,
                "lots": list(reversed(self.lots.values())),
                "alerts": list(self.alerts),
                "orders": list(reversed(self.orders.values())),
                "ai_reports": list(self.ai_reports),
                "flink": self._flink_snapshot(now),
            }


# --------------------------------------------------------------- consumer lag
class LagMonitor:
    """Every 2 s: committed offset of each consumer group vs. the newest offset in each partition."""

    def __init__(self, stop: threading.Event):
        self.stop = stop
        self.groups = {name: spec["group"] for name, spec in SERVICES.items()}
        self.groups.update(EXTERNAL_GROUPS)
        self.lag = {name: None for name in self.groups}
        self.history = {name: deque(maxlen=60) for name in self.groups}
        self.lock = threading.Lock()

    def run(self):
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
        while not self.stop.is_set():
            for name, group in self.groups.items():
                lag = self._group_lag(admin, group)
                with self.lock:
                    self.lag[name] = lag
                    self.history[name].append(lag if lag is not None else 0)
            self.stop.wait(2)

    @staticmethod
    def _group_lag(admin, group):
        try:
            result = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)])[group].result(timeout=5)
            committed = [tp for tp in result.topic_partitions if tp.offset >= 0]
            if not committed:
                return None
            latest = admin.list_offsets({TopicPartition(tp.topic, tp.partition): OffsetSpec.latest()
                                         for tp in committed})
            high = {(tp.topic, tp.partition): f.result(timeout=5).offset for tp, f in latest.items()}
            return sum(max(0, high[(tp.topic, tp.partition)] - tp.offset) for tp in committed)
        except Exception:  # noqa: BLE001 - group not created yet, broker restarting, ...
            return None

    def snapshot(self):
        with self.lock:
            return {name: {"lag": self.lag[name], "history": list(self.history[name])} for name in self.groups}


# ---------------------------------------------------------------------- Kafka
def consume_forever(state: FabState, stop: threading.Event):
    # Own consumer group per dashboard -> it gets a full copy of every topic (fan-out).
    consumer = create_consumer(f"web-dashboard-{uuid.uuid4().hex[:6]}", TOPICS,
                               offset_reset="latest", verbose=False)
    try:
        while not stop.is_set():
            msg, event = poll_event(consumer, 0.2)
            batch = [(msg, event)] if msg is not None else []
            batch += list(drain(consumer, 1000)) if msg is not None else []
            for m, ev in batch:
                try:
                    state.apply(m, ev)
                except Exception as exc:  # noqa: BLE001 - one odd event must not kill the dashboard
                    print(f"[web-dashboard] skipped event at {m.topic()}[{m.partition()}]@{m.offset()}: {exc!r}", flush=True)
            state.connected = bool(consumer.assignment())
    finally:
        consumer.close()


# ----------------------------------------------------------------------- HTTP
class Handler(BaseHTTPRequestHandler):
    state: FabState = None
    lag: LagMonitor = None
    manager = None
    producer = None
    producer_lock = threading.Lock()
    stop: threading.Event = None

    def log_message(self, *_):  # keep the terminal quiet
        pass

    def _json(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _services(self):
        lag = self.lag.snapshot()
        status = self.manager.status() if self.manager else {}
        rows = []
        for name, spec in SERVICES.items():
            rows.append({"name": name, "group": spec["group"], "reads": spec["reads"], "writes": spec["writes"],
                         "managed": self.manager is not None,
                         "running": status.get(name, {}).get("running"), **lag[name]})
        for name, group in EXTERNAL_GROUPS.items():
            rows.append({"name": name, "group": group, "reads": "6 topics", "writes": "PostgreSQL",
                         "managed": False, "running": None, **lag[name]})
        return rows

    def _full_snapshot(self):
        snap = self.state.snapshot()
        snap["services"] = self._services()
        snap["managed"] = self.manager is not None
        return snap

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            body = INDEX_HTML.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/api/snapshot":
            self._json(200, self._full_snapshot())
        elif self.path == "/events":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            try:
                while not self.stop.is_set():
                    data = json.dumps(self._full_snapshot())
                    self.wfile.write(f"data: {data}\n\n".encode())
                    self.wfile.flush()
                    time.sleep(0.5)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                pass  # browser tab closed
        else:
            self.send_error(404)

    def _body(self):
        try:
            return json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        except ValueError:
            return {}

    def do_POST(self):
        service_action = re.fullmatch(r"/api/services/([\w-]+)/(start|stop)", self.path)
        if service_action:
            name, action = service_action.groups()
            if self.manager is None:
                self._json(409, {"ok": False, "error": "Start the demo with run_demo.py to control services"})
            elif name not in SERVICES:
                self._json(404, {"ok": False, "error": f"unknown service {name}"})
            else:
                changed = self.manager.start(name) if action == "start" else self.manager.stop(name)
                self._json(200, {"ok": True, "name": name, "action": action, "changed": changed})
            return
        if self.path != "/api/inject":
            self.send_error(404)
            return
        body = self._body()
        tool_id, param = body.get("tool_id"), body.get("param")
        if tool_id not in TOOLS:
            self._json(400, {"ok": False, "error": f"unknown tool {tool_id}"})
            return
        params = list(TOOLS[tool_id]["params"])
        if param not in params:
            param = random.choice(params)
        event = new_event("InjectFault", "web-dashboard", tool_id=tool_id, parameter=param, direction=1)
        with self.producer_lock:
            publish(self.producer, TOPIC_EQUIPMENT_COMMANDS, tool_id, event)
            self.producer.flush(5)
        self._json(200, {"ok": True, "tool_id": tool_id, "param": param, "topic": TOPIC_EQUIPMENT_COMMANDS})


def serve(port=8080, manager=None, open_browser=True):
    """Run the dashboard until Ctrl+C. run_demo.py passes its ServiceManager to enable Stop/Start."""
    state, stop = FabState(), threading.Event()
    lag = LagMonitor(stop)
    Handler.state, Handler.stop, Handler.lag, Handler.manager = state, stop, lag, manager
    Handler.producer = create_producer("web-dashboard")
    threading.Thread(target=consume_forever, args=(state, stop), daemon=True).start()
    threading.Thread(target=lag.run, daemon=True).start()

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.daemon_threads = True
    url = f"http://localhost:{port}"
    print(f"Web dashboard on {url}  (reading Kafka at {BOOTSTRAP_SERVERS}). Ctrl+C to stop.", flush=True)
    if open_browser:
        threading.Timer(1.0, webbrowser.open, args=(url,)).start()
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.server_close()
        Handler.producer.flush(5)
        print("Web dashboard stopped.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--no-browser", action="store_true", help="don't open a browser tab automatically")
    args = parser.parse_args()
    serve(args.port, manager=None, open_browser=not args.no_browser)


if __name__ == "__main__":
    main()
