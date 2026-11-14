"""Starts, stops and tracks the demo's Python services (used by run_demo.py and the web dashboard).

Each service is a separate OS process with its own Kafka consumer group, exactly like
microservices in production. Stopping one from the dashboard shows what Kafka does when a
consumer goes away: its events wait in the topic (consumer lag grows) until it comes back.
"""
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
LOGS = HERE / "logs"

# name -> how to run it, which consumer group it uses, and what it does (shown on the dashboard)
SERVICES = {
    "spc-monitor": {
        "script": "spc_monitor.py", "group": "spc-monitor",
        "reads": "sensor-readings", "writes": "alerts",
    },
    "yield-monitor": {
        "script": "yield_monitor.py", "group": "yield-monitor",
        "reads": "lot.events + alerts", "writes": "alerts",
    },
    "maintenance-service": {
        "script": "maintenance_service.py", "group": "maintenance-service",
        "reads": "alerts", "writes": "work-orders",
    },
    "ai-rca-assistant": {
        "script": "rca_assistant.py", "group": "ai-rca-assistant",
        "reads": "lot.events + alerts + work-orders", "writes": "ai.incident-reports",
    },
    "lot-tracker": {
        "script": "lot_tracker.py", "group": "lot-tracker",
        "reads": "alerts + work-orders", "writes": "lot.events",
    },
    "equipment-simulator": {
        "script": "equipment_simulator.py", "group": "equipment-simulator",
        "reads": "commands + work-orders", "writes": "sensor-readings",
    },
}

# Consumers that are not Python processes we manage, but whose lag is worth showing
EXTERNAL_GROUPS = {
    "postgres-sink (Kafka Connect)": "connect-aidp-fab-postgres-sink",
}


class ServiceManager:
    def __init__(self, extra_args=None, env=None):
        self.extra_args = extra_args or {}
        self.env = {**os.environ, "PYTHONUNBUFFERED": "1", **(env or {})}
        self.procs = {}
        self.logs = {}
        self.started_at = {}
        self.lock = threading.Lock()
        LOGS.mkdir(exist_ok=True)

    def start(self, name):
        with self.lock:
            proc = self.procs.get(name)
            if proc and proc.poll() is None:
                return False
            spec = SERVICES[name]
            log = open(LOGS / f"{spec['script'][:-3]}.log", "a", encoding="utf-8")
            log.write(f"\n===== started {time.strftime('%H:%M:%S')} =====\n")
            log.flush()
            cmd = [sys.executable, spec["script"], *self.extra_args.get(name, [])]
            self.procs[name] = subprocess.Popen(cmd, cwd=HERE, env=self.env, stdout=log, stderr=subprocess.STDOUT)
            self.logs[name] = log
            self.started_at[name] = time.time()
            return True

    def stop(self, name, timeout=10):
        with self.lock:
            proc = self.procs.get(name)
            if not proc or proc.poll() is not None:
                return False
            proc.terminate()  # SIGTERM -> the service flushes and leaves its consumer group cleanly
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
        with self.lock:
            log = self.logs.pop(name, None)
            if log:
                log.close()
        return True

    def is_running(self, name):
        proc = self.procs.get(name)
        return bool(proc and proc.poll() is None)

    def status(self):
        now = time.time()
        return {
            name: {
                "running": self.is_running(name),
                "pid": self.procs[name].pid if self.is_running(name) else None,
                "uptime_s": int(now - self.started_at[name]) if self.is_running(name) else None,
            }
            for name in SERVICES
        }

    def start_all(self, order):
        for name in order:
            self.start(name)
            print(f"started {name:<22} pid={self.procs[name].pid}  log=logs/{SERVICES[name]['script'][:-3]}.log",
                  flush=True)

    def stop_all(self):
        for name in list(self.procs):
            self.stop(name)
