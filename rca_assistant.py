"""CONSUMER -> PRODUCER: AI root-cause assistant.

Listens to the same events as everyone else (lot events, alerts, work orders). When lots are put
on HOLD, it waits until the burst of holds is over, collects every related fact from the event
streams, and asks an LLM to write a short incident report. The report is published as an
IncidentReport event to fab.ai.incident-reports, where the dashboard (and PostgreSQL) pick it up.

Which "AI" writes the report (first one configured wins; AI_PROVIDER=watsonx|claude|ollama|template forces one):
  1. IBM watsonx.ai  if WATSONX_URL + WATSONX_PROJECT_ID + WATSONX_USERNAME/WATSONX_API_KEY are set
                     (model: WATSONX_MODEL, default openai/gpt-oss-120b). See ai.env.example.
  2. Claude          if ANTHROPIC_API_KEY is set        (model: AI_MODEL, default claude-haiku-5-5)
  3. Ollama (local)  if OLLAMA_MODEL is set, e.g. llama3.2  (server: OLLAMA_URL, default http://localhost:11434)
  4. Built-in template otherwise, so the demo always works offline.
"""
import json
import os
import random
import ssl
import sys
import time
import urllib.error
import urllib.request
from collections import OrderedDict, defaultdict, deque
from datetime import timedelta

from common import (TOPIC_AI_REPORTS, TOPIC_ALERTS, TOPIC_LOT_EVENTS, TOPIC_WORK_ORDERS, Shutdown,
                    age_seconds, create_consumer, create_producer, new_event, parse_iso, poll_event, publish)
from equipment import TOOLS, WAFERS_PER_LOT

HERE_DIR = os.path.dirname(os.path.abspath(__file__))
AI_ENV_FILES = ["ai.env", ".env", "ai.env.example"]


def load_ai_env():
    """Read AI settings from ai.env (or .env, or a filled-in ai.env.example) next to this file, so
    'source ai.env' is optional. Lines look like: export KEY="value". Variables already set in the
    terminal win; placeholder values such as "your-project-id" are ignored. Returns the files used."""
    used = []
    for name in AI_ENV_FILES:
        path = os.path.join(HERE_DIR, name)
        if not os.path.isfile(path):
            continue
        loaded = 0
        for line in open(path, encoding="utf-8-sig"):
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.removeprefix("export ").split("=", 1)
            key = key.strip()
            value = value.split(" #")[0].strip().strip('"').strip("'").strip()
            if key and value and not value.startswith("your-") and not os.environ.get(key):
                os.environ[key] = value
                loaded += 1
        if loaded:
            used.append(f"{name} ({loaded} settings)")
    return used


AI_ENV_LOADED = load_ai_env()

SOURCE = "ai-rca-assistant"
GROUP_ID = "ai-rca-assistant"
QUIET_S = 15          # write the report once no new hold for this tool arrived for this long
MAX_WAIT_S = 75       # ...or at the latest this long after the first hold
STALE_AFTER_S = 600   # ignore holds older than this when catching up

AI_MODEL = os.getenv("AI_MODEL", "claude-haiku-5-5")
ANTHROPIC_URL = os.getenv("ANTHROPIC_BASE_URL", "https://api.anthropic.com").rstrip("/") + "/v1/messages"
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434").rstrip("/") + "/api/chat"

# IBM watsonx.ai on Cloud Pak for Data
WATSONX_URL = os.getenv("WATSONX_URL", "").rstrip("/")            # e.g. https://cpd-cpd-instance.apps.example.com
WATSONX_PROJECT_ID = os.getenv("WATSONX_PROJECT_ID")
WATSONX_MODEL = os.getenv("WATSONX_MODEL", "openai/gpt-oss-120b")
WATSONX_VERSION = os.getenv("WATSONX_API_VERSION", "2023-05-29")
WATSONX_VERIFY_SSL = os.getenv("WATSONX_VERIFY_SSL", "true").lower() not in ("false", "0", "no")
TOKEN_TTL_S = 10 * 3600   # Cloud Pak for Data tokens last hours; refresh well before, or on HTTP 401

HEADINGS = ["SUMMARY", "TIMELINE", "ROOT CAUSE", "IMPACT", "ACTIONS TAKEN", "RECOMMENDATIONS"]
SYSTEM_PROMPT = (
    "You are a senior yield engineer in a semiconductor fab. You write short, factual incident reports "
    "for the shift team from the event data you are given. Use only the facts provided; never invent "
    "numbers, names or times. Write plain text (no markdown symbols like # or **). Use exactly these "
    "section headings, each on its own line in capitals: " + ", ".join(HEADINGS) + ". Under each heading "
    "write 1-3 short lines starting with '- '. Times are UTC, show them as HH:MM:SS. Keep the whole "
    "report under 170 words."
)


def hhmmss(iso):
    return parse_iso(iso).strftime("%H:%M:%S") if iso else "?"


# ------------------------------------------------------------------ AI providers
def _post_json(url, payload, headers, timeout=60, verify=True):
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), method="POST",
                                 headers={"content-type": "application/json", "accept": "application/json", **headers})
    context = None if verify else ssl._create_unverified_context()  # for clusters with self-signed certificates
    with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
        return json.loads(resp.read())


_watsonx_token = {"value": os.getenv("WATSONX_TOKEN"), "at": time.time() if os.getenv("WATSONX_TOKEN") else 0}


def watsonx_token(force=False):
    """Bearer token from username + API key (POST /icp4d-api/v1/authorize), cached and reused."""
    fresh = time.time() - _watsonx_token["at"] < TOKEN_TTL_S
    if _watsonx_token["value"] and fresh and not force:
        return _watsonx_token["value"]
    body = _post_json(f"{WATSONX_URL}/icp4d-api/v1/authorize",
                      {"username": os.environ["WATSONX_USERNAME"], "api_key": os.environ["WATSONX_API_KEY"]},
                      {}, timeout=30, verify=WATSONX_VERIFY_SSL)
    _watsonx_token.update(value=body["token"], at=time.time())
    print("watsonx: got a new bearer token", flush=True)
    return body["token"]


def ask_watsonx(prompt):
    payload = {
        "project_id": WATSONX_PROJECT_ID,
        "model_id": WATSONX_MODEL,
        "messages": [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": prompt}],
        "max_tokens": 2000,          # reasoning models like gpt-oss spend tokens thinking before answering
        "temperature": 0,
        "top_p": 1,
        "frequency_penalty": 0,
        "presence_penalty": 0,
    }
    url = f"{WATSONX_URL}/ml/v1/text/chat?version={WATSONX_VERSION}"
    for attempt in range(2):
        try:
            body = _post_json(url, payload, {"authorization": f"Bearer {watsonx_token(force=attempt > 0)}"},
                              timeout=120, verify=WATSONX_VERIFY_SSL)
            break
        except urllib.error.HTTPError as err:
            if err.code == 401 and attempt == 0 and os.getenv("WATSONX_API_KEY"):
                continue  # token expired: get a new one and retry once
            raise ValueError(f"watsonx HTTP {err.code}: {err.read()[:300].decode(errors='replace')}") from err
    text = (body["choices"][0]["message"].get("content") or "").strip()
    if not text:
        raise ValueError("watsonx returned an empty answer (try a larger max_tokens)")
    return text


def ask_claude(prompt):
    body = _post_json(ANTHROPIC_URL, {
        "model": AI_MODEL,
        "max_tokens": 600,
        "system": SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": prompt}],
    }, {"x-api-key": os.environ["ANTHROPIC_API_KEY"], "anthropic-version": "2023-06-01"})
    return "".join(block.get("text", "") for block in body["content"] if block.get("type") == "text").strip()


def ask_ollama(prompt):
    body = _post_json(OLLAMA_URL, {
        "model": OLLAMA_MODEL,
        "stream": False,
        "messages": [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": prompt}],
    }, {}, timeout=180)
    return body["message"]["content"].strip()


def watsonx_configured():
    return bool(WATSONX_URL and WATSONX_PROJECT_ID and
                (os.getenv("WATSONX_TOKEN") or (os.getenv("WATSONX_USERNAME") and os.getenv("WATSONX_API_KEY"))))


def provider_name():
    forced = os.getenv("AI_PROVIDER", "").lower()
    if forced == "watsonx" or (not forced and watsonx_configured()):
        return f"watsonx:{WATSONX_MODEL}"
    if forced == "template":
        return "template"
    if forced == "ollama":
        return f"ollama:{OLLAMA_MODEL}"
    if os.getenv("ANTHROPIC_API_KEY"):
        return f"claude:{AI_MODEL}"
    if OLLAMA_MODEL:
        return f"ollama:{OLLAMA_MODEL}"
    return "template"


def template_report(f):
    """Deterministic report from the same facts, used offline or if the AI call fails."""
    wo = f.get("work_order") or {}
    params = ", ".join(f["parameters"]) or "an unknown parameter"
    cmp_lines = [f"- {c['tool_id']}: {c['lots_failed']} of {c['lots_processed']} lots failed"
                 for c in f["same_step_comparison"]]
    timeline = []
    if f.get("first_warning"):
        timeline.append(f"- {f['first_warning']['time']} SPC warning: {f['first_warning']['message']}")
    if f.get("first_critical"):
        timeline.append(f"- {f['first_critical']['time']} SPC critical: {f['first_critical']['message']}")
    if wo.get("opened_at"):
        timeline.append(f"- {wo['opened_at']} work order {wo['id']} opened for {wo['technician']}")
    if wo.get("completed_at"):
        timeline.append(f"- {wo['completed_at']} repair completed: {wo['action_taken']}")
    timeline.append(f"- {f['holds_from']} to {f['holds_to']} {f['lots_held']} lots put on hold")
    if f["suspect_tool"] == "UNKNOWN":
        cause = "- No SPC alert matches these lots; engineering review needed."
    else:
        cause = (f"- {f['suspect_tool']} ({f['suspect_tool_type']}) drifted on {params}, max {f['max_abs_z']} sigma "
                 f"from target, while these lots were in the {f['step']} step.")
    lines = [
        "SUMMARY",
        f"- {f['lots_held']} lots put on hold after processing on {f['suspect_tool']}; "
        f"average yield {f['avg_yield_pct']}%.",
        "TIMELINE", *timeline,
        "ROOT CAUSE", cause, *cmp_lines,
        "IMPACT",
        f"- About {f['wafers_lost']} of {f['lots_held'] * WAFERS_PER_LOT} wafers failed inspection.",
        "ACTIONS TAKEN",
        (f"- {wo['technician']}: {wo['action_taken']}" if wo.get("action_taken")
         else ("- Repair in progress." if wo else "- No work order yet.")),
        "RECOMMENDATIONS",
        f"- Requalify {f['suspect_tool']} with a monitor wafer before full release.",
        "- Disposition held lots after engineering review.",
    ]
    return "\n".join(lines)


# --------------------------------------------------------------------- the service
class RcaAssistant:
    def __init__(self, producer):
        self.producer = producer
        self.lot_steps = OrderedDict()                      # lot -> [{step, tool, completed}]
        self.lot_yield = {}                                 # lot -> yield %
        self.spc_alerts = defaultdict(lambda: deque(maxlen=200))
        self.work_orders = OrderedDict()                    # wo id -> merged dict
        self.incidents = {}                                 # suspect tool -> collecting incident
        self.provider = provider_name()

    # ----- building context from the streams
    def on_event(self, e):
        etype = e.get("event_type")
        if etype == "StepCompleted":
            self.lot_steps.setdefault(e["lot_id"], []).append(
                {"step": e["step"], "tool": e["tool_id"], "completed": e["event_time"]})
            self.lot_steps.move_to_end(e["lot_id"])
            while len(self.lot_steps) > 400:
                old, _ = self.lot_steps.popitem(last=False)
                self.lot_yield.pop(old, None)
        elif etype == "InspectionCompleted":
            self.lot_yield[e["lot_id"]] = e["yield_pct"]
        elif etype == "AlertRaised" and e.get("alert_type") == "SPC_VIOLATION":
            self.spc_alerts[e["tool_id"]].append(e)
        elif etype in ("WorkOrderOpened", "WorkOrderCompleted"):
            wo = self.work_orders.setdefault(e["work_order_id"], {"id": e["work_order_id"], "tool_id": e["tool_id"]})
            if etype == "WorkOrderOpened":
                wo.update(technician=e.get("assigned_to"), opened=e["event_time"], reason=e.get("reason"))
            else:
                wo.update(completed=e["event_time"], action_taken=e.get("action_taken"),
                          technician=wo.get("technician") or e.get("completed_by"))
            while len(self.work_orders) > 100:
                self.work_orders.popitem(last=False)
        elif etype == "AlertRaised" and e.get("action") == "LOT_HOLD":
            if age_seconds(e) > STALE_AFTER_S:
                return
            tool = e.get("tool_id") or "UNKNOWN"
            inc = self.incidents.setdefault(tool, {"first": time.time(), "holds": []})
            inc["holds"].append(e)
            inc["last"] = time.time()
            print(f"collecting: {e['lot_id']} held, suspect {tool} ({len(inc['holds'])} lots so far)", flush=True)

    def tick(self):
        now = time.time()
        for tool, inc in list(self.incidents.items()):
            if now - inc["last"] >= QUIET_S or now - inc["first"] >= MAX_WAIT_S:
                del self.incidents[tool]
                self.write_report(tool, inc["holds"])

    # ----- turning events into facts
    def build_facts(self, tool, holds):
        spec = TOOLS.get(tool, {})
        step = spec.get("step")
        held_lots = [h["lot_id"] for h in holds]
        processed = [s["completed"] for lot in held_lots for s in self.lot_steps.get(lot, []) if s["tool"] == tool]
        times = sorted(parse_iso(t) for t in processed) or sorted(parse_iso(h["event_time"]) for h in holds)
        t0, t1 = times[0] - timedelta(seconds=120), times[-1] + timedelta(seconds=30)

        alerts = [a for a in self.spc_alerts.get(tool, []) if t0 <= parse_iso(a["event_time"]) <= t1]
        warn = next((a for a in alerts if a["severity"] == "WARNING"), None)
        crit = next((a for a in alerts if a["severity"] == "CRITICAL"), None)
        params = sorted({a["parameter"] for a in alerts} |
                        {p for h in holds for s in h.get("suspect_tools", []) if s["tool_id"] == tool
                         for p in s.get("parameters", [])})

        wo = next((w for w in reversed(self.work_orders.values())
                   if w["tool_id"] == tool and w.get("opened") and parse_iso(w["opened"]) >= t0), None)

        # Compare every tool of the same step over the same period: the "smoking gun" evidence
        comparison = defaultdict(lambda: {"processed": 0, "failed": 0})
        if step:
            for lot, steps in self.lot_steps.items():
                if lot not in self.lot_yield:
                    continue
                for s in steps:
                    if s["step"] == step and t0 <= parse_iso(s["completed"]) <= t1:
                        comparison[s["tool"]]["processed"] += 1
                        comparison[s["tool"]]["failed"] += self.lot_yield[lot] < 90

        yields = [h["yield_pct"] for h in holds]
        wafers_lost = sum(WAFERS_PER_LOT - h.get("wafers_passed", 0) for h in holds)
        return {
            "suspect_tool": tool,
            "suspect_tool_type": spec.get("tool_type", "?"),
            "step": step or "?",
            "parameters": params,
            "max_abs_z": max((abs(a.get("z_score", 0)) for a in alerts), default=0),
            "lots_held": len(holds),
            "held_lots": [{"lot_id": h["lot_id"], "yield_pct": h["yield_pct"]} for h in holds],
            "avg_yield_pct": round(sum(yields) / len(yields), 1),
            "wafers_lost": wafers_lost,
            "holds_from": hhmmss(min(h["event_time"] for h in holds)),
            "holds_to": hhmmss(max(h["event_time"] for h in holds)),
            "first_warning": {"time": hhmmss(warn["event_time"]), "message": warn["message"]} if warn else None,
            "first_critical": {"time": hhmmss(crit["event_time"]), "message": crit["message"]} if crit else None,
            "work_order": {
                "id": wo["id"], "technician": wo.get("technician"), "opened_at": hhmmss(wo.get("opened")),
                "completed_at": hhmmss(wo["completed"]) if wo.get("completed") else None,
                "action_taken": wo.get("action_taken"),
            } if wo else None,
            "same_step_comparison": [
                {"tool_id": t, "lots_processed": c["processed"], "lots_failed": c["failed"]}
                for t, c in sorted(comparison.items())
            ],
        }

    def write_report(self, tool, holds):
        facts = self.build_facts(tool, holds)
        prompt = ("Write the incident report for these lot holds. Event data collected from Kafka:\n"
                  + json.dumps(facts, indent=2))
        generated_by, error = self.provider, None
        started = time.time()
        try:
            if self.provider.startswith("watsonx:"):
                text = ask_watsonx(prompt)
            elif self.provider.startswith("claude:"):
                text = ask_claude(prompt)
            elif self.provider.startswith("ollama:"):
                text = ask_ollama(prompt)
            else:
                text = template_report(facts)
        except (urllib.error.URLError, KeyError, ValueError, TimeoutError, OSError) as exc:
            error = f"{type(exc).__name__}: {exc}"
            print(f"AI call failed ({error}); using the built-in template instead", flush=True)
            text, generated_by = template_report(facts), "template (AI call failed)"
        incident_id = f"INC-{random.randint(1000, 9999)}"
        event = new_event(
            "IncidentReport", SOURCE,
            incident_id=incident_id, suspect_tool=tool, step=facts["step"], parameters=facts["parameters"],
            lots_held=facts["lots_held"], avg_yield_pct=facts["avg_yield_pct"], wafers_lost=facts["wafers_lost"],
            work_order_id=(facts["work_order"] or {}).get("id"), generated_by=generated_by,
            generation_seconds=round(time.time() - started, 1), error=error, report=text, facts=facts,
        )
        publish(self.producer, TOPIC_AI_REPORTS, tool, event)
        self.producer.flush(5)
        print(f"\n=== {incident_id}  ({generated_by}, {event['generation_seconds']}s) ===\n{text}\n", flush=True)


SAMPLE_FACTS = {
    "suspect_tool": "ETCH-02", "suspect_tool_type": "PLASMA_ETCH", "step": "ETCH", "parameters": ["chamber_temp_c"],
    "max_abs_z": 3.4, "lots_held": 5, "held_lots": [{"lot_id": "LOT-101500", "yield_pct": 0.0}],
    "avg_yield_pct": 2.4, "wafers_lost": 122, "holds_from": "10:15:20", "holds_to": "10:15:58",
    "first_warning": {"time": "10:14:40", "message": "ETCH-02 chamber_temp_c drifting up: 4 of last 5 points beyond 1 sigma"},
    "first_critical": {"time": "10:14:55", "message": "ETCH-02 chamber_temp_c=62.7C outside control limits [57.6, 62.4]"},
    "work_order": {"id": "WO-12345", "technician": "Sushma", "opened_at": "10:14:56", "completed_at": "10:15:11",
                   "action_taken": "Replaced thermocouple and re-tuned heater PID loop"},
    "same_step_comparison": [{"tool_id": "ETCH-01", "lots_processed": 4, "lots_failed": 0},
                             {"tool_id": "ETCH-02", "lots_processed": 6, "lots_failed": 5}],
}


def self_test():
    """python rca_assistant.py --test : check the AI connection with sample facts, without Kafka."""
    provider = provider_name()
    print(f"Folder: {HERE_DIR}")
    print(f"Settings loaded from: {', '.join(AI_ENV_LOADED) or 'nothing (no ai.env / .env with values in this folder)'}")
    print(f"Provider: {provider}")
    if provider == "template":
        names = ["WATSONX_URL", "WATSONX_PROJECT_ID", "WATSONX_USERNAME", "WATSONX_API_KEY"]
        missing = [n for n in names if not os.getenv(n)]
        print(f"watsonx not used, missing: {', '.join(missing) or 'nothing (check AI_PROVIDER)'}")
        print("Fix: put ai.env in the folder above, with lines like  export WATSONX_URL=\"https://...\"")
    prompt = "Write the incident report for these lot holds. Event data collected from Kafka:\n" + json.dumps(SAMPLE_FACTS, indent=2)
    started = time.time()
    if provider.startswith("watsonx:"):
        text = ask_watsonx(prompt)
    elif provider.startswith("claude:"):
        text = ask_claude(prompt)
    elif provider.startswith("ollama:"):
        text = ask_ollama(prompt)
    else:
        text = template_report(SAMPLE_FACTS)
    print(f"--- report ({time.time() - started:.1f}s) ---\n{text}")


def main():
    if "--test" in sys.argv:
        self_test()
        return
    shutdown = Shutdown()
    consumer = create_consumer(GROUP_ID, [TOPIC_LOT_EVENTS, TOPIC_ALERTS, TOPIC_WORK_ORDERS], offset_reset="latest")
    producer = create_producer(SOURCE)
    assistant = RcaAssistant(producer)
    print(f"AI root-cause assistant running, reports written by: {assistant.provider}. Ctrl+C to stop.", flush=True)
    while not shutdown.requested:
        msg, event = poll_event(consumer, 0.5)
        if msg is not None:
            try:
                assistant.on_event(event)
            except (KeyError, ValueError, TypeError) as exc:
                print(f"skipped odd event at {msg.topic()}@{msg.offset()}: {exc!r}", flush=True)
        assistant.tick()
    producer.flush(5)
    consumer.close()


if __name__ == "__main__":
    main()