"""AIDP-FAB MCP server: 5 tools for a watsonx Orchestrate agent (Developer Edition on this Mac).

Dashboard API (run_demo.py):  get_fab_status, inject_fault
PostgreSQL (Docker, port 5433): get_held_lots, get_work_orders, get_incident_reports
"""
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal, Optional

import psycopg
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from psycopg.rows import dict_row

# Orchestrate runs this server in its own containers; host.docker.internal = your Mac.
DASHBOARD_URL = "http://192.168.1.112:8085"
DB = dict(host="192.168.1.112", port=5433, dbname="fab", user="fab", password="fab", connect_timeout=5)
TOOL_IDS = ["LITHO-01", "ETCH-01", "ETCH-02", "CVD-01", "CVD-02", "CMP-01"]

mcp = FastMCP(
    "AIDP-FAB",
    instructions=("Tools for the AIDP-FAB semiconductor fab demo. Process tools: LITHO-01, ETCH-01, ETCH-02, "
                  "CVD-01, CVD-02, CMP-01. Lots go LITHO > ETCH > CVD > CMP > METROLOGY; "
                  "a lot is put on HOLD when its yield is below 90%."),
)


def _clean(value):
    if isinstance(value, datetime):
        local = (value if value.tzinfo else value.replace(tzinfo=timezone.utc)).astimezone()
        return local.strftime("%Y-%m-%d %H:%M:%S %z")
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _clean(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_clean(v) for v in value]
    return value


def _query(sql, params=None):
    try:
        with psycopg.connect(**DB, row_factory=dict_row) as conn:
            conn.read_only = True
            return [_clean(r) for r in conn.execute(sql, params or {}).fetchall()]
    except psycopg.OperationalError as exc:
        raise ToolError("Cannot reach the fab database. Is the postgres container running? (docker compose ps)") from exc


def _dashboard(path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(DASHBOARD_URL + path, data=data, method="POST" if data else "GET",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            return json.loads(resp.read())
    except (urllib.error.URLError, OSError) as exc:
        raise ToolError("Cannot reach the live dashboard. Is the demo running? (python run_demo.py)") from exc


def _tool(tool_id):
    if tool_id is None:
        return None
    tool = tool_id.strip().upper()
    if tool not in TOOL_IDS:
        raise ToolError(f"Unknown tool '{tool_id}'. Valid tools: {', '.join(TOOL_IDS)}")
    return tool


@mcp.tool
def get_fab_status() -> dict:
    """Live status of the fab right now: each process tool's status (UP, WARNING drifting, CRITICAL out of
    control limits, DOWN under maintenance), KPIs (events per second, tools available, open work orders,
    lots on hold, line yield), open work orders, the 5 latest alerts, Kafka consumer lag per service and the
    latest Flink line yield. Use first for "how is the fab doing" or "is anything wrong"."""
    s = _dashboard("/api/snapshot")
    flink = (s.get("flink") or {}).get("line_yield") or []
    return {
        "kpis": s["kpi"],
        "events_per_second": s["rate"],
        "tools": [{"tool_id": t["id"], "type": t["type"], "status": t["status"],
                   "worst_parameter": t["worst_param"], "worst_z_score": t["worst_z"], "alert": t.get("alert_msg")}
                  for t in s["tools"]],
        "open_work_orders": [{"work_order_id": o["id"], "tool_id": o["tool_id"], "technician": o.get("assigned_to"),
                              "reason": o.get("detail")} for o in s["orders"] if o["status"] == "OPEN"],
        "lots_in_progress": sum(1 for lot in s["lots"] if lot["status"] == "RUN"),
        "latest_alerts": [{"time": _clean(datetime.fromisoformat(a["time"])), "severity": a["severity"],
                           "source": a["source"], "message": a["message"]} for a in s["alerts"][:5]],
        "consumer_lag": {sv["name"]: {"running": sv["running"], "lag": sv["lag"]} for sv in s.get("services", [])},
        "flink_line_yield_last_minute": (
            {"avg_yield_pct": flink[-1]["avg_yield"], "lots": flink[-1]["lots"], "lots_failed": flink[-1]["lots_failed"]}
            if flink else None),
    }


@mcp.tool
def inject_fault(tool_id: str, parameter: Optional[str] = None) -> dict:
    """DEMO ACTION that changes the system: makes one simulated process tool start drifting, which triggers the
    event chain (SPC warning ~20 s, critical ~35 s, maintenance work order, lot holds, AI incident report).
    Only use when the user explicitly asks to inject or simulate a fault, and confirm the tool first.
    tool_id: LITHO-01, ETCH-01, ETCH-02, CVD-01, CVD-02 or CMP-01. parameter: optional sensor name such as
    chamber_temp_c; random if omitted."""
    result = _dashboard("/api/inject", {"tool_id": _tool(tool_id), "param": parameter})
    return {"ok": result.get("ok", False), "tool_id": result.get("tool_id"), "parameter": result.get("param"),
            "what_happens_next": "SPC WARNING in ~20 s, CRITICAL in ~35 s, then a work order; affected lots go on "
                                 "hold and an AI incident report follows about a minute later."}


@mcp.tool
def get_held_lots(minutes: int = 120, limit: int = 10) -> dict:
    """Wafer lots put on HOLD (yield below 90%) in the last `minutes`, newest first: lot id, inspection time,
    yield, wafers passed out of 25, and the suspect tool and reason from the yield monitor."""
    rows = _query(
        "SELECT y.lot_id, y.inspected_at, y.yield_pct, y.wafers_passed, a.tool_id AS suspect_tool, "
        "a.message AS hold_reason FROM v_lot_yield y LEFT JOIN LATERAL ("
        "  SELECT tool_id, message FROM v_alerts WHERE lot_id = y.lot_id AND alert_type = 'YIELD_EXCURSION' "
        "  ORDER BY event_time DESC LIMIT 1) a ON true "
        "WHERE y.yield_pct < 90 AND y.inspected_at > now() - make_interval(mins => %(m)s) "
        "ORDER BY y.inspected_at DESC LIMIT %(l)s",
        {"m": max(1, minutes), "l": max(1, min(limit, 50))})
    return {"minutes": minutes, "count": len(rows), "held_lots": rows}


@mcp.tool
def get_work_orders(status: Optional[Literal["OPEN", "DONE"]] = None, tool_id: Optional[str] = None,
                    limit: int = 10) -> dict:
    """Maintenance work orders, newest first. They are opened automatically on a CRITICAL SPC alert. Each has the
    tool, technician, status (OPEN or DONE), open and completion times, repair duration in seconds, reason and
    repair action. Filter by status and/or tool_id."""
    where, params = ["true"], {"l": max(1, min(limit, 50))}
    if status:
        where.append("status = %(s)s")
        params["s"] = status.upper()
    if tool_id:
        where.append("tool_id = %(t)s")
        params["t"] = _tool(tool_id)
    rows = _query(
        "SELECT work_order_id, tool_id, status, technician, opened_at, completed_at, "
        "round(extract(epoch FROM completed_at - opened_at)) AS repair_seconds, reason, action_taken "
        f"FROM v_work_orders WHERE {' AND '.join(where)} ORDER BY opened_at DESC LIMIT %(l)s", params)
    return {"count": len(rows), "work_orders": rows}


@mcp.tool
def get_incident_reports(tool_id: Optional[str] = None, limit: int = 3) -> dict:
    """Incident reports written by the AI root-cause assistant after lots were put on hold, newest first:
    suspect tool, lots held, average yield, work order and the report (summary, timeline, root cause, impact,
    actions taken, recommendations). Use for "why did yield drop" or "what was the root cause"."""
    where, params = "true", {"l": max(1, min(limit, 10))}
    if tool_id:
        where, params["t"] = "suspect_tool = %(t)s", _tool(tool_id)
    rows = _query(
        "SELECT created_at, incident_id, suspect_tool, lots_held, avg_yield_pct, work_order_id, generated_by, report "
        f"FROM v_ai_incident_reports WHERE {where} ORDER BY created_at DESC LIMIT %(l)s", params)
    return {"count": len(rows), "incident_reports": rows}


if __name__ == "__main__":
    mcp.run(show_banner=False)  # stdio: watsonx Orchestrate starts this with "python server.py"