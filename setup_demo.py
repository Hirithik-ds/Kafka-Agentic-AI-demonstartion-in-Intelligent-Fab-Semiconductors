"""One-stop setup for the AIDP-FAB infrastructure in Docker.

    python setup_demo.py download        # once: JDBC connector + Flink Kafka connector (before docker compose up)
    python setup_demo.py init            # after "docker compose up -d": topics, PostgreSQL tables,
                                         #   PostgreSQL sink connector, Flink SQL jobs
    python setup_demo.py status          # what is running?
    python setup_demo.py flink-restart   # cancel and resubmit the Flink SQL jobs (after editing flink/aidp-fab.sql)
    python setup_demo.py connector-delete

Uses only the Python standard library plus confluent-kafka (already installed for the demo).
"""
import argparse
import io
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent

# ---- Kafka Connect + PostgreSQL
PLUGINS_DIR = HERE / "connect-plugins"
SINK_CONFIG = HERE / "connect" / "postgres-sink.json"
CONNECT_URL = "http://localhost:8083"
CONNECTOR_NAME = "aidp-fab-postgres-sink"
JDBC_VERSION = "10.9.9"   # 10.8.2 and older crash on Kafka 4 / Confluent Platform 8 (Utils.mkSet)
JDBC_DIR_NAME = f"confluentinc-kafka-connect-jdbc-{JDBC_VERSION}"
JDBC_URL = ("https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-jdbc/"
            f"versions/{JDBC_VERSION}/{JDBC_DIR_NAME}.zip")
JDBC_CLASS = "io.confluent.connect.jdbc.JdbcSinkConnector"

# ---- Flink
FLINK_LIB_DIR = HERE / "flink-lib"
FLINK_URL = "http://localhost:8081"
FLINK_KAFKA_JAR = "flink-sql-connector-kafka-3.4.0-1.20.jar"
FLINK_KAFKA_JAR_URL = ("https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/"
                       f"3.4.0-1.20/{FLINK_KAFKA_JAR}")
FLINK_JOBS = ["AIDP-FAB line yield (1 min, every 10 s)",
              "AIDP-FAB lots per tool (1 min, every 10 s)",
              "AIDP-FAB alerts per tool (1 min, every 10 s)"]


def ok(msg):
    print(f"  [ok]   {msg}", flush=True)


def fail(msg):
    print(f"  [FAIL] {msg}", flush=True)


# --------------------------------------------------------------------- helpers
def http(method, url, body=None, timeout=10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json", "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as err:
        raw = err.read()
        try:
            return err.code, json.loads(raw)
        except ValueError:
            return err.code, {"message": raw.decode(errors="replace")}


def wait_http(name, url, seconds=180, ready=lambda body: True):
    print(f"  ...    waiting for {name} at {url} ", end="", flush=True)
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            status, body = http("GET", url, timeout=3)
            if status == 200 and ready(body):
                print("ready")
                return True
        except (urllib.error.URLError, ConnectionError, OSError, ValueError):
            pass
        print(".", end="", flush=True)
        time.sleep(3)
    print("timed out")
    return False


def docker(*args, check=False):
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=check)


def download(url, what):
    print(f"  ...    downloading {what}")
    with urllib.request.urlopen(url, timeout=180) as resp:
        return resp.read()


# -------------------------------------------------------------------- download
def cmd_download(args):
    print("JDBC connector (Kafka Connect -> PostgreSQL)")
    import shutil
    for old in PLUGINS_DIR.glob("confluentinc-kafka-connect-jdbc-*"):
        if old.name != JDBC_DIR_NAME and old.is_dir():  # two versions on the plugin path would clash
            shutil.rmtree(old)
            ok(f"removed old version {old.name}")
    if (PLUGINS_DIR / JDBC_DIR_NAME).exists() and not args.force:
        ok(f"already in {PLUGINS_DIR.name}/{JDBC_DIR_NAME}")
    else:
        PLUGINS_DIR.mkdir(exist_ok=True)
        try:
            data = Path(args.jdbc_zip).read_bytes() if args.jdbc_zip else download(JDBC_URL, f"JDBC connector {JDBC_VERSION}")
            zipfile.ZipFile(io.BytesIO(data)).extractall(PLUGINS_DIR)
            ok(f"installed into {PLUGINS_DIR.name}/")
        except Exception as exc:  # noqa: BLE001
            fail(f"{exc}\n         Download it by hand: https://www.confluent.io/hub/confluentinc/kafka-connect-jdbc"
                 "\n         then run: python setup_demo.py download --jdbc-zip ~/Downloads/<zip file>")

    print("Flink Kafka SQL connector")
    target = FLINK_LIB_DIR / FLINK_KAFKA_JAR
    if target.exists() and not args.force:
        ok(f"already in {FLINK_LIB_DIR.name}/{FLINK_KAFKA_JAR}")
    else:
        FLINK_LIB_DIR.mkdir(exist_ok=True)
        try:
            target.write_bytes(download(FLINK_KAFKA_JAR_URL, FLINK_KAFKA_JAR))
            ok(f"saved {FLINK_LIB_DIR.name}/{FLINK_KAFKA_JAR}")
        except Exception as exc:  # noqa: BLE001
            fail(f"{exc}\n         Download it by hand: {FLINK_KAFKA_JAR_URL}\n         and put it in {FLINK_LIB_DIR}")
    print("\nIf the containers are already running, restart them so they see the new files:\n"
          "  docker compose restart connect flink-jobmanager flink-taskmanager")


# ------------------------------------------------------------------------ init
def step_topics():
    print("1. Kafka topics")
    try:
        from confluent_kafka.admin import AdminClient
        from common import BOOTSTRAP_SERVERS, TOPICS
        deadline = time.time() + 90
        while True:
            try:
                AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS}).list_topics(timeout=5)
                break
            except Exception:  # noqa: BLE001
                if time.time() > deadline:
                    raise
                time.sleep(3)
        subprocess.run([sys.executable, "create_topics.py"], cwd=HERE, check=True, stdout=subprocess.DEVNULL)
        ok(f"{len(TOPICS)} fab.* topics ready")
        return True
    except Exception as exc:  # noqa: BLE001
        fail(f"cannot reach Kafka on localhost:9092 ({exc}). Is it running?  ->  docker compose ps")
        return False


def step_postgres():
    print("2. PostgreSQL tables and views")
    for _ in range(40):
        if docker("exec", "postgres", "pg_isready", "-U", "fab", "-d", "fab").returncode == 0:
            break
        time.sleep(3)
    res = docker("exec", "postgres", "psql", "-U", "fab", "-d", "fab", "-v", "ON_ERROR_STOP=1", "-q",
                 "-f", "/docker-entrypoint-initdb.d/01-init.sql")
    if res.returncode == 0:
        ok("tables + v_* views in database 'fab'")
        return True
    fail(f"could not apply connect/postgres-init.sql: {(res.stderr or res.stdout).strip()}")
    return False


def connector_status():
    status, body = http("GET", f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status")
    if status == 404:
        return "NOT CREATED", None
    tasks = body.get("tasks", [])
    states = [body["connector"]["state"]] + [t["state"] for t in tasks]
    trace = next((t.get("trace") for t in tasks if t.get("trace")), None)
    return ("RUNNING" if all(s == "RUNNING" for s in states) and tasks else "/".join(states)), trace


def step_connector():
    print("3. Kafka Connect: PostgreSQL sink connector")
    if not wait_http("Kafka Connect", f"{CONNECT_URL}/"):
        fail("Kafka Connect not reachable  ->  docker compose logs connect")
        return False
    _, plugins = http("GET", f"{CONNECT_URL}/connector-plugins")
    if not any(p.get("class") == JDBC_CLASS for p in plugins or []):
        fail("JDBC connector plugin missing  ->  python setup_demo.py download  then  docker compose restart connect")
        return False
    status, body = http("PUT", f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/config", json.loads(SINK_CONFIG.read_text()))
    if status not in (200, 201):
        fail(f"Kafka Connect rejected the connector ({status}): {body.get('message', body)}")
        return False
    time.sleep(5)
    state, trace = connector_status()
    if state == "RUNNING":
        ok(f"'{CONNECTOR_NAME}' RUNNING")
        return True
    fail(f"'{CONNECTOR_NAME}' is {state}")
    if trace:
        print("\n".join("         " + line for line in trace.splitlines()[:10]))
    return False


def flink_jobs():
    status, body = http("GET", f"{FLINK_URL}/jobs/overview")
    return body.get("jobs", []) if status == 200 else []


def step_flink(restart=False):
    print("4. Flink SQL jobs")
    if not wait_http("Flink", f"{FLINK_URL}/overview", ready=lambda b: b.get("taskmanagers", 0) >= 1):
        fail("Flink not reachable  ->  docker compose logs flink-jobmanager")
        return False
    ours = [j for j in flink_jobs() if j["name"] in FLINK_JOBS and j["state"] in ("RUNNING", "CREATED", "INITIALIZING")]
    if ours and restart:
        for j in ours:
            http("PATCH", f"{FLINK_URL}/jobs/{j['jid']}?mode=cancel")
        ok(f"cancelled {len(ours)} running AIDP-FAB jobs")
        time.sleep(4)
        ours = []
    if len(ours) == len(FLINK_JOBS):
        ok("3 jobs already running (use 'flink-restart' to resubmit)")
        return True
    if ours:
        fail(f"only {len(ours)} of 3 jobs running  ->  python setup_demo.py flink-restart")
        return False
    if docker("exec", "flink-jobmanager", "sh", "-c", f"ls /opt/flink/lib/aidp/{FLINK_KAFKA_JAR}").returncode != 0:
        fail("Flink Kafka connector jar missing  ->  python setup_demo.py download  then  "
             "docker compose restart flink-jobmanager flink-taskmanager")
        return False
    print("  ...    submitting flink/aidp-fab.sql")
    res = docker("exec", "flink-jobmanager", "/opt/flink/bin/sql-client.sh", "-f", "/opt/flink/sql/aidp-fab.sql")
    output = (res.stdout or "") + (res.stderr or "")
    time.sleep(6)
    running = [j for j in flink_jobs() if j["name"] in FLINK_JOBS and j["state"] == "RUNNING"]
    if len(running) == len(FLINK_JOBS):
        ok("3 Flink SQL jobs RUNNING  ->  see them at http://localhost:8081")
        return True
    fail(f"{len(running)} of 3 jobs running. Flink SQL client output:")
    print("\n".join("         " + line for line in output.strip().splitlines()[-25:]))
    return False


def cmd_init(_args):
    results = {
        "Kafka topics": step_topics(),
        "PostgreSQL schema": step_postgres(),
        "PostgreSQL sink connector": step_connector(),
        "Flink SQL jobs": step_flink(),
    }
    print("\nSummary")
    for name, good in results.items():
        print(f"  {'[ok]  ' if good else '[FAIL]'} {name}")
    print("\nUIs:  dashboard http://localhost:8080 (after run_demo.py) | Kafbat http://localhost:8090 | "
          "Flink http://localhost:8081")
    if all(results.values()):
        print("\nAll set. Now run:  python run_demo.py --no-random-faults")
    else:
        sys.exit("\nSome steps failed (see above). Fix them and run  python setup_demo.py init  again; it is safe to repeat.")


def cmd_status(_args):
    print("Docker containers")
    res = docker("compose", "ps", "--format", "{{.Name}}\t{{.State}}\t{{.Status}}")
    print("\n".join("  " + line for line in (res.stdout or res.stderr).strip().splitlines()) or "  (none)")
    print("PostgreSQL sink connector")
    try:
        state, trace = connector_status()
        print(f"  {CONNECTOR_NAME}: {state}")
        if trace:
            print("\n".join("    " + line for line in trace.splitlines()[:8]))
    except (urllib.error.URLError, OSError):
        print("  Kafka Connect not reachable")
    print("Flink jobs")
    try:
        jobs = [j for j in flink_jobs() if j["name"] in FLINK_JOBS]
        for j in jobs:
            print(f"  {j['state']:<10} {j['name']}")
        if not jobs:
            print("  none submitted  ->  python setup_demo.py init")
    except (urllib.error.URLError, OSError):
        print("  Flink not reachable")


def cmd_flink_restart(_args):
    step_flink(restart=True)


def cmd_connector_delete(_args):
    status, _ = http("DELETE", f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}")
    print("deleted (data in PostgreSQL is kept)" if status in (200, 204) else f"nothing deleted ({status})")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    dl = sub.add_parser("download", help="download connector plugins")
    dl.add_argument("--jdbc-zip", help="use a JDBC connector ZIP you downloaded yourself")
    dl.add_argument("--force", action="store_true")
    dl.set_defaults(func=cmd_download)
    sub.add_parser("init", help="topics, PostgreSQL schema, sink connector, Flink jobs").set_defaults(func=cmd_init)
    sub.add_parser("status", help="show what is running").set_defaults(func=cmd_status)
    sub.add_parser("flink-restart", help="resubmit the Flink SQL jobs").set_defaults(func=cmd_flink_restart)
    sub.add_parser("connector-delete", help="remove the PostgreSQL sink").set_defaults(func=cmd_connector_delete)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
