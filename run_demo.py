"""One-command demo launcher.

Creates the topics, starts every service in the background (logs go to ./logs/)
and opens the live dashboard in your browser. Ctrl+C stops everything.

    python run_demo.py                      # browser dashboard on http://localhost:8080
    python run_demo.py --no-random-faults   # tools only break when you click "Inject fault"
    python run_demo.py --terminal           # terminal dashboard instead of the browser

For the AI incident reports, configure one of these before starting (otherwise a built-in template is used):
    source ai.env                           # IBM watsonx.ai (copy ai.env.example to ai.env and fill it in)
    export ANTHROPIC_API_KEY=...            # Claude
    export OLLAMA_MODEL=llama3.2            # a local Ollama model
"""
import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from services import ServiceManager

HERE = Path(__file__).resolve().parent
# Consumers first, the equipment simulator (the main producer) last
START_ORDER = ["spc-monitor", "yield-monitor", "maintenance-service", "ai-rca-assistant",
               "lot-tracker", "equipment-simulator"]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--terminal", action="store_true", help="use the terminal dashboard instead of the browser")
    parser.add_argument("--no-random-faults", action="store_true", help="only break tools on demand")
    parser.add_argument("--port", type=int, default=8080, help="browser dashboard port (default 8080)")
    parser.add_argument("--no-browser", action="store_true", help="don't open a browser tab automatically")
    args = parser.parse_args()

    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    subprocess.run([sys.executable, "create_topics.py"], cwd=HERE, env=env, check=True)

    extra = {"equipment-simulator": ["--no-random-faults"]} if args.no_random_faults else {}
    manager = ServiceManager(extra_args=extra)
    try:
        manager.start_all(START_ORDER)
        if args.no_random_faults:
            print("\nRandom faults are OFF: break tools with the 'Inject fault' buttons.")
        from rca_assistant import provider_name
        print(f"AI incident reports written by: {provider_name()}")
        print("\nOpening dashboard in 3 seconds...")
        time.sleep(3)
        if args.terminal:
            subprocess.run([sys.executable, "dashboard.py"], cwd=HERE, env=env)
        else:
            import web_dashboard
            web_dashboard.serve(args.port, manager=manager, open_browser=not args.no_browser)
    except KeyboardInterrupt:
        pass
    finally:
        print("\nStopping services...")
        manager.stop_all()
        print("All stopped. Logs are in ./logs/")


if __name__ == "__main__":
    main()
