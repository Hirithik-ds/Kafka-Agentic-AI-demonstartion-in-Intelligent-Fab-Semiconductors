"""Publish an InjectFault command so you can break a tool on cue during the demo.

Examples:
    python inject_fault.py ETCH-02
    python inject_fault.py CVD-01 --param susceptor_temp_c --direction up
"""
import argparse

from common import TOPIC_EQUIPMENT_COMMANDS, create_producer, new_event, publish
from equipment import TOOLS


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("tool_id", choices=sorted(TOOLS))
    parser.add_argument("--param", help="parameter to drift (default: random)")
    parser.add_argument("--direction", choices=["up", "down"], default="up")
    args = parser.parse_args()

    params = TOOLS[args.tool_id]["params"]
    if args.param and args.param not in params:
        parser.error(f"{args.tool_id} has parameters: {', '.join(params)}")

    producer = create_producer("demo-operator")
    event = new_event("InjectFault", "demo-operator", tool_id=args.tool_id, parameter=args.param,
                      direction=1 if args.direction == "up" else -1)
    publish(producer, TOPIC_EQUIPMENT_COMMANDS, args.tool_id, event)
    producer.flush(10)
    print(f"Sent InjectFault to {args.tool_id} ({args.param or 'random parameter'}, {args.direction}). "
          f"Watch the dashboard: WARNING -> CRITICAL -> work order -> tool DOWN -> repaired.")


if __name__ == "__main__":
    main()
