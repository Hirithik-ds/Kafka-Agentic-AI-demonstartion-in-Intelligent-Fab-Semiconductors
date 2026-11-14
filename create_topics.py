"""Create (or re-create with --reset) the Kafka topics used by the demo."""
import argparse
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic

from common import BOOTSTRAP_SERVERS, REPLICATION_FACTOR, TOPICS


def wait_until_deleted(admin, names, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        existing = admin.list_topics(timeout=10).topics
        if not any(n in existing for n in names):
            return
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reset", action="store_true", help="delete and re-create the demo topics (wipes data)")
    args = parser.parse_args()

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    try:
        existing = admin.list_topics(timeout=10).topics
    except Exception as exc:  # noqa: BLE001
        sys.exit(f"Cannot reach Kafka at {BOOTSTRAP_SERVERS}: {exc}\nIs your Confluent broker running?")

    if args.reset:
        to_delete = [t for t in TOPICS if t in existing]
        if to_delete:
            for topic, future in admin.delete_topics(to_delete).items():
                future.result()
                print(f"deleted  {topic}")
            wait_until_deleted(admin, to_delete)
            existing = admin.list_topics(timeout=10).topics

    new_topics = [
        NewTopic(name, num_partitions=cfg["partitions"], replication_factor=REPLICATION_FACTOR, config=cfg["config"])
        for name, cfg in TOPICS.items() if name not in existing
    ]
    if new_topics:
        for topic, future in admin.create_topics(new_topics).items():
            try:
                future.result()
                print(f"created  {topic}")
            except Exception as exc:  # noqa: BLE001
                print(f"FAILED   {topic}: {exc}")

    print(f"\nTopics on {BOOTSTRAP_SERVERS}:")
    metadata = admin.list_topics(timeout=10).topics
    for name in TOPICS:
        parts = len(metadata[name].partitions) if name in metadata else "missing!"
        print(f"  {name:<34} partitions={parts}")


if __name__ == "__main__":
    main()
