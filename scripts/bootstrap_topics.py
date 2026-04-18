import time
from confluent_kafka.admin import AdminClient, NewTopic

TOPICS = [
    "market.raw",
    "market.normalized",
    "order.commands",
    "risk.accepted",
    "risk.rejected",
    "execution.events",
    "portfolio.events",
    "query.events",
    "replay.jobs",
    "deadletter.events",
]


def main() -> None:
    bootstrap = __import__("os").environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    admin = AdminClient({"bootstrap.servers": bootstrap})

    for attempt in range(30):
        try:
            metadata = admin.list_topics(timeout=5)
            existing = set(metadata.topics.keys())
            to_create = [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in TOPICS if t not in existing]
            if to_create:
                futures = admin.create_topics(to_create)
                for topic, future in futures.items():
                    future.result()
                    print(f"created topic {topic}")
            print("topic bootstrap done")
            return
        except Exception as exc:  # pragma: no cover - infra bootstrap
            print(f"waiting kafka ({attempt}): {exc}")
            time.sleep(2)

    raise RuntimeError("failed to bootstrap topics")


if __name__ == "__main__":
    main()
