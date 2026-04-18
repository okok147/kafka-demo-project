from confluent_kafka import Consumer, Producer

from kafka_demo.common.config import settings



def build_producer() -> Producer:
    return Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})



def build_consumer(group_id: str, topics: list[str]) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe(topics)
    return consumer
