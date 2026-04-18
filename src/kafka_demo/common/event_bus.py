from confluent_kafka import Producer

from kafka_demo.common.envelope import EventEnvelope, new_event
from kafka_demo.common.serde import serialize_envelope
from kafka_demo.common.topics import DEADLETTER_EVENTS



def publish_event(producer: Producer, envelope: EventEnvelope) -> None:
    producer.produce(envelope.topic, key=envelope.key, value=serialize_envelope(envelope))
    producer.flush(5)



def publish_deadletter(
    producer: Producer,
    source_service: str,
    original_topic: str,
    original_key: str,
    raw_payload: str,
    error_message: str,
) -> None:
    deadletter = new_event(
        event_type="deadletter.created",
        topic=DEADLETTER_EVENTS,
        key=original_key or "unknown",
        payload={
            "source_service": source_service,
            "original_topic": original_topic,
            "event_key": original_key,
            "event_payload": raw_payload,
            "error_message": error_message,
        },
    )
    publish_event(producer, deadletter)
