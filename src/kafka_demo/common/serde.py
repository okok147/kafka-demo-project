import json
from dataclasses import asdict

from kafka_demo.common.envelope import EventEnvelope


class SerdeError(RuntimeError):
    pass


def serialize_envelope(envelope: EventEnvelope) -> bytes:
    try:
        return json.dumps(asdict(envelope), separators=(",", ":")).encode("utf-8")
    except Exception as exc:  # pragma: no cover - defensive path
        raise SerdeError(f"serialize failed: {exc}") from exc


def deserialize_envelope(raw: bytes) -> EventEnvelope:
    try:
        obj = json.loads(raw.decode("utf-8"))
        return EventEnvelope(**obj)
    except Exception as exc:
        raise SerdeError(f"deserialize failed: {exc}") from exc
