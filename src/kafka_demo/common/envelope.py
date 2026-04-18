from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
import uuid


@dataclass
class EventEnvelope:
    event_id: str
    event_type: str
    topic: str
    key: str
    payload: dict[str, Any]
    occurred_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    schema_version: int = 1
    metadata: dict[str, Any] = field(default_factory=dict)



def new_event(event_type: str, topic: str, key: str, payload: dict[str, Any], metadata: dict[str, Any] | None = None) -> EventEnvelope:
    return EventEnvelope(
        event_id=str(uuid.uuid4()),
        event_type=event_type,
        topic=topic,
        key=key,
        payload=payload,
        metadata=metadata or {},
    )
