from __future__ import annotations

import json
from typing import Any


def normalize_deadletter_payload(raw_payload: Any) -> dict[str, Any]:
    if isinstance(raw_payload, dict):
        return raw_payload

    if isinstance(raw_payload, list):
        return {"items": raw_payload}

    if isinstance(raw_payload, str):
        try:
            parsed = json.loads(raw_payload)
        except json.JSONDecodeError:
            return {"raw": raw_payload}

        if isinstance(parsed, dict):
            return parsed
        if isinstance(parsed, list):
            return {"items": parsed}
        return {"value": parsed}

    return {"value": raw_payload}


def encode_payload_for_replay(payload: Any) -> bytes:
    if isinstance(payload, dict):
        return json.dumps(payload, separators=(",", ":")).encode("utf-8")

    if isinstance(payload, list):
        return json.dumps(payload, separators=(",", ":")).encode("utf-8")

    if isinstance(payload, str):
        stripped = payload.strip()
        try:
            parsed = json.loads(stripped)
            if isinstance(parsed, (dict, list)):
                return json.dumps(parsed, separators=(",", ":")).encode("utf-8")
            return json.dumps({"value": parsed}, separators=(",", ":")).encode("utf-8")
        except json.JSONDecodeError:
            return json.dumps({"raw": payload}, separators=(",", ":")).encode("utf-8")

    return json.dumps({"value": payload}, separators=(",", ":")).encode("utf-8")
