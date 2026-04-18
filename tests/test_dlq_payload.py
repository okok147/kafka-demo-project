import json

from kafka_demo.common.dlq_payload import encode_payload_for_replay, normalize_deadletter_payload


def test_normalize_deadletter_payload_parses_event_json():
    raw = '{"event_id":"e1","payload":{"x":1}}'
    normalized = normalize_deadletter_payload(raw)
    assert normalized["event_id"] == "e1"
    assert normalized["payload"]["x"] == 1


def test_normalize_deadletter_payload_wraps_non_json():
    normalized = normalize_deadletter_payload("not-json")
    assert normalized == {"raw": "not-json"}


def test_encode_payload_for_replay_handles_legacy_string_json():
    legacy = '{"event_id":"e2","payload":{"ok":true}}'
    encoded = encode_payload_for_replay(legacy)
    decoded = json.loads(encoded.decode("utf-8"))
    assert decoded["event_id"] == "e2"
    assert decoded["payload"]["ok"] is True


def test_encode_payload_for_replay_handles_dict():
    encoded = encode_payload_for_replay({"event_id": "e3", "payload": {"v": 7}})
    decoded = json.loads(encoded.decode("utf-8"))
    assert decoded["event_id"] == "e3"
    assert decoded["payload"]["v"] == 7
