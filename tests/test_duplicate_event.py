import uuid



def test_duplicate_event_idempotency_key_returns_same_order_id():
    idempotency_key = "dup-key"
    store = {}

    def process() -> str:
        if idempotency_key in store:
            return store[idempotency_key]
        order_id = str(uuid.uuid4())
        store[idempotency_key] = order_id
        return order_id

    first = process()
    second = process()
    assert first == second
