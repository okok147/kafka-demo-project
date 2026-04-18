from decimal import Decimal

from kafka_demo.services.logic import risk_decision, should_fill



def test_end_to_end_happy_path():
    accepted, reason = risk_decision(
        max_notional=Decimal("100000"),
        max_open_orders=10,
        open_notional=Decimal("0"),
        open_order_count=0,
        qty=Decimal("10"),
        price=Decimal("100"),
    )
    assert accepted is True
    assert reason == "ACCEPTED"

    filled, fill_price = should_fill("BUY", Decimal("101"), Decimal("99.9"), Decimal("100.1"))
    assert filled is True
    assert fill_price == Decimal("100.1")
