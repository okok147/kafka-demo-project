from __future__ import annotations

from decimal import Decimal



def risk_decision(
    *,
    max_notional: Decimal,
    max_open_orders: int,
    open_notional: Decimal,
    open_order_count: int,
    qty: Decimal,
    price: Decimal,
) -> tuple[bool, str]:
    notional = qty * price
    if open_order_count >= max_open_orders:
        return False, "OPEN_ORDER_LIMIT"
    if open_notional + notional > max_notional:
        return False, "NOTIONAL_LIMIT"
    return True, "ACCEPTED"



def should_fill(side: str, limit_price: Decimal | None, bid: Decimal, ask: Decimal) -> tuple[bool, Decimal]:
    if side == "BUY":
        target = ask
        if limit_price is None:
            return True, target
        return limit_price >= ask, target

    target = bid
    if limit_price is None:
        return True, target
    return limit_price <= bid, target



def cancel_after_fill(open_order_exists: bool) -> str:
    return "execution.order.cancelled" if open_order_exists else "execution.order.cancel_after_fill"



def replay_rebuild_ids(deadletter_ids: list[int], already_replayed: set[int]) -> list[int]:
    return [x for x in deadletter_ids if x not in already_replayed]
