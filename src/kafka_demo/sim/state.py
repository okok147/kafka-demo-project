from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


@dataclass
class Position:
    account_id: str
    symbol: str
    quantity: Decimal
    avg_price: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal


@dataclass
class SimulationState:
    account_limits: dict[str, dict[str, Any]] = field(
        default_factory=lambda: {
            "ACC-001": {"max_notional": Decimal("200000"), "max_open_orders": 20},
            "ACC-002": {"max_notional": Decimal("50000"), "max_open_orders": 5},
        }
    )
    idempotency_keys: dict[str, dict[str, Any]] = field(default_factory=dict)
    orders: dict[str, dict[str, Any]] = field(default_factory=dict)
    order_timeline: list[dict[str, Any]] = field(default_factory=list)
    open_orders: dict[str, dict[str, Any]] = field(default_factory=dict)
    quote_cache: dict[str, dict[str, Decimal]] = field(default_factory=dict)
    fill_ledger: list[dict[str, Any]] = field(default_factory=list)
    account_balances: dict[str, Decimal] = field(default_factory=dict)
    cash_ledger: list[dict[str, Any]] = field(default_factory=list)
    positions: dict[tuple[str, str], Position] = field(default_factory=dict)
    pnl_snapshots: list[dict[str, Any]] = field(default_factory=list)
    deadletter_events: list[dict[str, Any]] = field(default_factory=list)
    replay_jobs: dict[str, dict[str, Any]] = field(default_factory=dict)
