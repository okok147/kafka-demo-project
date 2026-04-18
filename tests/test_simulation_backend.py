import asyncio

from kafka_demo.common.envelope import new_event
from kafka_demo.common.topics import MARKET_RAW
from kafka_demo.sim.engine import SimulationEngine


async def _run_happy_path() -> None:
    engine = SimulationEngine(enable_market_simulator=False)
    await engine.start()
    try:
        # Seed quote via market raw -> normalized pipeline.
        await engine.broker.produce(
            new_event(
                event_type="market.tick.raw",
                topic=MARKET_RAW,
                key="AAPL",
                payload={"symbol": "AAPL", "bid": 199.95, "ask": 200.0, "source": "test"},
            )
        )
        await asyncio.sleep(0.05)

        first = await engine.create_order(
            {"account_id": "ACC-001", "symbol": "AAPL", "side": "BUY", "quantity": 1, "price": 200},
            idempotency_key="idem-1",
        )
        second = await engine.create_order(
            {"account_id": "ACC-001", "symbol": "AAPL", "side": "BUY", "quantity": 1, "price": 200},
            idempotency_key="idem-1",
        )
        assert first["order_id"] == second["order_id"]

        await asyncio.sleep(0.1)
        dashboard = await engine.dashboard()
        assert dashboard["total_orders"] == 1
        assert dashboard["total_fills"] >= 1
    finally:
        await engine.close()


def test_simulation_backend_happy_path() -> None:
    asyncio.run(_run_happy_path())
