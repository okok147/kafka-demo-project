# Kafka Trading Demo Project

這是一個以 Kafka 事件驅動為核心的交易系統示範，覆蓋你列出的 Work Package 0-9。

## 架構與 Work Package 對應

### Work Package 0：Infra
- Docker Compose: `kafka`, `postgres`, 各 app service 全部在 `app_net`
- Topic auto-create: `scripts/bootstrap_topics.py`
- Admin bootstrap: `scripts/admin_bootstrap.py`
- Common env: `.env.example`

### Work Package 1：Common library
- Shared event envelope: `src/kafka_demo/common/envelope.py`
- Topic constants: `src/kafka_demo/common/topics.py`
- Serde: `src/kafka_demo/common/serde.py`
- Base error handling: `src/kafka_demo/common/errors.py`

### Work Package 2：Market path
- `market-data-simulator`
- `market-normalizer-service`
- Publish normalized ticks to `market.normalized`

### Work Package 3：Order path
- `order-api-service`
- `POST /orders`
- `POST /orders/{id}/cancel`
- Idempotency key: `Idempotency-Key` header

### Work Package 4：Risk
- Account limits table: `account_limits`
- Risk rules: max notional + max open orders
- Accepted/rejected events: `risk.accepted`, `risk.rejected`

### Work Package 5：Execution
- Open orders table: `open_orders`
- Quote cache: `quote_cache`
- Fill simulator: execution worker
- Cancel logic: cancel vs cancel-after-fill

### Work Package 6：Portfolio
- Fill ledger: `fill_ledger`
- Cash ledger: `cash_ledger`
- Positions current: `positions`
- PnL updates: `pnl_snapshots` + market tick updates

### Work Package 7：Query projector
- Orders view: `/views/orders`
- Timeline view: `/views/orders/{order_id}/timeline`
- Positions view: `/views/positions`
- PnL view: `/views/pnl`
- Dashboard API: `/dashboard`
- Website UI: `GET /` served by query-projector

### Work Package 8：Replay & DLQ
- Replay jobs table: `replay_jobs`
- Replay runner: `replay-runner`
- Deadletter topic/handler: `deadletter.events` + `deadletter-handler`
- Manual replay: `scripts/manual_replay.py`

### Work Package 9：Tests
- `tests/test_happy_path.py`
- `tests/test_duplicate_event.py`
- `tests/test_consumer_restart.py`
- `tests/test_replay_rebuild.py`
- `tests/test_cancel_after_fill.py`

## 啟動

```bash
docker compose up --build
```

## API

- Order API: `http://localhost:8000`
- Dashboard Website + Query API: `http://localhost:8080`

### 下單範例

```bash
curl -X POST http://localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: ord-001' \
  -d '{"account_id":"ACC-001","symbol":"AAPL","side":"BUY","quantity":"10","price":"200"}'
```

### 取消範例

```bash
curl -X POST http://localhost:8000/orders/<order_id>/cancel \
  -H 'Content-Type: application/json' \
  -d '{"reason":"user_clicked_cancel"}'
```

### 建立 replay job

```bash
curl -X POST 'http://localhost:8080/replay/jobs?requested_by=ops&deadletter_event_id=1'
```

## 本地測試

```bash
PYTHONPATH=src pytest -q
```
