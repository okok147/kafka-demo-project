# Kafka Trading Demo Project

這是一個以 Kafka 事件驅動為核心的交易系統示範，覆蓋你列出的 Work Package 0-9。

## Cloud Hosting（非 localhost）

本 repo 已提供 `render.yaml`，可直接在 Render 佈署整套 0-9（Kafka、Postgres、API、workers）。

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/okok147/kafka-demo-project)

部署後會得到至少兩個公開網址：
- `order-api-service.onrender.com`
- `query-projector.onrender.com`（Dashboard 網站入口）

註：`kafka-broker`、各 worker、Postgres 走 Render private network，不暴露公網。

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
- Market tick 觸發重試成交：新 `market.normalized` 會掃描該 symbol 未成交單
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
- Manual replay API: `POST /replay/deadletters/{id}`
- Manual replay: `scripts/manual_replay.py`

### Work Package 9：Tests
- `tests/test_happy_path.py`
- `tests/test_duplicate_event.py`
- `tests/test_consumer_restart.py`
- `tests/test_replay_rebuild.py`
- `tests/test_cancel_after_fill.py`
- `tests/test_dlq_payload.py`

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

### 手動重播單一 deadletter

```bash
curl -X POST 'http://localhost:8080/replay/deadletters/1?requested_by=ops'
```

## 本地測試

```bash
PYTHONPATH=src pytest -q
```

## Render 部署後 Smoke Test（範例）

先把 `<ORDER_API_URL>`、`<QUERY_URL>` 換成你的 Render 網址。

```bash
curl -X POST <ORDER_API_URL>/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: ord-render-001' \
  -d '{"account_id":"ACC-001","symbol":"AAPL","side":"BUY","quantity":"1","price":"200"}'
```

```bash
curl <QUERY_URL>/dashboard
```
