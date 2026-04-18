# Kafka Trading Demo Project

這是一個以 Kafka 事件驅動為核心的交易系統示範，覆蓋你列出的 Work Package 0-9。

## Visualization Demo（GitHub Pages）

線上動畫頁（Kafka event distribution + orderflow）：

- https://okok147.github.io/kafka-demo-project/

互動功能：
- 送出 BUY/SELL 訂單
- 取消最後 OPEN 訂單
- 注入故障到 DLQ
- Replay DLQ
- 即時顯示 topic 計數、事件流、orders view、positions/pnl

Kafka 技能用例（一鍵場景）：
- 用例 1：即時行情串流與正規化（Producer/Consumer）
- 用例 2：Idempotency / Duplicate 防重
- 用例 3：Risk Reject（風控規則）
- 用例 4：DLQ + Replay（故障回復）
- 用例 5：Consumer Restart 恢復（可觀察事件持續與補處理）
- 用例 6：Security Deny（用戶存取控制）

## Cloud Hosting（免費、免信用卡）

建議用 Hugging Face Docker Space（官方有免費 CPU Basic：2 vCPU、16GB RAM、50GB 磁碟）。

- 官方說明（Docker Space）：https://huggingface.co/docs/hub/en/spaces-sdks-docker
- 官方免費硬體說明（CPU Basic FREE）：https://huggingface.co/docs/hub/spaces-overview

### 部署步驟（Hugging Face）
1. 在 Hugging Face 建立新 Space，SDK 選 `Docker`。
2. 連接這個 repo：`okok147/kafka-demo-project`。
3. Space 啟動後，公開網址即為整套 0-9 對外入口（Dashboard + API）。

公開入口可直接使用：
- `GET /` Dashboard
- `POST /orders`
- `POST /orders/{id}/cancel`
- `GET /dashboard`
- `POST /replay/jobs`
- `POST /replay/deadletters/{id}`

註：Docker Space 內是單容器啟動（Kafka + Postgres + workers + APIs 全部同機）；若要正式生產高可用，再改多服務拓撲。

## Alternative：Simulation Backend（免 Kafka/Postgres）

如果你不想依賴任何付費/外部基礎設施，可直接跑模擬後端。  
這個模式仍採用 Kafka 概念（topic、producer、consumer、event envelope、DLQ/replay），但 broker 與資料庫皆為 in-memory 模擬。

啟動：

```bash
PYTHONPATH=src python3 -m kafka_demo.entrypoints.sim_server
```

預設入口：
- `http://localhost:8080/` Dashboard
- `POST /orders`
- `POST /orders/{id}/cancel`
- `GET /dashboard`
- `GET /views/orders`
- `GET /views/positions`
- `GET /views/pnl`
- `POST /replay/jobs`
- `POST /replay/deadletters/{id}`

可調整埠：

```bash
PORT=7860 PYTHONPATH=src python3 -m kafka_demo.entrypoints.sim_server
```

## Cloud Hosting（Render，需付費）

若你之後接受付費部署，repo 也保留了 `render.yaml`：

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/okok147/kafka-demo-project)

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
