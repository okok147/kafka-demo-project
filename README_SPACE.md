---
title: Kafka Demo Project
emoji: "📈"
colorFrom: blue
colorTo: green
sdk: docker
app_port: 7860
---

# Kafka Demo Project Space

這個 Space 會以單容器啟動完整交易示範鏈路：
- Kafka (KRaft)
- Postgres
- Market / Risk / Execution / Portfolio / Replay / DLQ workers
- Order API（內部）
- Query Projector + Dashboard（對外）

對外 API:
- `GET /`
- `POST /orders`
- `POST /orders/{id}/cancel`
- `GET /dashboard`
- `GET /views/orders`
- `GET /views/positions`
- `GET /views/pnl`
