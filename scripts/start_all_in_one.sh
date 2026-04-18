#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH=/app/src
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:9092}"
export POSTGRES_HOST="${POSTGRES_HOST:-127.0.0.1}"
export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
export POSTGRES_DB="${POSTGRES_DB:-kafkademo}"
export POSTGRES_USER="${POSTGRES_USER:-kafka}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-kafka}"
export ORDER_API_PORT="${ORDER_API_PORT:-8000}"
export QUERY_API_PORT="${QUERY_API_PORT:-${PORT:-7860}}"

echo "[boot] starting postgres"
PG_MAJOR="$(ls /etc/postgresql | sort -V | tail -n 1)"
pg_ctlcluster "${PG_MAJOR}" main start

runuser -u postgres -- psql -v ON_ERROR_STOP=1 -d postgres -c "DO \$\$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='${POSTGRES_USER}') THEN CREATE ROLE ${POSTGRES_USER} LOGIN PASSWORD '${POSTGRES_PASSWORD}'; ELSE ALTER ROLE ${POSTGRES_USER} WITH LOGIN PASSWORD '${POSTGRES_PASSWORD}'; END IF; END \$\$;"
if ! runuser -u postgres -- psql -tAc "SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}'" | grep -q 1; then
  runuser -u postgres -- createdb -O "${POSTGRES_USER}" "${POSTGRES_DB}"
fi

echo "[boot] applying schema"
PGPASSWORD="${POSTGRES_PASSWORD}" psql -h 127.0.0.1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -f /app/sql/init.sql >/tmp/schema.log 2>&1

echo "[boot] preparing kafka"
mkdir -p /tmp/kraft-combined-logs
cat >/tmp/kafka-server.properties <<'EOF'
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
log.dirs=/tmp/kraft-combined-logs
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
EOF

"${KAFKA_HOME}/bin/kafka-storage.sh" format -t abcdefghijklmnopqrstuv -c /tmp/kafka-server.properties --ignore-formatted >/tmp/kafka-format.log 2>&1 || true
"${KAFKA_HOME}/bin/kafka-server-start.sh" /tmp/kafka-server.properties >/tmp/kafka.log 2>&1 &
KAFKA_PID=$!

for _ in $(seq 1 120); do
  if nc -z 127.0.0.1 9092; then
    break
  fi
  sleep 1
done

if ! nc -z 127.0.0.1 9092; then
  echo "[boot] kafka failed to start"
  tail -n 100 /tmp/kafka.log || true
  exit 1
fi

echo "[boot] bootstrap topics and admin data"
python /app/scripts/bootstrap_topics.py
python /app/scripts/admin_bootstrap.py

echo "[boot] starting workers and apis"
python -m kafka_demo.entrypoints.market_data_simulator >/tmp/market-data-simulator.log 2>&1 &
python -m kafka_demo.entrypoints.market_normalizer >/tmp/market-normalizer.log 2>&1 &
python -m kafka_demo.entrypoints.risk_service >/tmp/risk-service.log 2>&1 &
python -m kafka_demo.entrypoints.execution_service >/tmp/execution-service.log 2>&1 &
python -m kafka_demo.entrypoints.portfolio_service >/tmp/portfolio-service.log 2>&1 &
python -m kafka_demo.entrypoints.replay_runner >/tmp/replay-runner.log 2>&1 &
python -m kafka_demo.entrypoints.deadletter_handler >/tmp/deadletter-handler.log 2>&1 &
python -m kafka_demo.entrypoints.order_api >/tmp/order-api.log 2>&1 &

trap 'kill ${KAFKA_PID} >/dev/null 2>&1 || true' EXIT

exec python -m kafka_demo.entrypoints.query_projector
