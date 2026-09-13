#!/bin/bash
set -e
cd "$(dirname "$0")/.."

echo "=== 1/5 Loading environment variables ==="
set -a
source .env
set +a

echo "=== 2/5 Starting core infrastructure ==="
docker compose up -d postgres zookeeper kafka clickhouse
echo "Waiting for Kafka to stabilize..."
sleep 20

echo "=== 3/5 Ensuring ClickHouse tables exist ==="
docker compose exec -T clickhouse clickhouse-client --multiquery << 'EOF'
CREATE TABLE IF NOT EXISTS invoices_typed
(
    invoice_id Int32,
    customer_name String,
    status String,
    op String,
    event_ts DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY event_ts;

CREATE TABLE IF NOT EXISTS invoices_quarantine
(
    invoice_id Int32,
    raw_message String,
    reason String,
    event_ts DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY event_ts;
EOF

echo "=== 4/5 Starting Connect, Gatekeeper, Airflow ==="
docker compose up -d connect
echo "Waiting for Kafka Connect..."
until curl -s -o /dev/null -w "%{http_code}" http://localhost:8083/connectors | grep -q "200"; do
  sleep 5
done
docker compose up -d gatekeeper airflow

echo "=== 5/5 Registering connectors if missing ==="
CONNECTORS=("invoices-postgres-connector" "invoices-snowflake-sink" "invoices-snowflake-quarantine")
for name in "${CONNECTORS[@]}"; do
  status=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:8083/connectors/$name")
  if [ "$status" == "200" ]; then
    echo "OK — $name already registered."
  else
    echo "Registering $name ..."
    envsubst < "connectors/${name}.json" > "/tmp/${name}-resolved.json"
    http_code=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" --data @"/tmp/${name}-resolved.json" http://localhost:8083/connectors)
    echo "  HTTP $http_code"
    rm -f "/tmp/${name}-resolved.json"
  fi
done

echo "=== Done ==="
docker compose ps
