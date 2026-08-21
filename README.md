# CDC Pipeline: Postgres → Debezium → Kafka → ClickHouse → dbt → Airflow

## Objective

Build a hands-on, real (non-simulated) Change Data Capture (CDC) pipeline covering the full lifecycle described in a target Data Engineering JD: ingestion via CDC, cloud-warehouse-style transformation, schema/data quality validation, status-lifecycle tracking, and scheduled orchestration — without using a managed framework like Spark.

This project was built end-to-end, debugged from scratch, with every architectural decision (including two pivots — BigQuery → ClickHouse, and BashOperator → DockerOperator) driven by real constraints hit during the build, documented below rather than glossed over.

## Final Architecture (fully working, automated)

```
Postgres (source DB)
   │  logical replication (WAL)
   ▼
Debezium (Postgres source connector, runs inside Kafka Connect)
   │  captures INSERT / UPDATE / DELETE as structured events
   ▼
Kafka (topic: cdc.public.invoices)
   │  durable, ordered event stream
   ▼
Gatekeeper (Python, always-on container)
   │  validates each event's fields (presence + type + unexpected-field detection)
   ├── valid   → ClickHouse: invoices_typed (clean, structured event log)
   └── invalid → ClickHouse: invoices_quarantine (raw event + exact reason, nothing lost)
                        │
                        ▼
              dbt (scheduled by Airflow, every 15 min)
                        │
                        ├── dbt run  → dim_invoices_current (deduplicated, one row per invoice, latest state)
                        └── dbt test → not_null + unique checks on the model
```

All services run locally in a GitHub Codespace via Docker Compose — no cloud account or billing required, and the full pipeline runs unattended once started.

## What Was Built, Phase by Phase

### Phase 1 — CDC Capture, Streaming, and Schema Mismatch Protection
- **Source database**: Postgres 15, `wal_level=logical`, an `invoices` table.
- **CDC capture**: Debezium Postgres connector via Kafka Connect's REST API (`pgoutput` plugin, dedicated replication slot, scoped via `table.include.list` to avoid a self-referential replication loop discovered during an earlier manual prototype).
- **Manual CDC prototype (preliminary)**: Before building the full stack, CDC internals were explored directly via Postgres's native logical replication API. This surfaced two production-relevant behaviors firsthand: DELETE events lose row data by default unless `REPLICA IDENTITY FULL` is set, and a landing table sharing replication scope with its source creates a feedback loop.
- **Sink — attempt 1 (BigQuery, not the final solution)**: WePay/Confluent's `kafka-connect-bigquery` connector was installed and successfully registered, but writes failed with `Access Denied: Streaming insert is not allowed in the free tier` — a GCP billing policy restriction, confirmed via connector logs, not a pipeline defect.
- **Sink — final (ClickHouse)**: Self-hosted ClickHouse, consuming from Kafka via its native `Kafka` table engine + materialized view, no Kafka Connect plugin required.
- **Gatekeeper**: A standalone, always-on Python service (`kafka-python` + `clickhouse-connect`) consuming directly from Kafka. Before trusting any event, it checks: are all expected fields **present**? Are they the **correct type**? Are there any **unexpected new fields**? Valid events go to `invoices_typed`; anything failing any check goes to `invoices_quarantine` with the full raw event and an exact, human-readable reason — nothing is ever silently dropped or corrupted.

### Phase 2 — Transformation Layer (dbt)
- dbt-core + `dbt-clickhouse` adapter, containerized, connected via environment variables (no hardcoded credentials).
- Model `dim_invoices_current`: deduplicates the raw event log (`invoices_typed`, which can have multiple rows per invoice as status changes over time) down to exactly one row per `invoice_id`, using `ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY event_ts DESC)` — this is the "current state" table matching the JD's "status-processing mechanisms to track invoice transactions throughout the lifecycle." The full event history remains intact in `invoices_typed` for audit/reconciliation purposes — current-state and full-history are kept as separate, complementary tables, not one replacing the other.
- dbt tests (`not_null`, `unique`) as the standardized, declarative counterpart to the Gatekeeper's hand-written Python checks — with an explicit, tested distinction understood between the two: dbt's `not_null` checks for `NULL`, not for "empty but present" values, which the Gatekeeper's logic catches separately.

### Phase 3 — Orchestration (Airflow)
- Airflow 2.9.3, standalone mode, containerized with a fixed admin user (via `.env`, avoiding the randomly-generated password default).
- DAG `invoice_pipeline`, scheduled every 15 minutes (`*/15 * * * *`), two tasks in dependency order: `dbt_run >> dbt_test`.
- Built using `DockerOperator` (not `BashOperator`) — each task spins up a fresh, isolated `dbt` container on the same Docker network, runs the command, and cleans up afterward. This was a deliberate correction after discovering `BashOperator` would have required installing Docker CLI tools inside the Airflow image itself; `DockerOperator` is the more standard, purpose-built pattern for orchestrating containerized tasks from Airflow.
- Verified with a real **scheduled** (not manually triggered) run: Airflow's scheduler woke up on its own at the 15-minute mark, ran `dbt_run` (model rebuilt successfully) then `dbt_test` (all 4 tests passed), fully unattended.

## Schema Change Scenarios — Tested Live, Not Theoretical

Each of the following was deliberately caused on the running Postgres source and traced end-to-end through Kafka into the Gatekeeper's decision:

| Change | How it was caused | Result |
|---|---|---|
| **Column added** (`due_date`, later `tax_amount`) | `ALTER TABLE invoices ADD COLUMN ...` | Debezium auto-included the new field with zero config. Initially the Gatekeeper silently ignored genuinely new fields (a real gap, caught and named explicitly rather than glossed over) — **fixed**: the Gatekeeper now explicitly flags any field not in its known set as `"unexpected new field(s)"`, closing the additive-drift blind spot. |
| **Column deleted / renamed** (`customer_name` → `client_name`) | `ALTER TABLE invoices RENAME COLUMN ...` | Correctly quarantined — `"missing field: customer_name"`, full original event preserved. |
| **Column type changed** (`customer_name` text → integer) | `ALTER TABLE invoices ALTER COLUMN ... TYPE INTEGER` | Correctly quarantined — `"wrong type for customer_name: expected str, got int"`. |

## Full Stack

| Layer | Technology |
|---|---|
| Source database | PostgreSQL 15 |
| Change capture | Debezium 2.5 (PostgreSQL connector, pgoutput plugin) |
| Streaming | Apache Kafka + Zookeeper (Debezium images) |
| Connector runtime | Kafka Connect (Confluent `cp-kafka-connect:7.6.1` base image, custom-built) |
| Sink (attempted) | WePay/Confluent `kafka-connect-bigquery` → Google BigQuery |
| Sink (working) | ClickHouse (native Kafka table engine + materialized view) |
| Schema validation | Standalone Python service (`kafka-python` + `clickhouse-connect`), always-on container |
| Transformation | dbt-core + dbt-clickhouse, containerized |
| Orchestration | Apache Airflow 2.9.3 (standalone mode, `DockerOperator`), containerized |
| Infra orchestration | Docker Compose, all long-running services `restart: unless-stopped` |
| Environment | GitHub Codespaces |
| Planned but not yet implemented | Airflow reconciliation task (source vs. destination row/sum counts); Snowflake as final sink |

## Result

The pipeline runs continuously and unattended, end to end. A row inserted into Postgres — clean or schema-broken — is captured by Debezium, streamed through Kafka, evaluated by the Gatekeeper, lands in the correct raw table, and is picked up automatically by Airflow's scheduled dbt run, which rebuilds the deduplicated current-state table and re-verifies its integrity via tests — all without manual intervention, and with no silent data loss at any stage.

## BigQuery Attempt (documented, not abandoned lightly)

Writes to BigQuery failed with `Access Denied: BigQuery: Streaming insert is not allowed in the free tier` — a Google Cloud billing policy restriction on Sandbox/free-tier projects, which also blocks provisioning the GCS bucket needed for the batch-load alternative (`enableBatchLoad`). Diagnosis was confirmed via connector logs, validating every upstream layer was functioning correctly right up to the final write call. Given no available billing method, the sink was switched to self-hosted ClickHouse.

## Room for Improvement / Next Steps

1. **Add the reconciliation task**: a third Airflow task comparing Postgres source row/sum counts against `dim_invoices_current`, flagging drift — the one piece of the original plan not yet built, directly matching the JD's "data reconciliation" requirement.
2. **Swap ClickHouse for Snowflake in a real environment**: since the target role specifically requires Snowflake, the same Debezium/Kafka pipeline could point at Snowflake's officially maintained Kafka Connector when billing/trial access is available — the capture, Gatekeeper, and orchestration layers would not need to change.
3. **Formalize the Gatekeeper's schema contract**: move `EXPECTED_FIELDS` out of a hardcoded Python dict into a versioned config (e.g. JSON Schema), so schema expectations are explicit, reviewable, and reusable outside the script itself.
4. **Persistent volumes**: Postgres, Kafka, ClickHouse, and Airflow currently run without persistent Docker volumes, so state (data, connector registrations, admin users) is lost on full recreation — observed and worked around directly multiple times during this project. Named volumes would remove this friction.
5. **Secrets handling**: configs currently reference values via `.env` (git-ignored) rather than hardcoded — the next step for full production-readiness would be externalizing secrets via a proper secrets manager rather than plain environment variables.

## Key Lessons 

- CDC's core idea — read the database's change log instead of polling/batch-querying — is universal across databases, but each database has its own knob determining how much detail is captured on UPDATE/DELETE (Postgres: `REPLICA IDENTITY`; MySQL: `binlog_format=ROW`; SQL Server: native CDC capture instances).
- CDC and batch orchestration (Airflow + dbt) are not competing choices — CDC solves low-latency, complete extraction (including deletes) from the source; Airflow + dbt still handle the batch transform/merge/test layer on top of the continuously-landing raw data. A pure-batch pipeline (incremental `WHERE updated_at > last_run` queries) is often sufficient and simpler; CDC earns its complexity specifically when near-real-time freshness, delete-visibility, or minimizing source-system load matter.
- Schema mismatch protection has (at least) three distinct failure modes, each needing separate handling: **missing/renamed fields** (structural absence), **type changes** (structurally present but semantically wrong), and **unexpected new fields** (additive drift, silently invisible rather than actively harmful, but still a real gap if unhandled) — a presence-only check misses the second and third categories entirely, which is exactly what building and then correcting this pipeline demonstrated firsthand.
- Silent failure is the real danger in schema drift, not crashes: raw JSON-extraction functions often return an empty value on a missing field by default rather than erroring — which can quietly produce technically-valid-looking but wrong data unless explicitly checked for.
- A quarantine pattern (route invalid records to a separate table with full raw payload + reason, rather than dropping or crashing) preserves both auditability and uptime.
- Not every sink requires Kafka Connect: some destinations (like ClickHouse) can consume directly from Kafka via a native table engine — a legitimate, simpler alternative to the Kafka Connect sink-connector pattern.
- For orchestrating containerized tasks from Airflow, `DockerOperator` is the correct, purpose-built tool — `BashOperator` shelling out to `docker compose` requires the orchestrator's own image to have Docker CLI tools installed, which is unnecessary extra surface area when `DockerOperator` talks to the Docker daemon directly via the Python SDK.
- Cloud provider free/sandbox tiers frequently restrict specific write paths (like BigQuery streaming inserts) even when the rest of the service is otherwise usable — a real operational constraint worth designing around, not a pipeline bug.
- Docker containers with no persistent volumes lose all state on recreation; this was hit repeatedly (source tables, connector registrations, Airflow's admin user all needed re-creating after restarts) and worked around by fixing credentials via environment variables and re-registering connectors as needed — a real, recurring operational lesson about the cost of skipping persistent storage even in a "just for learning" setup.
