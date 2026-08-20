# CDC Pipeline: Postgres → Debezium → Kafka → ClickHouse, with Automated Schema Mismatch Protection

## Objective

Build a hands-on, real (non-simulated) Change Data Capture (CDC) pipeline to understand how source-system changes can be captured, streamed, and safely landed in a data warehouse in near real-time — without using a managed framework like Spark — and to implement genuine schema mismatch protection rather than just describing it.

This project was built as preparation for a Data Engineering role requiring hands-on experience with CDC, Snowflake-style cloud warehousing, high-volume transactional data pipelines (e-Invoicing use case), and data quality / exception handling. The pipeline was first built against BigQuery, then re-pointed to a self-hosted ClickHouse instance after hitting a cloud billing restriction — both attempts are documented, since the debugging process itself demonstrates real CDC/pipeline understanding.

## Final Architecture (fully working, always-on)

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
   │  validates each event's fields (presence + type) before it's trusted
   ├── valid  → ClickHouse: invoices_typed (clean, structured table)
   └── invalid → ClickHouse: invoices_quarantine (raw event + exact reason, nothing lost)
```

All services run locally in a GitHub Codespace via Docker Compose, each with `restart: unless-stopped` — no cloud account or billing required, and no manual step needed to keep the pipeline running.

## What Was Built

1. **Source database**: Postgres 15, with `wal_level=logical` enabled and an `invoices` table.

2. **CDC capture**: Debezium Postgres connector registered via Kafka Connect's REST API, using `pgoutput` logical decoding and a dedicated replication slot (`invoices_slot`), scoped to only the `invoices` table via `table.include.list` (avoiding a self-referential replication loop discovered during an earlier manual prototype).

3. **Streaming transport**: Kafka + Zookeeper, single-broker local cluster, with Kafka Connect's internal topics set to replication factor 1 (required for single-broker setups).

4. **Manual CDC prototype (preliminary phase)**: Before building the full stack, CDC internals were explored directly via Postgres's native logical replication API (`pg_create_logical_replication_slot`, `pg_logical_slot_get_changes`) using a small Python polling script. This surfaced two production-relevant behaviors firsthand: DELETE events lose row data by default unless `REPLICA IDENTITY FULL` is set, and a landing table sharing replication scope with its source can create a feedback loop.

5. **Sink — attempt 1 (BigQuery, documented but not the final solution)**: WePay/Confluent's `kafka-connect-bigquery` connector was installed into a custom Kafka Connect image via `confluent-hub`. It successfully registered and attempted writes, but failed with `Access Denied: Streaming insert is not allowed in the free tier` — a GCP billing policy restriction, confirmed via connector logs, not a pipeline defect.

6. **Sink — final working solution (ClickHouse)**: Self-hosted ClickHouse, consuming from Kafka via its native `Kafka` table engine plus a materialized view, requiring no Kafka Connect plugin.

7. **Schema mismatch protection — the Gatekeeper**: A standalone Python service (its own Docker container, `restart: unless-stopped`) that consumes directly from the Kafka topic using `kafka-python`, and before trusting any event:
   - Checks that every expected field (`invoice_id`, `customer_name`, `status`) is **present**
   - Checks that each present field is the **correct type** (e.g. `customer_name` must be a string)
   - Routes the event to `invoices_typed` (clean) if all checks pass, or to `invoices_quarantine` (full raw event + exact human-readable reason, e.g. `"wrong type for customer_name: expected str, got int"`) if any check fails
   - Runs continuously and automatically — verified by inserting new rows into Postgres with no manual script launch and confirming they were classified correctly within seconds, purely from container logs

## Schema Change Scenarios — Tested Live, Not Theoretical

Each of the following was deliberately caused on the running Postgres source and observed end-to-end through Kafka into the Gatekeeper's decision:

| Change | How it was caused | Gatekeeper's result |
|---|---|---|
| **Column added** (`due_date`) | `ALTER TABLE invoices ADD COLUMN due_date DATE` | Passed through as OK — Debezium auto-included the new field in the event schema with zero config; the Gatekeeper ignores fields outside its expected set (documented as a real limitation — new columns are currently invisible to the clean table until the Gatekeeper's expected-fields list is updated) |
| **Column deleted / renamed** (`customer_name` → `client_name`) | `ALTER TABLE invoices RENAME COLUMN customer_name TO client_name` | Correctly quarantined — `"missing field: customer_name"`, with the full original event preserved (data itself isn't lost, just held for review) |
| **Column type changed** (`customer_name` text → integer) | `ALTER TABLE invoices ALTER COLUMN customer_name TYPE INTEGER USING 0` | Correctly quarantined — `"wrong type for customer_name: expected str, got int"` |

## Full Stack

| Layer | Technology |
|---|---|
| Source database | PostgreSQL 15 |
| Change capture | Debezium 2.5 (PostgreSQL connector, pgoutput plugin) |
| Streaming | Apache Kafka + Zookeeper (Debezium images) |
| Connector runtime | Kafka Connect (Confluent `cp-kafka-connect:7.6.1` base image, custom-built) |
| Sink (attempted) | WePay/Confluent `kafka-connect-bigquery` → Google BigQuery |
| Sink (working) | ClickHouse (native Kafka table engine + materialized view) |
| Schema validation | Standalone Python service (`kafka-python` + `clickhouse-connect`), its own always-on Docker container |
| Orchestration of infra | Docker Compose, all services `restart: unless-stopped` |
| Environment | GitHub Codespaces |
| Planned but not yet implemented | dbt (batch transform layer, model contracts) + Airflow (scheduling, reconciliation) |

## Result

The pipeline runs continuously and unattended. A row inserted into Postgres — clean or schema-broken — is captured by Debezium, streamed through Kafka, evaluated by the Gatekeeper, and lands in the correct destination table (clean or quarantine) within seconds, with no manual intervention and no silent data loss at any stage.

## BigQuery Attempt (documented, not abandoned lightly)

Writes to BigQuery failed with `Access Denied: BigQuery: Streaming insert is not allowed in the free tier` — a Google Cloud billing policy restriction on Sandbox/free-tier projects, which also blocks provisioning the GCS bucket needed for the batch-load alternative (`enableBatchLoad`). Diagnosis was confirmed via connector logs (`docker logs connect`), which surfaced the exact `403 Forbidden` response from the BigQuery API, validating that every upstream layer was functioning correctly right up to the final write call. Given no available billing method, the sink was switched to self-hosted ClickHouse.

## Room for Improvement / Next Steps

1. **Swap ClickHouse for Snowflake in a real environment**: since the target role specifically requires Snowflake, the same Debezium/Kafka pipeline could point at Snowflake's officially maintained Kafka Connector when billing/trial access is available.
2. **Handle "column added" properly**: currently new columns are silently ignored by the Gatekeeper rather than flagged. A stronger version would compare the incoming event's full field set against a known schema and flag *unexpected new fields* too, not just missing/wrong-type ones — giving visibility into additive drift, not just breaking drift.
3. **Add the merge/transform layer (dbt)**: raw events currently land as an append-only log. The next step is a model that performs a `MERGE`/upsert into a clean "current state" table per invoice, idempotent against at-least-once Kafka delivery.
4. **Add orchestration and reconciliation (Airflow)**: schedule merge runs, and add a reconciliation DAG comparing row counts/sums between Postgres source and the destination current-state table — directly relevant to the "data reconciliation" and "exception handling" requirements of the target role.
5. **Formalize the schema contract**: move `EXPECTED_FIELDS` out of hardcoded Python into a versioned config (e.g., JSON Schema or a dbt contract), so schema expectations are explicit, reviewable, and reusable outside the Gatekeeper script itself.
6. **Secrets handling**: connector/service configs reference values via `.env` (git-ignored); production-readiness would mean externalizing secrets via Kafka Connect's `FileConfigProvider` or a secrets manager.
7. **Persistent volumes**: Postgres, Kafka, and ClickHouse currently run without persistent Docker volumes, so data is lost on `docker compose down` (observed directly multiple times during this project). Adding named volumes would allow the environment to be stopped/restarted without re-seeding source data.

## Key Lessons 

- CDC's core idea — read the database's change log instead of polling/batch-querying — is universal across databases, but each database has its own knob determining how much detail is captured on UPDATE/DELETE (Postgres: `REPLICA IDENTITY`; MySQL: `binlog_format=ROW`; SQL Server: native CDC capture instances).
- Schema mismatch protection has two honest failure modes to design for separately: **missing/renamed fields** (structural absence) and **type changes** (structurally present but semantically wrong) — a presence-only check silently misses the second category.
- Silent failure is the real danger in schema drift, not crashes: ClickHouse's `JSONExtractString` on a missing field returns an empty string by default, not an error — which can quietly corrupt a "clean" table unless explicitly checked for.
- A quarantine pattern (route invalid records to a separate table with full raw payload + reason, rather than dropping or crashing) preserves both auditability and uptime — nothing is lost, nothing blocks the pipeline, and every failure has a clear, specific explanation.
- Not every sink requires Kafka Connect: some destinations (like ClickHouse) can consume directly from Kafka via a native table engine — a legitimate, simpler alternative to the Kafka Connect sink-connector pattern for certain architectures.
- Cloud provider free/sandbox tiers frequently restrict specific write paths (like BigQuery streaming inserts) even when the rest of the service is otherwise usable — a real operational constraint worth designing around, not a pipeline bug.
- Python print statements are buffered by default inside Docker containers and won't appear in `docker logs` until the buffer flushes; running Python with `-u` (unbuffered) is necessary for real-time log visibility in containerized services.
- Docker containers with no persistent volumes lose all state on `down`/recreate; restarting a stack's stateful services together (or using `stop`/`start` instead of `down`/`up` when no config has changed) avoids state-mismatch errors between dependent services (e.g., Kafka broker re-registration conflicts in Zookeeper).