# CDC Pipeline: Postgres → Debezium → Kafka → ClickHouse (with a BigQuery attempt documented)

## Objective

Build a hands-on, real (non-simulated) Change Data Capture (CDC) pipeline to understand how source-system changes can be captured and streamed into a data warehouse in near real-time — without using a managed framework like Spark.

This project was built as preparation for a Data Engineering role requiring hands-on experience with CDC, Snowflake-style cloud warehousing, and high-volume transactional data pipelines (e-Invoicing use case). The pipeline was first built against BigQuery, then re-pointed to a self-hosted ClickHouse instance after hitting a cloud billing restriction — both attempts are documented below, since the debugging process itself demonstrates real CDC/pipeline understanding.

## Final Architecture (working end-to-end)

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
ClickHouse Kafka Table Engine (native consumer, no separate connector)
   │  materialized view continuously copies new messages
   ▼
ClickHouse MergeTree table (cdc_invoices_raw) — queryable via SQL / Web UI
```

All services run locally in a GitHub Codespace via Docker Compose — no cloud account or billing required for the final working version.

## What Was Built

1. **Source database**: Postgres 15, with `wal_level=logical` enabled and an `invoices` table (invoice_id, customer_name, amount, status, updated_at).

2. **CDC capture**: Debezium Postgres connector registered via Kafka Connect's REST API, using `pgoutput` logical decoding and a dedicated replication slot (`invoices_slot`), scoped to only the `invoices` table via `table.include.list`.

3. **Streaming transport**: Kafka + Zookeeper, single-broker local cluster. Kafka Connect configured with replication factor 1 for internal topics (config/offset/status storage), required for single-broker setups.

4. **Manual CDC prototype (preliminary phase)**: Before building the full Kafka/Debezium stack, CDC internals were explored directly using Postgres's native logical replication API (`pg_create_logical_replication_slot`, `pg_logical_slot_get_changes`) via a small Python polling script. This surfaced two important production-relevant behaviors firsthand:
   - **DELETE events lose row data by default** (`REPLICA IDENTITY DEFAULT`) unless the table has `REPLICA IDENTITY FULL` — critical for pipelines needing full old-row visibility (e.g. invoice deletion audit trails).
   - **A landing table sitting inside the same replication scope as the source data causes a feedback loop** (each captured write becomes a new captured event). Solved by explicitly scoping the connector to only the intended source table(s).

5. **Sink — attempt 1 (BigQuery)**: WePay/Confluent's `kafka-connect-bigquery` connector was installed into a custom Kafka Connect image (built on `confluentinc/cp-kafka-connect`) via `confluent-hub`. It successfully registered, ran, and auto-created the target BigQuery table schema from the Debezium event structure — but writes failed with `Access Denied: Streaming insert is not allowed in the free tier`, a GCP billing policy restriction (not a pipeline defect). See "BigQuery Attempt" section below for full detail.

6. **Sink — attempt 2 / final working solution (ClickHouse)**: Self-hosted ClickHouse (Docker), using its native `Kafka` table engine to consume directly from the `cdc.public.invoices` topic — no Kafka Connect plugin required. A `MergeTree` table plus a `MATERIALIZED VIEW` continuously persist incoming events. Verified end-to-end: inserting a row into Postgres reliably appears in ClickHouse within seconds, queryable both via `clickhouse-client` and ClickHouse's built-in Web SQL UI (port 8123).

## Full Stack

| Layer | Technology |
|---|---|
| Source database | PostgreSQL 15 |
| Change capture | Debezium 2.5 (PostgreSQL connector, pgoutput plugin) |
| Streaming | Apache Kafka + Zookeeper (Debezium images) |
| Connector runtime | Kafka Connect (Confluent `cp-kafka-connect:7.6.1` base image, custom-built) |
| Sink (attempted) | WePay/Confluent `kafka-connect-bigquery` → Google BigQuery |
| Sink (working) | ClickHouse (native Kafka table engine + materialized view) |
| Orchestration of infra | Docker Compose |
| Environment | GitHub Codespaces |
| Planned but not yet implemented | dbt (raw → staging merge/upsert, schema contracts) + Airflow (orchestration, reconciliation) |

## Result

The pipeline is verified working end-to-end, with real data provably traveling through every layer:
- Inserting a row into Postgres `invoices` is captured by Debezium and published as a structured JSON change event to Kafka, including full before/after row images, operation type (`op: c/u/d`), transaction ID, and LSN.
- ClickHouse's Kafka table engine consumes the topic in real time; a materialized view persists each event into a permanent `MergeTree` table.
- Confirmed via direct query (both CLI and Web UI) that inserted rows appear in ClickHouse within seconds of the source Postgres insert, with the full Debezium event payload intact.

## BigQuery Attempt (documented, not abandoned lightly)

Writes to BigQuery failed with:
```
Access Denied: BigQuery: Streaming insert is not allowed in the free tier
```

This is a **Google Cloud billing policy restriction**, not a pipeline design or configuration issue: the default BigQuery sink connector write path uses the streaming insert API, which requires a billing-enabled GCP project. A Sandbox/free-tier project without a linked payment method is blocked from streaming inserts, and also blocked from provisioning the GCS bucket needed for the batch-load alternative (`enableBatchLoad`) — so the workaround path was blocked by the same root constraint.

**Diagnosis confirmed via connector logs** (`docker logs connect`), which surfaced the exact underlying `403 Forbidden` / `accessDenied` response from the BigQuery API — validating that every upstream layer (Postgres, Debezium, Kafka, Kafka Connect, the sink connector's write logic) was functioning correctly right up to the final API call. Given no available billing method, the sink was switched to a self-hosted ClickHouse instance, which achieves the same architectural goal (CDC events landing in a queryable analytical store) with zero cloud dependency.

## Room for Improvement / Next Steps

1. **Swap ClickHouse for Snowflake in a real environment**: since the target role specifically requires Snowflake, the same Debezium/Kafka pipeline could be pointed at Snowflake's officially maintained Kafka Connector when billing/trial access is available — the capture and transport layers would not need to change.
2. **Add the merge/transform layer (dbt)**: currently, raw change events land as an append-only log (every INSERT/UPDATE/DELETE is a new row). The next step is a model that performs a `MERGE`/upsert from this raw event log into a clean "current state" table per invoice, handling out-of-order and duplicate (at-least-once delivery) events idempotently.
3. **Add orchestration and reconciliation (Airflow)**: schedule merge runs, and add a reconciliation DAG comparing row counts / sums between Postgres source and the destination current-state table to catch any pipeline gaps — directly relevant to the "data reconciliation" and "exception handling" requirements of the target role.
4. **Schema mismatch protection**: land raw data as loosely-typed/JSON (already the case here) to tolerate source schema drift, and enforce a stricter schema at the staging/transform layer so drift fails loudly there instead of silently corrupting downstream tables. Next experiment: alter the Postgres `invoices` table live (add/rename/change a column type) and observe exactly how the Debezium event schema and downstream consumers respond.
5. **Secrets handling**: connector/service configs currently reference values via `.env` (git-ignored) rather than hardcoded — the Postgres and ClickHouse passwords follow this pattern; the next step for full production-readiness would be externalizing secrets via Kafka Connect's `FileConfigProvider` or a secrets manager rather than plain environment variables.
6. **Persistent volumes**: Postgres, Kafka, and ClickHouse currently run without persistent Docker volumes, so data is lost on `docker compose down` (observed directly during this project — source table and connector registrations had to be recreated multiple times). Adding named volumes would allow the environment to be stopped/restarted without re-seeding source data.

## Key Lessons 

- CDC's core idea — read the database's change log instead of polling/batch-querying — is universal across databases, but each database has its own knob that determines how much detail is captured on UPDATE/DELETE (Postgres: `REPLICA IDENTITY`; MySQL: `binlog_format=ROW`; SQL Server: native CDC capture instances).
- A CDC pipeline's raw/landing layer must be explicitly scoped away from its own destination if the destination lives in the same source system, to avoid feedback loops.
- Kafka Connect's internal topics (config/offset/status storage) require enough brokers to satisfy their replication factor — single-broker local setups need this explicitly set to 1.
- Cloud provider free/sandbox tiers frequently restrict specific write paths (like BigQuery streaming inserts) even when the rest of the service is otherwise usable — a real operational constraint worth designing around, not a pipeline bug.
- Not every sink requires Kafka Connect: some destinations (like ClickHouse) can consume directly from Kafka via a native table engine, which can be simpler to operate for certain architectures and is a legitimate alternative to the Kafka Connect sink-connector pattern.
- Docker containers with no persistent volumes lose all state on `down`/recreate — when only some containers in a multi-service stack restart (rather than all together), previously-registered identities (like a Kafka broker ID held in Zookeeper) can conflict with freshly-started ones. Restarting a stack's stateful services together (or using `stop`/`start` instead of `down`/`up` when no config has changed) avoids this class of error.