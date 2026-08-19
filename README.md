# CDC Pipeline: Postgres → Debezium → Kafka → BigQuery

## Objective

Build a hands-on, real (non-simulated) Change Data Capture (CDC) pipeline to understand how source-system changes can be captured and streamed into a cloud data warehouse in near real-time — without using a managed framework like Spark.

This project was built as preparation for a Data Engineering role requiring hands-on experience with CDC, Snowflake-style cloud warehousing, and high-volume transactional data pipelines (e-Invoicing use case). BigQuery was used in place of Snowflake due to trial/account availability, but the pipeline architecture and concepts transfer directly.

## Architecture

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
BigQuery Sink Connector (Kafka Connect)
   │  writes each change event as a row
   ▼
BigQuery (dataset: cdc_raw, table: cdc_public_invoices)
```

All services run locally in a GitHub Codespace via Docker Compose.

## What Was Built

1. **Source database**: Postgres 15, with `wal_level=logical` enabled and an `invoices` table (invoice_id, customer_name, amount, status, updated_at).

2. **CDC capture**: Debezium Postgres connector registered via Kafka Connect's REST API, using `pgoutput` logical decoding and a dedicated replication slot (`invoices_slot`), scoped to only the `invoices` table via `table.include.list` (to avoid a self-referential replication loop, discovered and fixed during a manual prototype phase — see "Key Lessons" below).

3. **Streaming transport**: Kafka + Zookeeper, single-broker local cluster. Kafka Connect configured with replication factor 1 for internal topics (config/offset/status storage), required for single-broker setups.

4. **Sink**: WePay/Confluent's `kafka-connect-bigquery` connector, installed into a custom Kafka Connect image (built on `confluentinc/cp-kafka-connect`) via `confluent-hub`. Configured with `autoCreateTables=true`, so the target BigQuery table schema is derived automatically from the incoming Debezium event schema (before/after/source/op/ts_ms/transaction).

5. **Manual CDC prototype (preliminary phase)**: Before building the full Kafka/Debezium stack, CDC internals were explored directly using Postgres's native logical replication API (`pg_create_logical_replication_slot`, `pg_logical_slot_get_changes`) via a small Python polling script. This surfaced two important production-relevant behaviors firsthand:
   - **DELETE events lose row data by default** (`REPLICA IDENTITY DEFAULT`) unless the table has `REPLICA IDENTITY FULL`, which is critical for any pipeline needing full old-row visibility (e.g. invoice deletion audit trails).
   - **A landing table sitting inside the same replication scope as the source data can cause a feedback loop** (each captured write becomes a new captured event). Solved by explicitly scoping the connector to only the intended source table(s).

## Full Stack

| Layer | Technology |
|---|---|
| Source database | PostgreSQL 15 |
| Change capture | Debezium 2.5 (PostgreSQL connector, pgoutput plugin) |
| Streaming | Apache Kafka + Zookeeper (Debezium images) |
| Connector runtime | Kafka Connect (Confluent `cp-kafka-connect:7.6.1` base image, custom-built) |
| Sink connector | WePay/Confluent `kafka-connect-bigquery` |
| Destination warehouse | Google BigQuery (dataset `cdc_raw`) |
| Orchestration of infra | Docker Compose |
| Environment | GitHub Codespaces |
| Planned but not yet implemented | dbt (raw → staging merge/upsert, schema contracts) + Airflow (orchestration, reconciliation) |

## Result

The pipeline was verified working end-to-end at the mechanical level:
- Inserting a row into Postgres `invoices` was correctly captured by Debezium and published as a structured JSON change event to the Kafka topic `cdc.public.invoices`, including full before/after row images, operation type (`op: c/u/d`), transaction ID, and LSN.
- The BigQuery sink connector successfully registered, ran, and auto-created the target table `cdc_raw.cdc_public_invoices` with a schema matching the Debezium event envelope.
- The connector correctly attempted to write each captured event to BigQuery in real time.

## Limitation Hit

Writes to BigQuery failed with:
```
Access Denied: BigQuery: Streaming insert is not allowed in the free tier
```

This is a **Google Cloud billing policy restriction**, not a pipeline design or configuration issue: the default BigQuery sink connector write path uses the streaming insert API, which requires a billing-enabled GCP project (a Sandbox/free-tier project without a linked payment method is blocked from streaming inserts, and blocked from provisioning most other paid-tier resources such as GCS buckets needed for the batch-load alternative).

**Diagnosis confirmed via connector logs** (`docker logs connect`), which surfaced the exact underlying `403 Forbidden` / `accessDenied` response from the BigQuery API — validating that every upstream layer (Postgres, Debezium, Kafka, Kafka Connect, the sink connector's write logic) was functioning correctly up to the final API call.

## Room for Improvement / Next Steps

1. **Resolve the write path**: either enable GCP billing (streaming inserts work immediately, GCP's free credit tier covers typical usage), or reconfigure the sink connector with `enableBatchLoad=true` + a GCS staging bucket, which avoids the streaming API and is free-tier compatible (though less real-time — writes land in batches on an interval rather than immediately).
2. **Swap BigQuery for Snowflake**: since the target role specifically requires Snowflake, re-pointing the sink to a Snowflake sink connector (Kafka Connector for Snowflake, officially maintained) would make this project directly match the job's tech stack.
3. **Add the merge/transform layer (dbt)**: currently, raw change events land as an append-only log (every INSERT/UPDATE/DELETE is a new row). The next step is a dbt model that performs a `MERGE`/upsert from this raw event log into a clean "current state" table per invoice, handling out-of-order and duplicate (at-least-once delivery) events idempotently.
4. **Add orchestration and reconciliation (Airflow)**: schedule the dbt merge runs, and add a reconciliation DAG comparing row counts / sums between Postgres source and the BigQuery current-state table to catch any pipeline gaps — directly relevant to the "data reconciliation" and "exception handling" requirements of the target role.
5. **Schema mismatch protection**: land raw data as loosely-typed/JSON to tolerate source schema drift, and enforce a stricter schema (dbt model contracts) at the staging layer so drift fails loudly there instead of silently corrupting downstream tables.
6. **Secrets handling**: connector configs currently have the Postgres password inlined directly in committed JSON for local development speed; production/portfolio-complete version should externalize this via Kafka Connect's `FileConfigProvider` or environment-variable substitution at deploy time.
7. **Persistent volumes**: Postgres and Kafka currently run without persistent Docker volumes, so data is lost on `docker compose down`. Adding named volumes would allow the environment to be stopped/restarted without re-seeding source data.

## Key Lessons (for interview discussion)

- CDC's core idea — read the database's change log instead of polling/batch-querying — is universal across databases, but each database has its own knob that determines how much detail is captured on UPDATE/DELETE (Postgres: `REPLICA IDENTITY`; MySQL: `binlog_format=ROW`; SQL Server: native CDC capture instances).
- A CDC pipeline's raw/landing layer must be explicitly scoped away from its own destination if the destination lives in the same source system, to avoid feedback loops.
- Kafka Connect's internal topics (config/offset/status storage) require enough brokers to satisfy their replication factor — single-broker local setups need this explicitly set to 1.
- Cloud provider free/sandbox tiers frequently restrict specific write paths (like BigQuery streaming inserts) even when the rest of the service is otherwise usable — a real operational constraint worth designing around (e.g., preferring batch load, or documenting billing prerequisites) rather than a pipeline bug.