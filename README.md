# CDC Pipeline: Postgres → Debezium → Kafka → ClickHouse / Snowflake → dbt → Airflow

## Objective

Build a hands-on, real (non-simulated) Change Data Capture (CDC) pipeline covering the full lifecycle of streaming data into a warehouse: ingestion via CDC, cloud-warehouse-style transformation, schema/data quality validation, status-lifecycle tracking, and scheduled orchestration — without using a managed framework like Spark.

This project was built end-to-end, debugged from scratch, with every architectural decision — including three sink attempts (BigQuery, ClickHouse, Snowflake) — driven by real constraints hit during the build, documented below rather than glossed over. The final pipeline proves out **two working sinks in parallel**: a self-hosted ClickHouse instance and Snowflake, both fed from the same Kafka topic.

## Final Architecture (fully working, automated)

```
Postgres (source DB)
   │  logical replication (WAL)
   ▼
Debezium (Postgres source connector, runs inside Kafka Connect)
   │  captures INSERT / UPDATE / DELETE as structured events
   ▼
Kafka (topic: cdc.public.invoices — raw)
   │  durable, ordered event stream
   ▼
Gatekeeper (Python, always-on)
   │  validates fields: presence, type, unexpected new fields
   ├── valid   → Kafka topic: cdc.public.invoices.valid
   │                 ├── ClickHouse: invoices_typed (direct write)
   │                 └── Snowflake Kafka Connector → CDC_RAW.INVOICES."cdc.public.invoices.valid"
   └── invalid → Kafka topic: cdc.public.invoices.invalid
                     ├── ClickHouse: invoices_quarantine (direct write, includes rejection reason)
                     └── Snowflake Kafka Connector → CDC_RAW.INVOICES."cdc.public.invoices.invalid"
                        │
                        ▼
              dbt (scheduled by Airflow, every 15 min, scoped per warehouse)
                        │
                        ├── ClickHouse:  dbt run --select dim_invoices_current           → dbt test
                        └── Snowflake:   dbt run --select dim_invoices_current_snowflake → dbt test
```

**Key architectural point**: the Gatekeeper is the single validation checkpoint for the entire pipeline. Rather than duplicating validation logic per destination, it republishes validated events into their own Kafka topics (`.valid` / `.invalid`). Both ClickHouse and Snowflake — and any future sink — consume only from the `.valid` topic, so every downstream consumer is protected without needing any validation code of its own. Adding a third sink in the future would require zero new validation code, only pointing it at the existing `.valid` topic.

All services run in Docker Compose — locally on a personal machine or in a GitHub Codespace. No cloud billing required for the ClickHouse path; the Snowflake path uses a free trial account.

## What Was Built, Phase by Phase

### Phase 1 — CDC Capture, Streaming, and Sink Selection

- **Source database**: Postgres 15, `wal_level=logical`, an `invoices` table.
- **CDC capture**: Debezium Postgres connector via Kafka Connect's REST API (`pgoutput` plugin, dedicated replication slot, scoped via `table.include.list` to avoid a self-referential replication loop discovered during an earlier manual prototype).
- **Manual CDC prototype (preliminary)**: Before building the full stack, CDC internals were explored directly via Postgres's native logical replication API. This surfaced two production-relevant behaviors firsthand: DELETE events lose row data by default unless `REPLICA IDENTITY FULL` is set, and a landing table sharing replication scope with its source creates a feedback loop.
- **Sink attempt — BigQuery (abandoned, not lightly)**: WePay/Confluent's `kafka-connect-bigquery` connector was installed and successfully registered, but writes failed with `Access Denied: Streaming insert is not allowed in the free tier` — a GCP billing policy restriction on Sandbox/free-tier projects, confirmed via connector logs, not a pipeline defect. This restriction also blocks provisioning the GCS bucket needed for the batch-load alternative. Diagnosis confirmed every upstream layer (Postgres → Debezium → Kafka) was functioning correctly right up to the final write call. The sink was switched to self-hosted ClickHouse, and Snowflake was added later as a second target.
- **Sink — ClickHouse (working)**: Self-hosted ClickHouse, consuming from Kafka via its native `Kafka` table engine + materialized view, no Kafka Connect plugin required.
- **Sink — Snowflake (working)**: see Phase 4.
- **Gatekeeper**: A standalone, always-on Python service (`kafka-python` + `clickhouse-connect`) consuming from the raw Kafka topic. Before trusting any event, it checks: are all expected fields **present**? Are they the **correct type**? Are there any **unexpected new fields**? Valid events are written to ClickHouse's `invoices_typed` table **and** republished to `cdc.public.invoices.valid`; invalid events go to `invoices_quarantine` (with the full raw event and an exact reason) and are republished to `cdc.public.invoices.invalid`.

### Phase 2 — Transformation Layer (dbt)

- dbt-core + `dbt-clickhouse` adapter, containerized, connected via environment variables (no hardcoded credentials).
- Model `dim_invoices_current`: deduplicates the raw event log (`invoices_typed`) down to exactly one row per `invoice_id`, using `ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY event_ts DESC)` — a "current state" table tracking each invoice's latest lifecycle status. Full event history remains intact in `invoices_typed` for audit/reconciliation.
- dbt tests (`not_null`, `unique`) as the standardized, declarative counterpart to the Gatekeeper's hand-written Python checks.

### Phase 3 — Orchestration (Airflow) and Reconciliation

- Airflow 2.9.3, standalone mode, containerized, fixed admin credentials via `.env`.
- DAG `invoice_pipeline`, scheduled every 15 minutes, three tasks in dependency order: `dbt_run >> dbt_test >> reconciliation`.
- Built using `DockerOperator` — each task spins up a fresh, isolated container on the same Docker network, runs, and cleans up. Chosen over `BashOperator` after discovering the latter would require installing Docker CLI tools inside the Airflow image itself.
- **Reconciliation**: a standalone Python service comparing Postgres source row counts against the ClickHouse `dim_invoices_current` count, logging a clear PASS/FAIL.

### Phase 4 — Snowflake as a Second Sink

Since Snowflake is a common target warehouse in real-world data engineering roles, the pipeline was extended to feed it in parallel from the same Kafka topic, using the officially maintained Snowflake Kafka Connector — proving the architecture against a genuine cloud warehouse, not just a self-hosted substitute.

**Setup performed (all scripted, in `snowflake/setup.sql`):**
- A dedicated, auto-suspending warehouse (`CDC_WH`, XSMALL, so it doesn't burn credits while idle)
- A database and schema (`CDC_RAW.INVOICES`)
- A **least-privilege dedicated role and user** (`KAFKA_CONNECTOR_ROLE` / `KAFKA_CONNECTOR_USER`) — scoped to only this schema, with only `USAGE`, `CREATE TABLE`, `INSERT`, `SELECT` — not the personal admin login
- **Key-pair authentication**: an RSA key pair generated locally; only the public key ever leaves the local machine, attached to the Snowflake user via `ALTER USER ... SET RSA_PUBLIC_KEY=...`

**Connector setup and debugging (real issues hit and resolved):**
- Installed via `confluent-hub install snowflakeinc/snowflake-kafka-connector` into the same `connect` container already running Debezium and the (unused) BigQuery plugin
- First failure: `com.snowflake.kafka.connector.SnowflakeSinkConnector` does not exist in v4 of the connector — the actual class is `SnowflakeStreamingSinkConnector`, found directly from the connector's own error message listing available plugins
- Second failure: `value.converter` needed to be `org.apache.kafka.connect.json.JsonConverter`, not a Snowflake-specific class that doesn't exist
- Third failure: `snowflake.private.key must be non-empty` — this connector version requires the key **content** inline in the config (not a file path), unlike Debezium/BigQuery's pattern
- Fourth failure: several `snowflake.streaming.*` compatibility-check settings are mandatory in v4 unless explicitly disabled (`snowflake.streaming.validate.compatibility.with.classic=false`) or set (`snowflake.role.name`) — again, the error message enumerated every missing value precisely
- **A real security incident, caught and remediated**: because this connector requires the private key inline, an early debugging step printed the full connector registration response — including the private key — directly in a chat/terminal log. Recognized immediately, the key pair was treated as compromised: the old key was revoked, a fresh key pair was generated, and the new public key was reattached to the Snowflake user via `ALTER USER`, closing the exposure before the connector was ever used with production-shaped data. All subsequent registrations used the `${SNOWFLAKE_PRIVATE_KEY}` env-var substitution pattern with output suppressed (`curl -o /dev/null -w "%{http_code}"`) to prevent recurrence.
- **A permissions gap after success**: once the connector was running and writing data, querying the auto-created table `CDC_RAW.INVOICES."cdc.public.invoices"` failed with an access-control error — the table was owned by `KAFKA_CONNECTOR_ROLE`, and even the `ACCOUNTADMIN` role couldn't see it without an explicit `GRANT SELECT`, illustrating how Snowflake's role-based access is enforced even against admin-level accounts by default.

**Result — verified with real data**: rows inserted into Postgres are visible end-to-end in Snowflake within seconds, with `rowsInsertedCount` incrementing and `rowsErrorCount=0` in the connector's own status logs, and confirmed directly via `SELECT` against the Snowflake table showing the full Debezium event payload (`RECORD_METADATA`, `SCHEMA`, `PAYLOAD` columns, Snowflake's default ingestion schema for schemaless JSON).

### Phase 5 — dbt and Airflow Extended to Snowflake

The dbt project was extended with a second target profile (`snowflake`, alongside the original `dev`/ClickHouse target), using the `dbt-snowflake` adapter and the same key-pair authentication already set up for the Kafka connector.

- **Model `dim_invoices_current_snowflake`**: parses the Debezium event out of Snowflake's raw `PAYLOAD` JSON column (`PAYLOAD:after:invoice_id::INT`, etc.) and applies the same `ROW_NUMBER()` deduplication logic as the ClickHouse model — same business rule, different SQL dialect, since Snowflake's raw landing table stores the whole event as JSON rather than pre-parsed columns.
- **A real dialect issue, found and fixed**: the raw Snowflake table name (`cdc.public.invoices.valid`, with literal dots) needed to be explicitly double-quoted in dbt's `source` definition (`identifier: '"cdc.public.invoices.valid"'`) — without it, Snowflake parsed the dots as database/schema/table separators and failed with "too many qualifiers."
- **Tests**: the same `not_null` / `unique` checks as the ClickHouse model, run independently against Snowflake — confirmed passing (4/4) on real data.
- **Airflow integration, and a genuine model-selection bug**: two new DAG tasks (`dbt_run_snowflake >> dbt_test_snowflake`) were added as an independent branch alongside the existing ClickHouse chain. The first attempt broke the ClickHouse tasks: `dbt run` with no model selection tries to build *every* model in the project against whichever target is active, so the ClickHouse task attempted to compile Snowflake-specific syntax (`PAYLOAD:after:invoice_id::INT`) as ClickHouse SQL and failed with a syntax error. Fixed by scoping every dbt task explicitly with `--select <model_name>`, so each warehouse's task only ever touches its own model.

### Phase 6 — Closing the Validation Gap for Snowflake

Originally, the Snowflake Kafka Connector consumed directly from the raw `cdc.public.invoices` topic — meaning Snowflake received every event, including schema-broken ones, with no protection at all, unlike ClickHouse's Gatekeeper-mediated path. Two changes closed this gap:

1. **The Gatekeeper republishes to Kafka, not just ClickHouse.** After validating each event (the same presence/type/unexpected-field checks as before), it publishes the event into `cdc.public.invoices.valid` or `cdc.public.invoices.invalid`, in addition to its existing ClickHouse writes.
2. **Both Snowflake connectors were repointed** to consume from these topics instead of the raw one: the main sink now reads only `cdc.public.invoices.valid` (deleting and re-registering it with an updated `topics` config), and a second, new connector (`invoices-snowflake-quarantine`) was added consuming `cdc.public.invoices.invalid` into its own auto-created Snowflake table. This gave Snowflake full clean/quarantine parity with ClickHouse — rejected events are never silently dropped on either sink, and both are independently inspectable.

Verified by inspecting the raw Kafka topic directly (`kafka-console-consumer`) to confirm the split was working, then confirming both connectors' channel logs referenced the correct topic post-switch, and querying both new Snowflake tables directly to confirm previously-quarantined test invoices appeared correctly there too.

Along the way, a legitimate column (`tax_amount`, added earlier during schema-drift testing) was being incorrectly flagged as "unexpected" on every event because it had never been added to the Gatekeeper's `EXPECTED_FIELDS` — fixed by adding it with a type that allows `null`.

**One remaining, permanent asymmetry**: ClickHouse's quarantine table stores the Gatekeeper's plain-English rejection reason alongside the raw event (`invoices_quarantine.reason`), via a direct write from the Gatekeeper. Snowflake's quarantine table has only the raw event payload, since the reason was never part of the Kafka message itself — only ClickHouse gets it directly from the Gatekeeper's own write path.

## Schema Change Scenarios — Tested Live, Not Theoretical

Each of the following was deliberately caused on the running Postgres source and traced end-to-end through Kafka into the Gatekeeper's decision (ClickHouse path):

| Change | How it was caused | Result |
|---|---|---|
| **Column added** (`due_date`, later `tax_amount`) | `ALTER TABLE invoices ADD COLUMN ...` | Debezium auto-included the new field with zero config. Initially the Gatekeeper silently ignored genuinely new fields (a real gap, caught and named explicitly) — **fixed**: the Gatekeeper now explicitly flags any field not in its known set as `"unexpected new field(s)"`. |
| **Column deleted / renamed** (`customer_name` → `client_name`) | `ALTER TABLE invoices RENAME COLUMN ...` | Correctly quarantined — `"missing field: customer_name"`, full original event preserved. |
| **Column type changed** (`customer_name` text → integer) | `ALTER TABLE invoices ALTER COLUMN ... TYPE INTEGER` | Correctly quarantined — `"wrong type for customer_name: expected str, got int"`. |

## Full Stack

| Layer | Technology |
|---|---|
| Source database | PostgreSQL 15 |
| Change capture | Debezium 2.5 (PostgreSQL connector, pgoutput plugin) |
| Streaming | Apache Kafka + Zookeeper (Debezium images) |
| Connector runtime | Kafka Connect (Confluent `cp-kafka-connect:7.6.1` base image, custom-built) |
| Sink (attempted, not used) | WePay/Confluent `kafka-connect-bigquery` → Google BigQuery |
| Sink (working) | ClickHouse (native Kafka table engine + materialized view) |
| Sink (working) | Snowflake (official Kafka Connector, Snowpipe Streaming, key-pair auth) |
| Schema validation | Standalone Python service (`kafka-python` consumer/producer + `clickhouse-connect`), always-on container; republishes to `cdc.public.invoices.valid` / `.invalid` Kafka topics so both sinks share one validation checkpoint |
| Transformation | dbt-core + `dbt-clickhouse` + `dbt-snowflake`, containerized, one model per warehouse, each dbt task scoped with `--select` |
| Orchestration | Apache Airflow 2.9.3 (standalone mode, `DockerOperator`), containerized |
| Reconciliation | Standalone Python service comparing Postgres source vs. ClickHouse destination counts |
| Infra orchestration | Docker Compose, all long-running services `restart: unless-stopped`, named volumes on every stateful service |
| Environment | GitHub Codespaces / local Docker (Ubuntu) — pipeline verified portable across both |

## Result

The pipeline runs continuously and unattended, end to end, feeding two independent, verified sinks from a single validation checkpoint. A row inserted into Postgres — clean or schema-broken — is captured by Debezium, streamed through Kafka, and evaluated once by the Gatekeeper, which sorts it into a validated or invalid Kafka topic. Both ClickHouse and Snowflake consume only from the validated topic (and both have their own quarantine table for rejected events), so neither ever sees schema-broken data undetected. Airflow's scheduled DAG runs dbt against both warehouses in parallel — rebuilding and re-testing each one's current-state table — plus a reconciliation check confirming Postgres and the ClickHouse destination counts match.

## Room for Improvement / Next Steps

1. **Secrets handling maturity**: connector/service configs reference values via `.env` (git-ignored); the Snowflake private key required extra care beyond `.env` alone given the key-exposure incident. The next step for full production-readiness would be a proper secrets manager (e.g. HashiCorp Vault, cloud KMS) rather than plain environment variables, plus a documented key-rotation runbook.
2. **Failure alerting/observability**: right now, a failure anywhere in the pipeline (a connector crashing, a dbt test failing, a container stuck restarting) is only visible by manually checking logs or the Airflow UI — there's no notification when something breaks. Next step would be wiring Airflow's built-in failure callbacks (email/Slack on task failure) and/or a lightweight health-check/alerting layer on the always-on services (Gatekeeper, connectors) that aren't managed by Airflow at all.

## Key Lessons

- Docker containers without named volumes lose all state on recreation — this was hit repeatedly throughout the project (source tables, connector registrations, and Airflow's admin user all needed re-creating after restarts, including across a full migration from GitHub Codespaces to a local machine and back). Fixed by adding named volumes for every stateful service (`postgres`, `zookeeper`, `kafka`, `clickhouse`, `airflow`) and verified directly: a test row inserted into Postgres survived a full `docker compose rm` + recreate of the container.
- Validation rules belong in data, not code: the Gatekeeper's expected-fields contract was originally a hardcoded Python dict; moving it into an external `schema_contract.json` file (loaded at startup, types resolved from string names) means the schema contract can be reviewed, diffed, and edited by someone without touching application logic.
- A single dbt project serving multiple warehouse targets must scope every run explicitly with `--select <model>`, or `dbt run` will try to build every model against whichever target is active — a model written in one warehouse's SQL dialect will fail loudly against another if not excluded.
- Table or column identifiers containing characters outside normal SQL naming (like literal dots, from a Kafka topic name such as `cdc.public.invoices.valid`) must be explicitly quoted in dbt source definitions, or the database will misinterpret the dots as path separators between database/schema/table.
- CDC's core idea — read the database's change log instead of polling/batch-querying — is universal across databases, but each database has its own knob determining how much detail is captured on UPDATE/DELETE (Postgres: `REPLICA IDENTITY`; MySQL: `binlog_format=ROW`; SQL Server: native CDC capture instances).
- Every warehouse vendor has its own authentication model for automated services, and they are not interchangeable: ClickHouse used simple username/password; BigQuery used a service-account JSON key; Snowflake's Kafka Connector required RSA key-pair authentication specifically, with the private key needed inline in config (not as a file reference).
- Schema mismatch protection has at least three distinct failure modes, each needing separate handling: **missing/renamed fields**, **type changes**, and **unexpected new fields** — a presence-only check misses the second and third categories entirely.
- Silent failure is the real danger in schema drift, not crashes: raw JSON-extraction functions often return an empty value on a missing field by default rather than erroring.
- A quarantine pattern (route invalid records to a separate table with full raw payload + reason) preserves both auditability and uptime.
- Validation is cheaper to build once, upstream, than per-destination: republishing validated events into their own Kafka topic meant a second consumer (Snowflake) could inherit full protection with zero new validation code — the fix was a one-line `topics` config change, not new logic.
- Role-based access in a real warehouse applies even to admin accounts by default: a table created by a service role was invisible to `ACCOUNTADMIN` until an explicit `GRANT SELECT` was issued — a good illustration of least-privilege design working as intended, not a bug.
- **Handling a real credential exposure**: when a private key was inadvertently displayed in a debugging session, the correct response was immediate rotation — revoke the exposed key, generate a fresh pair, reattach the new public key, and change tooling (output suppression) to prevent recurrence — rather than assuming a low-actual-risk situation meant no action was needed.
- Connector configuration errors from mature, well-maintained plugins (Snowflake's, in this case) tend to be genuinely actionable — the exact missing/invalid config keys were enumerated directly in the error response, in contrast to vaguer failures seen with less mature tooling.