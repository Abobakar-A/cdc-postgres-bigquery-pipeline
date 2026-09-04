# CDC Pipeline: Postgres → Debezium → Kafka → ClickHouse / Snowflake → dbt → Airflow

## Objective

Build a hands-on, real (non-simulated) Change Data Capture (CDC) pipeline covering the full lifecycle described: ingestion via CDC, cloud-warehouse-style transformation, schema/data quality validation, status-lifecycle tracking, and scheduled orchestration — without using a managed framework like Spark.

This project was built end-to-end, debugged from scratch, with every architectural decision — including three sink attempts (BigQuery, ClickHouse, Snowflake) — driven by real constraints hit during the build, documented below rather than glossed over. The final pipeline proves out **two working sinks in parallel**: a self-hosted ClickHouse instance and Snowflake , both fed from the same Kafka topic.

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
                     └── ClickHouse: invoices_quarantine (direct write)
                        │
                        ▼
              dbt (scheduled by Airflow, every 15 min)
                        │
                        ├── dbt run  → dim_invoices_current (deduplicated, one row per invoice, latest state)
                        └── dbt test → not_null + unique checks on the model
```

**Key architectural point**: the Gatekeeper is the single validation checkpoint for the entire pipeline. Rather than duplicating validation logic per destination, it republishes validated events into their own Kafka topics (`.valid` / `.invalid`). Both ClickHouse and Snowflake — and any future sink — consume only from the `.valid` topic, so every downstream consumer is protected without needing any validation code of its own. This was a deliberate correction: Snowflake originally consumed directly from the raw topic and had no protection at all; repointing it to the validated topic closed that gap without touching the Snowflake connector's own logic.

All services run in Docker Compose — locally on a personal machine or in a GitHub Codespace. No cloud billing required for the ClickHouse path; the Snowflake path uses a free trial account.

## What Was Built, Phase by Phase

### Phase 1 — CDC Capture, Streaming, and Schema Mismatch Protection
- **Source database**: Postgres 15, `wal_level=logical`, an `invoices` table.
- **CDC capture**: Debezium Postgres connector via Kafka Connect's REST API (`pgoutput` plugin, dedicated replication slot, scoped via `table.include.list` to avoid a self-referential replication loop discovered during an earlier manual prototype).
- **Manual CDC prototype (preliminary)**: Before building the full stack, CDC internals were explored directly via Postgres's native logical replication API. This surfaced two production-relevant behaviors firsthand: DELETE events lose row data by default unless `REPLICA IDENTITY FULL` is set, and a landing table sharing replication scope with its source creates a feedback loop.
- **Sink — attempt 1 (BigQuery, not used further)**: WePay/Confluent's `kafka-connect-bigquery` connector was installed and successfully registered, but writes failed with `Access Denied: Streaming insert is not allowed in the free tier` — a GCP billing policy restriction, confirmed via connector logs, not a pipeline defect.
- **Sink — ClickHouse (working)**: Self-hosted ClickHouse, consuming from Kafka via its native `Kafka` table engine + materialized view, no Kafka Connect plugin required.
- **Sink — Snowflake (working)**: See dedicated section below.
- **Gatekeeper**: A standalone, always-on Python service (`kafka-python` + `clickhouse-connect`) consuming from the raw Kafka topic. Before trusting any event, it checks: are all expected fields **present**? Are they the **correct type**? Are there any **unexpected new fields**? Valid events are written to ClickHouse's `invoices_typed` table **and** republished to a new Kafka topic, `cdc.public.invoices.valid`; invalid events go to `invoices_quarantine` (with the full raw event and an exact reason) and are republished to `cdc.public.invoices.invalid`. This makes the Gatekeeper a shared validation checkpoint for the whole pipeline — see Phase 4 for how this closed a real protection gap for Snowflake.

### Phase 2 — Transformation Layer (dbt)
- dbt-core + `dbt-clickhouse` adapter, containerized, connected via environment variables (no hardcoded credentials).
- Model `dim_invoices_current`: deduplicates the raw event log (`invoices_typed`) down to exactly one row per `invoice_id`, using `ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY event_ts DESC)` — the "current state" table matching the JD's "status-processing mechanisms to track invoice transactions throughout the lifecycle." Full event history remains intact in `invoices_typed` for audit/reconciliation.
- dbt tests (`not_null`, `unique`) as the standardized, declarative counterpart to the Gatekeeper's hand-written Python checks.

### Phase 3 — Orchestration (Airflow) and Reconciliation
- Airflow 2.9.3, standalone mode, containerized, fixed admin credentials via `.env`.
- DAG `invoice_pipeline`, scheduled every 15 minutes, three tasks in dependency order: `dbt_run >> dbt_test >> reconciliation`.
- Built using `DockerOperator` — each task spins up a fresh, isolated container on the same Docker network, runs, and cleans up. Chosen over `BashOperator` after discovering the latter would require installing Docker CLI tools inside the Airflow image itself.
- **Reconciliation**: a standalone Python service comparing Postgres source row counts against the ClickHouse `dim_invoices_current` count, logging a clear PASS/FAIL — directly implementing the JD's "data reconciliation" requirement.

### Phase 4 — Snowflake as a Second, Production-Matching Sink

Since the target JD specifically requires Snowflake (not ClickHouse), the pipeline was extended to feed Snowflake in parallel from the same Kafka topic, using the officially maintained Snowflake Kafka Connector — proving the architecture against the actual target warehouse, not just a substitute.

**Setup performed (all scripted, in `snowflake/setup.sql`):**
- A dedicated, auto-suspending warehouse (`CDC_WH`, XSMALL, so it doesn't burn credits while idle)
- A database and schema (`CDC_RAW.INVOICES`)
- A **least-privilege dedicated role and user** (`KAFKA_CONNECTOR_ROLE` / `KAFKA_CONNECTOR_USER`) — scoped to only this schema, with only `USAGE`, `CREATE TABLE`, `INSERT`, `SELECT` — not the personal admin login
- **Key-pair authentication**: an RSA key pair generated locally; only the public key ever leaves the local machine, attached to the Snowflake user via `ALTER USER ... SET RSA_PUBLIC_KEY=...`

**Connector setup and debugging (real issues hit and resolved):**
- Installed via `confluent-hub install snowflakeinc/snowflake-kafka-connector` into the same `connect` container already running Debezium and the (unused) BigQuery plugin
- First registration attempt failed: `com.snowflake.kafka.connector.SnowflakeSinkConnector` does not exist in v4 of the connector — the actual class is `SnowflakeStreamingSinkConnector`, found directly from the connector's own error message listing available plugins
- Second failure: `value.converter` needed to be `org.apache.kafka.connect.json.JsonConverter`, not a Snowflake-specific class that doesn't exist
- Third failure: `snowflake.private.key must be non-empty` — this connector version requires the key **content** inline in the config (not a file path), unlike Debezium/BigQuery's pattern
- Fourth failure: several `snowflake.streaming.*` compatibility-check settings are mandatory in v4 unless explicitly disabled (`snowflake.streaming.validate.compatibility.with.classic=false`) or set (`snowflake.role.name`) — again, the error message enumerated every missing value precisely
- **A real security incident, caught and remediated**: because this connector requires the private key inline, an early debugging step printed the full connector registration response — which included the private key — directly in a chat/terminal log. Recognized immediately, the key pair was treated as compromised: the old key was revoked (removed), a fresh key pair was generated, and the new public key was reattached to the Snowflake user via `ALTER USER`, closing the exposure before the connector was ever used with production-shaped data. All subsequent registrations used the `${SNOWFLAKE_PRIVATE_KEY}` env-var substitution pattern with output suppressed (`curl -o /dev/null -w "%{http_code}"`) to prevent recurrence.
- **A permissions gap after success**: once the connector was running and writing data, querying the auto-created table `CDC_RAW.INVOICES."cdc.public.invoices"` failed with an access-control error — the table was owned by `KAFKA_CONNECTOR_ROLE`, and even the `ACCOUNTADMIN` role couldn't see it without an explicit `GRANT SELECT`, illustrating how Snowflake's role-based access is enforced even against admin-level accounts by default.

**Result — verified with real data**: rows inserted into Postgres are visible end-to-end in Snowflake within seconds, with `rowsInsertedCount` incrementing and `rowsErrorCount=0` in the connector's own status logs, and confirmed directly via `SELECT` against the Snowflake table showing the full Debezium event payload (`RECORD_METADATA`, `SCHEMA`, `PAYLOAD` columns, Snowflake's default ingestion schema for schemaless JSON).

**Closing the validation gap**: the connector originally consumed directly from the raw `cdc.public.invoices` topic, meaning Snowflake received every event — including schema-broken ones — with no protection at all, unlike ClickHouse. Rather than writing Snowflake-specific validation logic, the connector was repointed to consume from `cdc.public.invoices.valid` instead (deleting and re-registering it with an updated `topics` config). This gave Snowflake the same protection as ClickHouse for free, since both now only ever see Gatekeeper-approved events. Verified by inspecting the raw Kafka topic directly (`kafka-console-consumer`) to confirm the split was working, then confirming the connector's own channel logs referenced the `.valid` topic by name post-switch. Along the way, a legitimate column (`tax_amount`, added earlier during schema-drift testing) was being incorrectly flagged as "unexpected" on every event because it had never been added to the Gatekeeper's `EXPECTED_FIELDS` — fixed by adding it with a type that allows `null`.

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
| Sink (working, matches JD) | Snowflake (official Kafka Connector, Snowpipe Streaming, key-pair auth) |
| Schema validation | Standalone Python service (`kafka-python` + `clickhouse-connect` + `kafka-python` producer), always-on container; republishes to `cdc.public.invoices.valid` / `.invalid` Kafka topics so both sinks share one validation checkpoint |
| Transformation | dbt-core + dbt-clickhouse, containerized |
| Orchestration | Apache Airflow 2.9.3 (standalone mode, `DockerOperator`), containerized |
| Reconciliation | Standalone Python service comparing Postgres source vs. ClickHouse destination counts |
| Infra orchestration | Docker Compose, all long-running services `restart: unless-stopped` |
| Environment | GitHub Codespaces / local Docker (Ubuntu) — pipeline verified portable across both |
| Planned but not yet implemented | dbt model + tests targeting the Snowflake table directly (currently dbt only targets ClickHouse) |

## Result

The pipeline runs continuously and unattended, end to end, feeding two independent, verified sinks from a single validation checkpoint. A row inserted into Postgres — clean or schema-broken — is captured by Debezium, streamed through Kafka, and evaluated once by the Gatekeeper, which sorts it into a validated or invalid Kafka topic. Both ClickHouse and Snowflake consume only from the validated topic, so neither ever sees schema-broken data. Airflow's scheduled dbt run rebuilds and re-tests the ClickHouse-side current-state table automatically, with a reconciliation check confirming source/destination counts match.

## BigQuery Attempt (documented, not abandoned lightly)

Writes to BigQuery failed with `Access Denied: BigQuery: Streaming insert is not allowed in the free tier` — a Google Cloud billing policy restriction on Sandbox/free-tier projects, which also blocks provisioning the GCS bucket needed for the batch-load alternative. Diagnosis was confirmed via connector logs, validating every upstream layer was functioning correctly right up to the final write call. The sink was switched to self-hosted ClickHouse, and later, Snowflake was added as the JD-matching production target.

## Room for Improvement / Next Steps

1. **Point dbt at Snowflake too**: currently dbt only transforms the ClickHouse-side data; adding a Snowflake target/profile would let the same `dim_invoices_current` logic run against the JD's actual warehouse.
2. **Formalize the Gatekeeper's schema contract**: move `EXPECTED_FIELDS` out of a hardcoded Python dict into a versioned config (e.g. JSON Schema).
3. **Persistent volumes**: Postgres, Kafka, ClickHouse, and Airflow currently run without persistent Docker volumes, so state is lost on full recreation — observed and worked around directly multiple times, including across a full migration from GitHub Codespaces to a local machine and back.
4. **Secrets handling maturity**: connector/service configs reference values via `.env` (git-ignored); the Snowflake private key required extra care beyond `.env` alone given the key-exposure incident — the next step for full production-readiness would be a proper secrets manager (e.g. HashiCorp Vault, cloud KMS) rather than plain environment variables, and a documented key-rotation runbook.
5. **The invalid topic is currently a dead end**: `cdc.public.invoices.invalid` is written to but nothing consumes it yet beyond the Gatekeeper's own ClickHouse quarantine write — a small consumer or alert could surface these to a human for review.

## Key Lessons (for interview discussion)

- CDC's core idea — read the database's change log instead of polling/batch-querying — is universal across databases, but each database has its own knob determining how much detail is captured on UPDATE/DELETE (Postgres: `REPLICA IDENTITY`; MySQL: `binlog_format=ROW`; SQL Server: native CDC capture instances).
- Every warehouse vendor has its own authentication model for automated services, and they are not interchangeable: ClickHouse used simple username/password; BigQuery used a service-account JSON key; Snowflake's Kafka Connector required RSA key-pair authentication specifically, with the private key needed inline in config (not as a file reference) — a real constraint that shaped how secrets had to be handled.
- Schema mismatch protection has (at least) three distinct failure modes, each needing separate handling: **missing/renamed fields**, **type changes**, and **unexpected new fields** — a presence-only check misses the second and third categories entirely.
- Silent failure is the real danger in schema drift, not crashes: raw JSON-extraction functions often return an empty value on a missing field by default rather than erroring.
- A quarantine pattern (route invalid records to a separate table with full raw payload + reason) preserves both auditability and uptime.
- Validation is cheaper to build once, upstream, than per-destination: republishing validated events into their own Kafka topic (rather than teaching every sink its own validation rules) meant a second consumer (Snowflake) could be added later and inherit full protection with zero new validation code — the fix was a one-line config change (`topics`), not new logic.
- Role-based access in a real warehouse applies even to admin accounts by default: a table created by a service role was invisible to `ACCOUNTADMIN` until an explicit `GRANT SELECT` was issued — a good illustration of least-privilege design working as intended, not a bug.
- **Handling a real credential exposure**: when a private key was inadvertently displayed in a debugging session, the correct response was immediate rotation — revoke the exposed key, generate a fresh pair, reattach the new public key, and change tooling (output suppression) to prevent recurrence — rather than assuming a low-actual-risk situation meant no action was needed. Documented here deliberately, since recognizing and correctly responding to this kind of incident is itself a relevant, assessable skill.
- Connector configuration errors from mature, well-maintained plugins (Snowflake's, in this case) tend to be genuinely actionable — the exact missing/invalid config keys were enumerated directly in the error response, in contrast to vaguer failures seen with less mature tooling.
- Docker containers with no persistent volumes lose all state on recreation; this was hit repeatedly — including during a full project migration from GitHub Codespaces (after exhausting its free storage quota) to a local machine, and back again once the Codespace quota reset — and worked around each time by re-registering connectors and recreating source data from committed setup scripts.
