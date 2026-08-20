CREATE TABLE IF NOT EXISTS cdc_invoices_kafka
(
    raw_message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'cdc.public.invoices',
    kafka_group_name = 'clickhouse_invoices_group',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS cdc_invoices_raw
(
    raw_message String,
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY ingested_at;

CREATE MATERIALIZED VIEW IF NOT EXISTS cdc_invoices_mv TO cdc_invoices_raw AS
SELECT raw_message, now() AS ingested_at
FROM cdc_invoices_kafka;
