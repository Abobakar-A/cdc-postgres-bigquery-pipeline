{{ config(materialized='table') }}
WITH parsed_events AS (
    SELECT
        PAYLOAD:after:invoice_id::INT AS invoice_id,
        PAYLOAD:after:customer_name::STRING AS customer_name,
        PAYLOAD:after:status::STRING AS status,
        PAYLOAD:op::STRING AS op,
        RECORD_METADATA:CreateTime::BIGINT AS event_ts
    FROM {{ source('snowflake_raw', 'cdc_public_invoices_valid') }}
),

ranked_events AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY event_ts DESC) AS rn
    FROM parsed_events
)

SELECT
    invoice_id,
    customer_name,
    status,
    op,
    event_ts AS last_updated_at
FROM ranked_events
WHERE rn = 1
