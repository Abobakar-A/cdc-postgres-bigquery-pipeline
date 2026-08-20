WITH ranked_events AS (
    SELECT
        invoice_id,
        customer_name,
        status,
        op,
        event_ts,
        ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY event_ts DESC) AS rn
    FROM {{ source('cdc_raw', 'invoices_typed') }}
)

SELECT
    invoice_id,
    customer_name,
    status,
    op,
    event_ts AS last_updated_at
FROM ranked_events
WHERE rn = 1
