from kafka import KafkaConsumer
import json
import clickhouse_connect
from datetime import datetime

# The fields we EXPECT, and the Python type we expect each one to be
EXPECTED_FIELDS = {
    'invoice_id': int,
    'customer_name': str,
    'status': str,
}

client = clickhouse_connect.get_client(
    host='clickhouse',
    port=8123,
    username='default',
    password='clickhouse123'
)

consumer = KafkaConsumer(
    'cdc.public.invoices',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Gatekeeper listening for invoice changes...")

for message in consumer:
    event = message.value
    after = event['payload']['after']
    op = event['payload']['op']

    problems = []

    for field, expected_type in EXPECTED_FIELDS.items():
        if field not in after:
            problems.append(f"missing field: {field}")
        elif not isinstance(after[field], expected_type):
            actual_type = type(after[field]).__name__
            problems.append(f"wrong type for {field}: expected {expected_type.__name__}, got {actual_type}")

    if problems:
        reason = "; ".join(problems)
        print(f"QUARANTINED invoice_id={after.get('invoice_id')} — {reason}")
        client.insert(
            'invoices_quarantine',
            [[after.get('invoice_id', 0), json.dumps(event), reason, datetime.now()]],
            column_names=['invoice_id', 'raw_message', 'reason', 'event_ts']
        )
    else:
        print(f"OK invoice_id={after['invoice_id']} customer={after['customer_name']}")
        client.insert(
            'invoices_typed',
            [[after['invoice_id'], after['customer_name'], after['status'], op, datetime.now()]],
            column_names=['invoice_id', 'customer_name', 'status', 'op', 'event_ts']
        )