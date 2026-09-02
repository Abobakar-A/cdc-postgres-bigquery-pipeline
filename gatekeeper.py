import os
from kafka import KafkaConsumer, KafkaProducer
import json
import clickhouse_connect
from datetime import datetime

KAFKA_BOOTSTRAP_SERVERS = os.environ['KAFKA_BOOTSTRAP_SERVERS']
CLICKHOUSE_HOST = os.environ['CLICKHOUSE_HOST']
CLICKHOUSE_PORT = int(os.environ.get('CLICKHOUSE_PORT', 8123))
CLICKHOUSE_USER = os.environ['CLICKHOUSE_USER']
CLICKHOUSE_PASSWORD = os.environ['CLICKHOUSE_PASSWORD']

VALID_TOPIC = 'cdc.public.invoices.valid'
INVALID_TOPIC = 'cdc.public.invoices.invalid'

EXPECTED_FIELDS = {
    'invoice_id': int,
    'customer_name': str,
    'status': str,
    'amount': dict,
    'updated_at': int,
    'due_date': int,
    'tax_amount': (dict, type(None)),
}

client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD
)

consumer = KafkaConsumer(
    'cdc.public.invoices',
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
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
            if isinstance(expected_type, tuple):
                expected_name = " or ".join(t.__name__ for t in expected_type)
            else:
                expected_name = expected_type.__name__
            problems.append(f"wrong type for {field}: expected {expected_name}, got {actual_type}")

    unexpected_fields = [f for f in after.keys() if f not in EXPECTED_FIELDS]
    if unexpected_fields:
        problems.append(f"unexpected new field(s): {unexpected_fields}")

    if problems:
        reason = "; ".join(problems)
        print(f"QUARANTINED invoice_id={after.get('invoice_id')} — {reason}")
        client.insert(
            'invoices_quarantine',
            [[after.get('invoice_id', 0), json.dumps(event), reason, datetime.now()]],
            column_names=['invoice_id', 'raw_message', 'reason', 'event_ts']
        )
        producer.send(INVALID_TOPIC, value=event)
    else:
        print(f"OK invoice_id={after['invoice_id']} customer={after['customer_name']}")
        client.insert(
            'invoices_typed',
            [[after['invoice_id'], after['customer_name'], after['status'], op, datetime.now()]],
            column_names=['invoice_id', 'customer_name', 'status', 'op', 'event_ts']
        )
        producer.send(VALID_TOPIC, value=event)

    producer.flush()
