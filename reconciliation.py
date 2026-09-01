import os
import psycopg2
import clickhouse_connect

# Connect to Postgres (source)
pg_conn = psycopg2.connect(
    host=os.environ['POSTGRES_HOST'],
    port=5432,
    dbname=os.environ['POSTGRES_DB'],
    user=os.environ['POSTGRES_USER'],
    password=os.environ['POSTGRES_PASSWORD']
)
pg_cursor = pg_conn.cursor()

# Connect to ClickHouse (destination)
ch_client = clickhouse_connect.get_client(
    host=os.environ['CLICKHOUSE_HOST'],
    port=8123,
    username=os.environ['CLICKHOUSE_USER'],
    password=os.environ['CLICKHOUSE_PASSWORD']
)

# Count check
pg_cursor.execute("SELECT COUNT(*) FROM invoices")
pg_count = pg_cursor.fetchone()[0]

ch_result = ch_client.query("SELECT COUNT(*) FROM cdc_raw.dim_invoices_current")
ch_count = ch_result.result_rows[0][0]

print(f"Postgres source count:          {pg_count}")
print(f"ClickHouse current-state count: {ch_count}")

if pg_count == ch_count:
    print("RECONCILIATION PASSED — counts match.")
else:
    diff = pg_count - ch_count
    print(f"RECONCILIATION FAILED — mismatch of {diff} record(s).")
    exit(1)
