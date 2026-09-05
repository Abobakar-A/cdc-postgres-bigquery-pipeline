import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

CLICKHOUSE_HOST = os.environ['CLICKHOUSE_HOST']
CLICKHOUSE_USER = os.environ['CLICKHOUSE_USER']
CLICKHOUSE_PASSWORD = os.environ['CLICKHOUSE_PASSWORD']
POSTGRES_HOST = os.environ['POSTGRES_HOST']
POSTGRES_DB = os.environ['POSTGRES_DB']
POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
SNOWFLAKE_ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
SNOWFLAKE_USER = os.environ['SNOWFLAKE_USER']
SNOWFLAKE_PRIVATE_KEY = os.environ['SNOWFLAKE_PRIVATE_KEY']
SNOWFLAKE_ROLE = os.environ['SNOWFLAKE_ROLE']
SNOWFLAKE_DATABASE = os.environ['SNOWFLAKE_DATABASE']
SNOWFLAKE_WAREHOUSE = os.environ['SNOWFLAKE_WAREHOUSE']
SNOWFLAKE_SCHEMA = os.environ['SNOWFLAKE_SCHEMA']

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dbt_mount = [
    Mount(source='/workspaces/cdc-postgres-bigquery-pipeline/dbt_project',
          target='/app/dbt_project', type='bind')
]

dbt_docker_args = dict(
    image='cdc-postgres-bigquery-pipeline-dbt',
    docker_url='unix://var/run/docker.sock',
    network_mode='cdc-postgres-bigquery-pipeline_default',
    auto_remove=True,
    environment={
        'CLICKHOUSE_HOST': CLICKHOUSE_HOST,
        'CLICKHOUSE_USER': CLICKHOUSE_USER,
        'CLICKHOUSE_PASSWORD': CLICKHOUSE_PASSWORD,
    },
    mounts=dbt_mount,
)

dbt_snowflake_docker_args = dict(
    image='cdc-postgres-bigquery-pipeline-dbt',
    docker_url='unix://var/run/docker.sock',
    network_mode='cdc-postgres-bigquery-pipeline_default',
    auto_remove=True,
    environment={
        'SNOWFLAKE_ACCOUNT': SNOWFLAKE_ACCOUNT,
        'SNOWFLAKE_USER': SNOWFLAKE_USER,
        'SNOWFLAKE_PRIVATE_KEY': SNOWFLAKE_PRIVATE_KEY,
        'SNOWFLAKE_ROLE': SNOWFLAKE_ROLE,
        'SNOWFLAKE_DATABASE': SNOWFLAKE_DATABASE,
        'SNOWFLAKE_WAREHOUSE': SNOWFLAKE_WAREHOUSE,
        'SNOWFLAKE_SCHEMA': SNOWFLAKE_SCHEMA,
    },
    mounts=dbt_mount,
)

with DAG(
    dag_id='invoice_pipeline',
    default_args=default_args,
    description='Run dbt transform, tests, and reconciliation for invoice CDC pipeline (ClickHouse + Snowflake)',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=['dbt', 'cdc', 'invoices'],
) as dag:

    dbt_run = DockerOperator(
        task_id='dbt_run',
        command='dbt run --profiles-dir . --select dim_invoices_current',
        **dbt_docker_args,
    )

    dbt_test = DockerOperator(
        task_id='dbt_test',
        command='dbt test --profiles-dir . --select dim_invoices_current',
        **dbt_docker_args,
    )

    reconciliation = DockerOperator(
        task_id='reconciliation',
        image='cdc-postgres-bigquery-pipeline-reconciliation',
        docker_url='unix://var/run/docker.sock',
        network_mode='cdc-postgres-bigquery-pipeline_default',
        auto_remove=True,
        environment={
            'POSTGRES_HOST': POSTGRES_HOST,
            'POSTGRES_DB': POSTGRES_DB,
            'POSTGRES_USER': POSTGRES_USER,
            'POSTGRES_PASSWORD': POSTGRES_PASSWORD,
            'CLICKHOUSE_HOST': CLICKHOUSE_HOST,
            'CLICKHOUSE_USER': CLICKHOUSE_USER,
            'CLICKHOUSE_PASSWORD': CLICKHOUSE_PASSWORD,
        },
    )

    dbt_run_snowflake = DockerOperator(
        task_id='dbt_run_snowflake',
        command='dbt run --profiles-dir . --target snowflake --select dim_invoices_current_snowflake',
        **dbt_snowflake_docker_args,
    )

    dbt_test_snowflake = DockerOperator(
        task_id='dbt_test_snowflake',
        command='dbt test --profiles-dir . --target snowflake --select dim_invoices_current_snowflake',
        **dbt_snowflake_docker_args,
    )

    dbt_run >> dbt_test >> reconciliation
    dbt_run_snowflake >> dbt_test_snowflake
