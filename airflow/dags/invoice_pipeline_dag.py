import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

CLICKHOUSE_HOST = os.environ['CLICKHOUSE_HOST']
CLICKHOUSE_USER = os.environ['CLICKHOUSE_USER']
CLICKHOUSE_PASSWORD = os.environ['CLICKHOUSE_PASSWORD']

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

common_docker_args = dict(
    image='cdc-postgres-bigquery-pipeline-dbt',
    docker_url='unix://var/run/docker.sock',
    network_mode='cdc-postgres-bigquery-pipeline_default',
    auto_remove=True,
    environment={
        'CLICKHOUSE_HOST': CLICKHOUSE_HOST,
        'CLICKHOUSE_USER': CLICKHOUSE_USER,
        'CLICKHOUSE_PASSWORD': CLICKHOUSE_PASSWORD,
    },
    mounts=[
        Mount(source='/workspaces/cdc-postgres-bigquery-pipeline/dbt_project',
              target='/app/dbt_project', type='bind')
    ],
)

with DAG(
    dag_id='invoice_pipeline',
    default_args=default_args,
    description='Run dbt transform and tests for invoice CDC pipeline',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=['dbt', 'cdc', 'invoices'],
) as dag:

    dbt_run = DockerOperator(
        task_id='dbt_run',
        command='dbt run --profiles-dir .',
        **common_docker_args,
    )

    dbt_test = DockerOperator(
        task_id='dbt_test',
        command='dbt test --profiles-dir .',
        **common_docker_args,
    )

    dbt_run >> dbt_test
