from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import subprocess
import sys

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='weather_pipeline',
    default_args=default_args,
    description='Hourly weather data pipeline - ingestion + dbt',
    schedule_interval='@hourly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['weather', 'dbt', 'snowflake'],
) as dag:

    ingest_weather_data = BashOperator(
        task_id='ingest_weather_data',
        bash_command='cd /opt/airflow/ingestion && python ingest.py',
    )

    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command='cd /opt/airflow/dbt_weather && dbt seed --profiles-dir /opt/airflow',
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_weather && dbt run --profiles-dir /opt/airflow',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_weather && dbt test --profiles-dir /opt/airflow',
    )

    dbt_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command='cd /opt/airflow/dbt_weather && dbt docs generate --profiles-dir /opt/airflow',
    )

    ingest_weather_data >> dbt_seed >> dbt_run >> dbt_test >> dbt_docs