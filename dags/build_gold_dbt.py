from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('build_gold_dbt', schedule_interval='@daily', start_date=datetime(2025,1,1), catchup=False) as dag:
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir . --project-dir .'
    )\n