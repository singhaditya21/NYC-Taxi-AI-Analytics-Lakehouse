from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('rag_sync', schedule_interval='@daily', start_date=datetime(2025, 1, 1), catchup=False) as dag:
    sync_chroma_task = BashOperator(
        task_id='sync_chroma_rag',
        bash_command='python /opt/airflow/rag/rag_pipeline.py'
    )\n