from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {'owner': 'data_engineer', 'retries': 1}

with DAG('ingest_bronze', default_args=default_args, schedule_interval='@daily', start_date=datetime(2025, 1, 1), catchup=False) as dag:
    ingest_task = SparkSubmitOperator(
        task_id='run_spark_bronze',
        conn_id='spark_default',
        application='/opt/airflow/spark_jobs/bronze_ingest.py',
        packages='io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4'
    )\n