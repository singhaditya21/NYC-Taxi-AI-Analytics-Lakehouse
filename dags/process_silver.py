from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG('process_silver', schedule_interval='@daily', start_date=datetime(2025,1,1), catchup=False) as dag:
    silver_task = SparkSubmitOperator(
        task_id='run_spark_silver',
        conn_id='spark_default',
        application='/opt/airflow/spark_jobs/silver_transform.py',
        packages='io.delta:delta-spark_2.12:3.2.0'
    )\n