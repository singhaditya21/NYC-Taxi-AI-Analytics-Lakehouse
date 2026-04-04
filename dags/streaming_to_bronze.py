from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG('streaming_to_bronze', schedule_interval='@once', start_date=datetime(2025, 1, 1), catchup=False) as dag:
    streaming_task = SparkSubmitOperator(
        task_id='run_kafka_consumer',
        conn_id='spark_default',
        application='/opt/airflow/streaming/consumer_spark.py',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0'
    )\n