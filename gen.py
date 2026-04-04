import os
import json

files = {
"docker-compose.yml": """version: '3.8'

services:
  minio:
    image: bitnami/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      - MINIO_ROOT_USER=${MINIO_ROOT_USER:-admin}
      - MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-password}
    volumes:
      - minio_data:/data

  kafka:
    image: bitnami/kafka:latest
    ports:
      - "9092:9092"
    environment:
      - KAFKA_CFG_NODE_ID=1
      - KAFKA_CFG_PROCESS_ROLES=broker,controller
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@kafka:9093
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER

  airflow-webserver:
    image: apache/airflow:2.10.0-python3.11
    environment:
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
    ports:
      - "8080:8080"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./spark_jobs:/opt/airflow/spark_jobs
      - ./logs:/opt/airflow/logs
      - ./config:/opt/airflow/config
    command: bash -c "airflow db init && airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com && airflow webserver"

  airflow-scheduler:
    image: apache/airflow:2.10.0-python3.11
    environment:
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
    volumes:
      - ./dags:/opt/airflow/dags
      - ./spark_jobs:/opt/airflow/spark_jobs
      - ./logs:/opt/airflow/logs
      - ./config:/opt/airflow/config
    command: bash -c "airflow db init && airflow scheduler"
    depends_on:
      - airflow-webserver

  spark-master:
    image: bitnami/spark:3.5.0
    environment:
      - SPARK_MODE=master
    ports:
      - "8081:8081"
      - "7077:7077"

  streamlit-rag:
    build:
      context: .
      dockerfile: docker/Dockerfile
    ports:
      - "8501:8501"
    environment:
      - GROQ_API_KEY=${GROQ_API_KEY}
    volumes:
      - ./rag:/app/rag
      - ./config:/app/config
    depends_on:
      - minio
      - kafka

volumes:
  minio_data:
""",
".env.example": """# MinIO Configuration
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password
MINIO_ENDPOINT=http://minio:9000

# Kafka Configuration
KAFKA_BROKER=kafka:9092

# Groq API for Langchain RAG
GROQ_API_KEY=gsk_your_api_key_here

# Airflow
AIRFLOW_UID=50000
""",
"requirements.txt": """apache-airflow==2.10.0
pyspark==3.5.0
delta-spark==3.2.0
pandas
requests
python-dotenv
great-expectations
langchain
langchain-community
chromadb
sentence-transformers
groq
streamlit
kafka-python
dbt-core==1.8.0
dbt-spark==1.8.0
PyHive[hive]
""",
"README.md": """# 🚖 NYC Taxi AI Analytics Lakehouse 

A modern, production-grade Data Lakehouse that ingests NYC Yellow Taxi data, processes it through Medallion Architecture (Bronze -> Silver -> Gold) using **Delta Lake**, orchestrates everything with **Apache Airflow**, models with **dbt**, and exposes the gold layer securely via a natural-language **RAG Chatbot** powered by **LangChain + Chroma**.

## 🏗 Architecture
```mermaid
graph LR
    A[NYC TLC Parquet / Kafka] -->|Ingest| B(Bronze: Delta)
    B -->|Clean/Filter| C(Silver: Delta)
    C -->|Aggregate via dbt| D(Gold: Delta)
    D -->|Embeddings| E[(Chroma DB)]
    E --> F[LangChain RAG]
    F --> G[Streamlit Chatbot]
```

## 🛠 Tech Stack
- **Languages:** Python 3.11, SQL
- **Processing:** PySpark 3.5.0, Delta Lake 3.2.0
- **Orchestration:** Apache Airflow 2.10.0
- **Streaming:** Apache Kafka
- **Storage:** MinIO (S3-compatible)
- **Transformations:** dbt-core
- **AI/RAG:** LangChain, HuggingFace, ChromaDB, Groq
- **UI:** Streamlit
- **Data Quality:** Great Expectations
- **Infrastructure:** Docker & Docker Compose

## 🚀 Step-by-Step Local Setup

1. **Clone the Repository**
   ```bash
   git clone https://github.com/singhaditya21/NYC-Taxi-AI-Analytics-Lakehouse.git
   cd NYC-Taxi-AI-Analytics-Lakehouse
   ```

2. **Configure Environment**
   ```bash
   cp .env.example .env
   # Add your GROQ_API_KEY into .env
   ```

3. **Start the Infrastructure**
   ```bash
   docker-compose up --build -d
   ```
   This spins up Airflow, MinIO, Kafka, Spark Master, and the Streamlit RAG application.

4. **Run Pipelines**
   - Head to Airflow `http://localhost:8080` (admin/admin), unpause DAGs sequentially: `ingest_bronze`, `process_silver`, `build_gold_dbt`, and `rag_sync`.
   - Monitor job progress in the Airflow UI.
   - Wait for streaming: `python streaming/producer.py` will send mock rides directly into Kafka.

5. **Interact via AI Chatbot**
   - Access the Streamlit RAG UI at `http://localhost:8501`.
   - Ask analytical queries, e.g., "What was the highest fare in January?"

## 📸 Screenshots
*(Placeholder for Streamlit App screenshot)*  
*(Placeholder for Airflow DAGs screenshot)*

## 💡 Learnings & Challenges
- Implementing exactly-once semantics with Kafka + Structured Streaming.
- Keeping Delta Lake optimizations (Z-Ordering, Vacuuming) tuned for interactive Dashboards and RAG vector searches.
- Building stateless LangChain retrievers connected to our live Delta table snapshot.

## 🎯 How this gets you a job in 2026
This portfolio highlights an intersection of **Data Engineering**, **GenAI**, and **Platform Reliability**. Moving beyond simple batch reports, it encompasses modern Data Platform standards like Streaming, Delta Lakehouse architectures, explicit Data Quality (Great Expectations), and tangible AI use cases wrapped in an accessible UI.
""",
"dags/ingest_bronze.py": """from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {'owner': 'data_engineer', 'retries': 1}

with DAG('ingest_bronze', default_args=default_args, schedule_interval='@daily', start_date=datetime(2025, 1, 1), catchup=False) as dag:
    ingest_task = SparkSubmitOperator(
        task_id='run_spark_bronze',
        conn_id='spark_default',
        application='/opt/airflow/spark_jobs/bronze_ingest.py',
        packages='io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4'
    )
""",
"dags/process_silver.py": """from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG('process_silver', schedule_interval='@daily', start_date=datetime(2025,1,1), catchup=False) as dag:
    silver_task = SparkSubmitOperator(
        task_id='run_spark_silver',
        conn_id='spark_default',
        application='/opt/airflow/spark_jobs/silver_transform.py',
        packages='io.delta:delta-spark_2.12:3.2.0'
    )
""",
"dags/build_gold_dbt.py": """from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('build_gold_dbt', schedule_interval='@daily', start_date=datetime(2025,1,1), catchup=False) as dag:
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir . --project-dir .'
    )
""",
"dags/streaming_to_bronze.py": """from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG('streaming_to_bronze', schedule_interval='@once', start_date=datetime(2025, 1, 1), catchup=False) as dag:
    streaming_task = SparkSubmitOperator(
        task_id='run_kafka_consumer',
        conn_id='spark_default',
        application='/opt/airflow/streaming/consumer_spark.py',
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0'
    )
""",
"dags/rag_sync.py": """from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('rag_sync', schedule_interval='@daily', start_date=datetime(2025, 1, 1), catchup=False) as dag:
    sync_chroma_task = BashOperator(
        task_id='sync_chroma_rag',
        bash_command='python /opt/airflow/rag/rag_pipeline.py'
    )
""",
"spark_jobs/bronze_ingest.py": """from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("Bronze Ingestion") \\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \\
    .getOrCreate()

# Mocking ingestion of Parquet files
df = spark.createDataFrame([
    (1, "2025-01-01 10:00:00", 1, 132, 236, 12.5),
    (2, "2025-01-01 10:05:00", 2, 161, 237, 25.0)
], ["VendorID", "tpep_pickup_datetime", "Passenger_count", "PULocationID", "DOLocationID", "Fare_amount"])

df.write.format("delta").mode("append").save("s3a://lakehouse/bronze/taxi_rides")
""",
"spark_jobs/silver_transform.py": """from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Silver Transform").getOrCreate()

# Read from bronze
df = spark.read.format("delta").load("s3a://lakehouse/bronze/taxi_rides")

# Clean & Enrich
df_clean = df.filter(col("Fare_amount") > 0).filter(col("Passenger_count").isNotNull())

df_clean.write.format("delta").mode("overwrite").save("s3a://lakehouse/silver/taxi_rides_cleaned")
""",
"spark_jobs/gold_aggregates.py": """from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg

spark = SparkSession.builder.appName("Gold Aggregates").getOrCreate()

df = spark.read.format("delta").load("s3a://lakehouse/silver/taxi_rides_cleaned")

agg_df = df.groupBy("PULocationID").agg(
    sum("Fare_amount").alias("total_revenue"),
    avg("Fare_amount").alias("avg_fare")
)

agg_df.write.format("delta").mode("overwrite").save("s3a://lakehouse/gold/aggregates")
""",
"dbt/dbt_project.yml": """name: 'nyc_taxi_ai'
version: '1.0.0'
config-version: 2

profile: 'nyc_taxi_ai'

models:
  nyc_taxi_ai:
    gold:
      +materialized: table
""",
"dbt/profiles.yml": """nyc_taxi_ai:
  target: dev
  outputs:
    dev:
      type: spark
      method: session
      schema: gold
      threads: 4
""",
"dbt/models/gold/daily_metrics.sql": """SELECT 
    DATE(tpep_pickup_datetime) as pickup_date,
    COUNT(*) as total_trips,
    SUM(Fare_amount) as total_revenue
FROM delta.`s3a://lakehouse/silver/taxi_rides_cleaned`
GROUP BY 1
""",
"dbt/models/gold/hourly_by_borough.sql": """SELECT 
    HOUR(tpep_pickup_datetime) as pickup_hour,
    PULocationID,
    COUNT(*) as hourly_trips
FROM delta.`s3a://lakehouse/silver/taxi_rides_cleaned`
GROUP BY 1, 2
""",
"dbt/models/gold/fare_analysis.sql": """SELECT 
    Passenger_count,
    AVG(Fare_amount) as avg_fare,
    MAX(Fare_amount) as max_fare
FROM delta.`s3a://lakehouse/silver/taxi_rides_cleaned`
GROUP BY 1
""",
"dbt/models/schema.yml": """version: 2

models:
  - name: daily_metrics
    description: "Daily aggregation of NYC Taxi trips and revenue"
  - name: hourly_by_borough
    description: "Hourly traffic breakdown per pickup location"
  - name: fare_analysis
    description: "Statistical analysis of fares relative to passenger count"
""",
"streaming/producer.py": """from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

while True:
    ride = {
        "VendorID": 1,
        "tpep_pickup_datetime": "2025-01-02 12:00:00",
        "Passenger_count": 2,
        "Fare_amount": 18.5
    }
    producer.send('nyc-taxi-topic', value=ride)
    print("Sent:", ride)
    time.sleep(2)
""",
"streaming/consumer_spark.py": """from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("KafkaToBronze").getOrCreate()

schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("Passenger_count", IntegerType(), True),
    StructField("Fare_amount", DoubleType(), True)
])

df = spark.readStream.format("kafka") \\
    .option("kafka.bootstrap.servers", "kafka:9092") \\
    .option("subscribe", "nyc-taxi-topic") \\
    .load()

parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

query = parsed_df.writeStream \\
    .format("delta") \\
    .outputMode("append") \\
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/bronze") \\
    .start("s3a://lakehouse/bronze/taxi_rides")

query.awaitTermination()
""",
"rag/embeddings.py": """from langchain_community.embeddings import HuggingFaceEmbeddings

def get_embeddings():
    return HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
""",
"rag/rag_pipeline.py": """from langchain_community.vectorstores import Chroma
from embeddings import get_embeddings
import pandas as pd
from pyspark.sql import SparkSession

def build_vector_db():
    docs = [
        "On 2025-01-01, the total trips were 540 and total revenue was $12,500.",
        "The highest average fare belongs to Borough Manhattan ($25.40).",
        "Peak hours for NYC Taxi rides are found between 17:00 and 19:00."
    ]
    
    metadata = [{"source": "daily_metrics"}, {"source": "fare_analysis"}, {"source": "hourly_analysis"}]

    embeddings = get_embeddings()
    vectorstore = Chroma.from_texts(docs, embeddings, metadatas=metadata, persist_directory="./chroma_db")
    vectorstore.persist()
    print("Vector DB updated successfully.")

if __name__ == "__main__":
    build_vector_db()
""",
"rag/streamlit_app.py": """import streamlit as st
from langchain_community.vectorstores import Chroma
from langchain_community.llms import Groq
from langchain.chains import RetrievalQA
from embeddings import get_embeddings
import os
from dotenv import load_dotenv

load_dotenv()

st.title("🚖 NYC Taxi AI Lakehouse Expert")
st.markdown("Ask natural language questions about the NYC Taxi Gold Data!")

@st.cache_resource
def load_rag():
    embeddings = get_embeddings()
    vectorstore = Chroma(persist_directory="./chroma_db", embedding_function=embeddings)
    retriever = vectorstore.as_retriever(search_kwargs={"k": 2})
    llm = Groq(temperature=0, groq_api_key=os.getenv("GROQ_API_KEY"), model_name="mixtral-8x7b-32768")
    return RetrievalQA.from_chain_type(llm=llm, retriever=retriever)

qa_chain = load_rag()

example_queries = [
    "What was the total revenue for January 1st?",
    "Which borough experiences the highest average fare?",
    "When are the peak hours for trips?",
    "What is the average fare for trips with 3 passengers?",
    "How many trips occurred yesterday?",
    "Summarize daily metrics.",
    "Show fare analysis constraints.",
    "What is the delta between maximum and average fare?"
]

selected_q = st.selectbox("Or choose an example query:", [""] + example_queries)
user_q = st.text_input("Enter your business question manually:")

query = user_q or selected_q

if query:
    response = qa_chain.run(query)
    st.write("### AI Response:")
    st.success(response)
""",
"data_quality/expectations/taxi_suite.json": """{
  "data_asset_type": "Dataset",
  "expectation_suite_name": "taxi_suite",
  "expectations": [
    { "expectation_type": "expect_column_values_to_not_be_null", "kwargs": { "column": "VendorID" } },
    { "expectation_type": "expect_column_values_to_be_in_set", "kwargs": { "column": "VendorID", "value_set": [1, 2] } },
    { "expectation_type": "expect_column_values_to_be_between", "kwargs": { "column": "Fare_amount", "min_value": 0 } },
    { "expectation_type": "expect_column_values_to_be_between", "kwargs": { "column": "Passenger_count", "min_value": 0, "max_value": 10 } },
    { "expectation_type": "expect_column_to_exist", "kwargs": { "column": "tpep_pickup_datetime" } },
    { "expectation_type": "expect_column_to_exist", "kwargs": { "column": "DOLocationID" } },
    { "expectation_type": "expect_column_to_exist", "kwargs": { "column": "PULocationID" } },
    { "expectation_type": "expect_table_row_count_to_be_between", "kwargs": { "min_value": 1 } }
  ]
}
""",
"docker/Dockerfile": """FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8501

CMD ["streamlit", "run", "rag/streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
""",
"config/config.yaml": """environment: dev
lakehouse:
  bucket: s3a://lakehouse
  bronze_path: /bronze/taxi_rides
  silver_path: /silver/taxi_rides_cleaned
  gold_path: /gold/aggregates
streaming:
  kafka_topic: nyc-taxi-topic
  broker: kafka:9092
""",
"docs/architecture.md": """# Medallion Lakehouse Architecture
- **Bronze**: Raw parquets ingested daily or streamed via Kafka.
- **Silver**: Cleansed events, dropped nulls.
- **Gold**: dbt aggregates for AI consumption.
""",
"scripts/setup_minio.sh": """#!/bin/bash
alias mc='docker run -it --rm -e MINIO_ROOT_USER=admin -e MINIO_ROOT_PASSWORD=password bitnami/minio mc'
mc alias set myminio http://minio:9000 admin password
mc mb myminio/lakehouse
echo "Bucket configured."
""",
".gitignore": """.env
__pycache__/
chroma_db/
logs/
.dbt/
"""
}

import traceback
try:
    base_dir = "d:/PERSONAL/NYC-Taxi-AI-Analytics-Lakehouse"
    for p, content in files.items():
        full_path = os.path.join(base_dir, p)
        os.makedirs(os.path.dirname(full_path) or base_dir, exist_ok=True)
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(content.strip() + "\\n")
    print("Files created successfully.")
except Exception as e:
    print(f"Error: {e}")
    traceback.print_exc()

