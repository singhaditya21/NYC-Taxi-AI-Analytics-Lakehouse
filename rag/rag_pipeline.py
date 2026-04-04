from langchain_community.vectorstores import Chroma
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
    build_vector_db()\n