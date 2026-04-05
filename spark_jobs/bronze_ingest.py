from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Bronze Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Ingesting actual Parquet file from local data directory
# In a real environment, this might be heavily partitioned AWS S3 data.
data_path = "/app/data/yellow_tripdata.parquet"

try:
    df = spark.read.parquet(data_path)
    # Write to Bronze Delta Lake
    df.write.format("delta").mode("append").save("s3a://lakehouse/bronze/taxi_rides")
    print("Bronze ingestion complete.")
except Exception as e:
    print(f"Failed to ingest: {e}")