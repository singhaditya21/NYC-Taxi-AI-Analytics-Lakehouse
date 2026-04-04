from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Bronze Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Mocking ingestion of Parquet files
df = spark.createDataFrame([
    (1, "2025-01-01 10:00:00", 1, 132, 236, 12.5),
    (2, "2025-01-01 10:05:00", 2, 161, 237, 25.0)
], ["VendorID", "tpep_pickup_datetime", "Passenger_count", "PULocationID", "DOLocationID", "Fare_amount"])

df.write.format("delta").mode("append").save("s3a://lakehouse/bronze/taxi_rides")\n