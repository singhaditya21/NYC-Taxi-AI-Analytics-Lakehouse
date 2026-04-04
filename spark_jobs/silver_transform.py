from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Silver Transform").getOrCreate()

# Read from bronze
df = spark.read.format("delta").load("s3a://lakehouse/bronze/taxi_rides")

# Clean & Enrich
df_clean = df.filter(col("Fare_amount") > 0).filter(col("Passenger_count").isNotNull())

df_clean.write.format("delta").mode("overwrite").save("s3a://lakehouse/silver/taxi_rides_cleaned")\n