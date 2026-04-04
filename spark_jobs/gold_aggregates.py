from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg

spark = SparkSession.builder.appName("Gold Aggregates").getOrCreate()

df = spark.read.format("delta").load("s3a://lakehouse/silver/taxi_rides_cleaned")

agg_df = df.groupBy("PULocationID").agg(
    sum("Fare_amount").alias("total_revenue"),
    avg("Fare_amount").alias("avg_fare")
)

agg_df.write.format("delta").mode("overwrite").save("s3a://lakehouse/gold/aggregates")\n