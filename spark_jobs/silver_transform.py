from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Silver Transform").getOrCreate()

# Read from bronze
df = spark.read.format("delta").load("s3a://lakehouse/bronze/taxi_rides")

# Read lookup table
zone_path = "/app/data/taxi_zone_lookup.csv"
try:
    df_zones = spark.read.option("header", "true").csv(zone_path)
except Exception as e:
    print(f"Warning: Zone lookup not found at {zone_path}. Error: {e}")
    df_zones = None

# Clean & Enrich
df_clean = df.filter(col("Fare_amount") > 0).filter(col("Passenger_count").isNotNull())

if df_zones:
    df_clean = df_clean.join(df_zones, df_clean.PULocationID == df_zones.LocationID, "left") \
                       .withColumnRenamed("Borough", "PUBorough") \
                       .withColumnRenamed("Zone", "PUZone") \
                       .drop("LocationID", "service_zone")

df_clean.write.format("delta").mode("overwrite").save("s3a://lakehouse/silver/taxi_rides_cleaned")