SELECT 
    HOUR(tpep_pickup_datetime) as pickup_hour,
    PULocationID,
    COUNT(*) as hourly_trips
FROM delta.`s3a://lakehouse/silver/taxi_rides_cleaned`
GROUP BY 1, 2\n