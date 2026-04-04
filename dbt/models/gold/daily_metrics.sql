SELECT 
    DATE(tpep_pickup_datetime) as pickup_date,
    COUNT(*) as total_trips,
    SUM(Fare_amount) as total_revenue
FROM delta.`s3a://lakehouse/silver/taxi_rides_cleaned`
GROUP BY 1\n