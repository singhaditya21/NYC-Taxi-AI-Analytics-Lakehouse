SELECT 
    Passenger_count,
    AVG(Fare_amount) as avg_fare,
    MAX(Fare_amount) as max_fare
FROM delta.`s3a://lakehouse/silver/taxi_rides_cleaned`
GROUP BY 1\n