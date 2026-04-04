from kafka import KafkaProducer
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
    time.sleep(2)\n