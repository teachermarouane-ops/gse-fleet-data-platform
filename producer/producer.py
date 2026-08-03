from kafka import KafkaProducer
import json
import time

# Connect to Kafka running on localhost:9092
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send one test message
message = {"message": "hello kafka"}
producer.send('test-topic', value=message)

# Make sure the message is actually sent before the script exits
producer.flush()

print(f"Sent: {message}")