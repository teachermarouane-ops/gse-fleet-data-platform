from kafka import KafkaConsumer
from minio import Minio
import json
import io
from datetime import datetime

# --- Connect to Kafka ---
consumer = KafkaConsumer(
    'gse-telemetry',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# --- Connect to MinIO ---
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin123",
    secure=False  # local dev, no HTTPS
)

BUCKET_NAME = "bronze"

print("Listening for telemetry messages... (Ctrl+C to stop)")

try:
    for message in consumer:
        reading = message.value

        # Build a unique filename: equipment_id + timestamp
        equipment_id = reading["equipment_id"]
        timestamp = reading["timestamp"].replace(":", "-")  # avoid colons in filenames
        filename = f"{equipment_id}_{timestamp}.json"

        # Convert the reading to bytes (MinIO needs a byte stream)
        data_bytes = json.dumps(reading).encode('utf-8')
        data_stream = io.BytesIO(data_bytes)

        # Upload to MinIO
        minio_client.put_object(
            BUCKET_NAME,
            filename,
            data_stream,
            length=len(data_bytes),
            content_type="application/json"
        )

        print(f"Saved to MinIO: {filename}")

except KeyboardInterrupt:
    print("\nStopping consumer...")