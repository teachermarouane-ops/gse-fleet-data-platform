from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timezone

# Connect to Kafka running on localhost:9092
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Simulated fleet: a few units of each equipment type
FLEET = [
    {"equipment_id": "TOW-001", "equipment_type": "tow_tractor"},
    {"equipment_id": "TOW-002", "equipment_type": "tow_tractor"},
    {"equipment_id": "BELT-001", "equipment_type": "belt_loader"},
    {"equipment_id": "BELT-002", "equipment_type": "belt_loader"},
    {"equipment_id": "GPU-001", "equipment_type": "gpu"},
]

FAULT_CODES = [None, None, None, None, "OVERHEAT", "LOW_OIL", "SENSOR_ERROR"]
# None appears more often, so faults are the exception, not the rule

def generate_reading(equipment):
    return {
        "equipment_id": equipment["equipment_id"],
        "equipment_type": equipment["equipment_type"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "engine_hours": round(random.uniform(500, 5000), 1),
        "temperature_c": round(random.uniform(60, 95), 1),
        "fault_code": random.choice(FAULT_CODES)
    }

print("Starting sensor simulation... (Ctrl+C to stop)")

try:
    while True:
        equipment = random.choice(FLEET)
        reading = generate_reading(equipment)
        producer.send('gse-telemetry', value=reading)
        print(f"Sent: {reading}")
        time.sleep(3)  # simulate a new reading every 3 seconds

except KeyboardInterrupt:
    print("\nStopping simulation...")
    producer.flush()
    producer.close()