import json
import io
import random
from datetime import datetime, timedelta
from minio import Minio

# --- Connect to MinIO (same setup as consumer_to_minio.py) ---
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin123",
    secure=False
)

EQUIPMENT_IDS = [
    "TOW-001", "TOW-002", "BELT-001", "BELT-002", "GPU-001"
]

TECHNICIANS = ["TECH-007", "TECH-014", "TECH-021", "TECH-033"]

ACTIONS = ["filter_replacement", "inspection", "oil_change", "belt_tension_check", "battery_check"]

NOTES = [
    "No abnormal wear observed",
    "Minor oil leak detected, flagged for follow-up",
    "Completed without issues",
    "Replaced worn component",
    "Requires further monitoring"
]

TASK_TYPES = ["routine_inspection", "engine_service", "brake_check", "tire_rotation", "software_update"]
STATUSES = ["pending", "confirmed", "completed", "cancelled"]


def ensure_bucket(bucket_name):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")


def upload_json(bucket_name, filename, record):
    data_bytes = json.dumps(record).encode("utf-8")
    data_stream = io.BytesIO(data_bytes)
    minio_client.put_object(
        bucket_name,
        filename,
        data_stream,
        length=len(data_bytes),
        content_type="application/json"
    )
    print(f"Saved to {bucket_name}: {filename}")


def generate_technician_logs(count=15):
    ensure_bucket("technician-logs")
    for _ in range(count):
        equipment_id = random.choice(EQUIPMENT_IDS)
        timestamp = (datetime.utcnow() - timedelta(hours=random.randint(0, 72))).isoformat() + "+00:00"
        record = {
            "technician_id": random.choice(TECHNICIANS),
            "equipment_id": equipment_id,
            "action": random.choice(ACTIONS),
            "timestamp": timestamp,
            "notes": random.choice(NOTES)
        }
        filename = f"{equipment_id}_{timestamp.replace(':', '-')}.json"
        upload_json("technician-logs", filename, record)


def generate_scheduling(count=15):
    ensure_bucket("scheduling")
    for _ in range(count):
        equipment_id = random.choice(EQUIPMENT_IDS)
        scheduled_date = (datetime.utcnow() + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d")
        record = {
            "equipment_id": equipment_id,
            "scheduled_date": scheduled_date,
            "task_type": random.choice(TASK_TYPES),
            "status": random.choice(STATUSES)
        }
        filename = f"{equipment_id}_{scheduled_date}_{random.randint(1000,9999)}.json"
        upload_json("scheduling", filename, record)


if __name__ == "__main__":
    print("Generating technician logs...")
    generate_technician_logs()
    print("\nGenerating scheduling data...")
    generate_scheduling()
    print("\nDone.")
