from minio import Minio
import json
import io

minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin123",
    secure=False
)

BUCKET_NAME = "bronze"

bad_records = [
    {
        "equipment_id": "XYZ-999",  # invalid ID format
        "equipment_type": "tow_tractor",
        "timestamp": "2026-08-05T00:30:00+00:00",
        "engine_hours": 1200.0,
        "temperature_c": 75.0,
        "fault_code": None
    },
    {
        "equipment_id": "TOW-001",
        "equipment_type": "tow_tractor",
        "timestamp": "2026-08-05T00:30:03+00:00",
        "engine_hours": 1200.0,
        "temperature_c": 999.9,  # out of range
        "fault_code": None
    },
    {
        "equipment_id": "BELT-001",
        # equipment_type missing on purpose
        "timestamp": "2026-08-05T00:30:06+00:00",
        "engine_hours": 1200.0,
        "temperature_c": 75.0,
        "fault_code": None
    }
]

for i, record in enumerate(bad_records):
    filename = f"TEST-BAD-RECORD-{i}.json"
    data_bytes = json.dumps(record).encode('utf-8')
    data_stream = io.BytesIO(data_bytes)

    minio_client.put_object(
        BUCKET_NAME,
        filename,
        data_stream,
        length=len(data_bytes),
        content_type="application/json"
    )
    print(f"Injected bad record: {filename}")

print("Done injecting test records.")