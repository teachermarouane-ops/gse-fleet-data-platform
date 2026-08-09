import sys
from pyspark.sql.functions import to_timestamp, col
import os

# Reuse the existing Spark session setup instead of duplicating MinIO config
sys.path.insert(0, os.path.dirname(__file__))
from bronze_to_silver import create_spark_session, BRONZE_PATH

from soda.scan import Scan

def run_bronze_quality_scan():
    spark = create_spark_session()

    # Read the same bronze data bronze_to_silver.py reads
    df = spark.read.json(BRONZE_PATH)
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
    df.createOrReplaceTempView("bronze_telemetry")

    scan = Scan()
    scan.set_scan_definition_name("bronze_telemetry_quality_gate")
    scan.set_data_source_name("spark_df")
    scan.add_spark_session(spark, data_source_name="spark_df")

    # Load checks from the standalone data contract file instead of an inline string
    contract_path = os.path.join(os.path.dirname(__file__), "checks", "bronze_contract.yml")
    scan.add_sodacl_yaml_file(contract_path)

    scan.execute()

    print(scan.get_logs_text())
    print("\n--- CHECK RESULTS ---")
    print(scan.get_scan_results())


    scan_failed = scan.has_check_fails()
    spark.stop()

    if scan_failed:
        sys.exit(1)

if __name__ == "__main__":
    run_bronze_quality_scan()
