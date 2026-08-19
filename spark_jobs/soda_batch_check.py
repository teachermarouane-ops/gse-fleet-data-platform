import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from bronze_to_silver import create_spark_session

from soda.scan import Scan

TECHNICIAN_LOGS_PATH = "s3a://technician-logs/"
SCHEDULING_PATH = "s3a://scheduling/"


def run_batch_quality_scan():
    spark = create_spark_session()

    tech_df = spark.read.json(TECHNICIAN_LOGS_PATH)
    tech_df.createOrReplaceTempView("technician_logs")

    sched_df = spark.read.json(SCHEDULING_PATH)
    sched_df.createOrReplaceTempView("scheduling")

    scan = Scan()
    scan.set_scan_definition_name("batch_data_quality_gate")
    scan.set_data_source_name("spark_df")
    scan.add_spark_session(spark, data_source_name="spark_df")

    contract_path = os.path.join(os.path.dirname(__file__), "checks", "batch_contract.yml")
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
    run_batch_quality_scan()
