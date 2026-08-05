from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp, datediff, when
import sys

BRONZE_PATH = "s3a://bronze/"
SILVER_PATH = "s3a://silver/valid/"
SILVER_REJECTED_PATH = "s3a://silver/rejected/"

VALID_ID_PATTERN = r"^(TOW|BELT|GPU)-\d{3}$"


def create_spark_session():
    """Creates a Spark session configured to talk to MinIO (S3-compatible storage)."""
    spark = (
        SparkSession.builder
        .appName("BronzeToSilver")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")  # keep output quiet, per our earlier decision
    return spark


def read_bronze_data(spark):
    """Reads all raw JSON telemetry files from the bronze bucket."""
    df = spark.read.json(BRONZE_PATH)
    return df


def validate_and_clean(df):
    """Applies data quality rules, splitting records into valid and rejected."""

    df = df.withColumn("parsed_timestamp", to_timestamp(col("timestamp")))

    df = df.withColumn(
        "is_valid_id", col("equipment_id").rlike(VALID_ID_PATTERN)
    ).withColumn(
        "has_required_fields",
        col("equipment_id").isNotNull()
        & col("timestamp").isNotNull()
        & col("equipment_type").isNotNull()
    ).withColumn(
        "temp_in_range",
        (col("temperature_c") >= 0) & (col("temperature_c") <= 150)
    ).withColumn(
        "hours_in_range",
        (col("engine_hours") >= 0) & (col("engine_hours") <= 50000)
    ).withColumn(
        "has_valid_timestamp", col("parsed_timestamp").isNotNull()
    )

    df = df.withColumn(
        "is_valid",
        col("is_valid_id")
        & col("has_required_fields")
        & col("temp_in_range")
        & col("hours_in_range")
        & col("has_valid_timestamp")
    )

    valid_df = df.filter(col("is_valid") == True).drop(
        "is_valid_id", "has_required_fields", "temp_in_range",
        "hours_in_range", "has_valid_timestamp", "is_valid"
    )

    rejected_df = df.filter(col("is_valid") == False)

    return valid_df, rejected_df


def write_silver_data(valid_df, rejected_df):
    """Writes valid records to silver/valid, rejected ones to silver/rejected."""
    valid_df.write.mode("overwrite").json(SILVER_PATH)
    rejected_df.write.mode("overwrite").json(SILVER_REJECTED_PATH)


def main():
    spark = create_spark_session()

    try:
        bronze_df = read_bronze_data(spark)
        total_count = bronze_df.count()

        valid_df, rejected_df = validate_and_clean(bronze_df)
        valid_count = valid_df.count()
        rejected_count = rejected_df.count()

        write_silver_data(valid_df, rejected_df)

        print(f"Processed {total_count} records: {valid_count} valid, {rejected_count} rejected.")

    except Exception as e:
        print(f"ERROR: pipeline failed - {e}", file=sys.stderr)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()