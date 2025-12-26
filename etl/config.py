import os
import sys
import boto3
from pyspark.sql import SparkSession

def get_spark_session(app_name):
    # --- System Environment Setup ---
    os.environ['JAVA_HOME'] = '/opt/java-11'
    os.environ.pop('SPARK_HOME', None)
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # --- AWS Credentials ---
    session = boto3.Session()
    credentials = session.get_credentials().get_frozen_credentials()

    # --- Jar Management ---
    jar_base = "/run/media/jeremymccormick/ssd-storage/drought-etl/spark-jars"
    jars = [
        f"{jar_base}/hadoop-aws-3.3.4.jar",
        f"{jar_base}/aws-java-sdk-bundle-1.12.262.jar",
        f"{jar_base}/iceberg-spark-runtime-3.5_2.12-1.4.3.jar",
        f"{jar_base}/iceberg-aws-bundle-1.4.3.jar"
    ]

    # --- Spark Session Builder ---
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "2g") \
        .config("spark.jars", ",".join(jars)) \
        .config("spark.hadoop.fs.s3a.access.key", credentials.access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", credentials.secret_key) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://drought-data-lake/iceberg-warehouse") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()