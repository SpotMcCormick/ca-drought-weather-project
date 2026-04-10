# spark_config.py
import os
import sys
from pyspark.sql import SparkSession
from pathlib import Path

# java and py set up - module level, not inside function
os.environ['JAVA_HOME'] = '/opt/java-11'
os.environ.pop('SPARK_HOME', None)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def get_spark_session(app_name: str) -> SparkSession:
    '''
    Description: Got tired of pasting my spark session params over and over again.
    :param app_name: name you want to call your spark session
    :return: spark session
    '''

    project_root = Path(__file__).resolve().parents[1]  
    jar_base = os.environ.get("SPARK_JAR_DIR", str(project_root / "spark-jars"))
    
    jars = [
        f"{jar_base}/hadoop-aws-3.3.4.jar",
        f"{jar_base}/aws-java-sdk-bundle-1.12.262.jar",
        f"{jar_base}/iceberg-spark-runtime-3.5_2.12-1.4.3.jar",
        f"{jar_base}/iceberg-aws-bundle-1.4.3.jar",
    ]

    spark_conf = {
        "spark.driver.memory": "6g",
        "spark.executor.memory": "6g",
        "spark.memory.offHeap.enabled": "true",
        "spark.memory.offHeap.size": "2g",
        "spark.jars": ",".join(jars),
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.warehouse": "s3://drought-data-lake/iceberg-warehouse",
        "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    }

    builder = SparkSession.builder.appName(app_name)
    for key, val in spark_conf.items():
        builder = builder.config(key, val)

    return builder.getOrCreate()