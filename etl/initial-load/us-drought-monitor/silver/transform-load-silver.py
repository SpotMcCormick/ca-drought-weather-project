import sys
import logging
import boto3

# --- THE PATH FIX ---
# Adjust this to point to the 'etl' folder where your config.py lives
sys.path.append('/run/media/jeremymccormick/ssd-storage/drought-etl/etl')

from config import get_spark_session

from pyspark.sql.functions import col, to_date, year

# --- Logging Setup ---
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    filename='../../../../logs/initial-load.log',  # Keeps your existing log path
    level=logging.INFO
)

# --- AWS & Spark Initialization ---
# This one call replaces all the os.environ, boto3 credential fetching, and Spark builder blocks
spark = get_spark_session("silver-transformation-iceberg-drought")

# These stay for the S3 client extraction
s3 = boto3.client('s3')
aws_bucket = 'drought-data-lake'
prefix = 'drought-us-monitor/initial-load/bronze/data/'


# --- ETL Functions ---

def extract_bronze():
    '''
    Description: extracting the bronze raw data from the s3 bucket
    '''
    try:
        logging.info("getting data from s3 bucket")
        response = s3.list_objects_v2(Bucket=aws_bucket, Prefix=prefix)

        if 'Contents' not in response:
            logging.error(f"No files found at {prefix}")
            raise ValueError("Bronze layer is empty")

        logging.info("getting most recent file")
        max_obj = max(response['Contents'], key=lambda x: x['LastModified'])

        # We use s3a:// for Spark to read from S3
        s3_path = f"s3a://{aws_bucket}/{max_obj['Key']}"
        logging.info(f"making {s3_path} into a spark dataframe")

        df = spark.read.json(s3_path)
        return df
    except Exception as e:
        logging.error(f"error with extraction: {e}", exc_info=True)
        return None


def transform_s3_data(df):
    '''
    Description: Transforming data of our json
    '''
    if df is None: return None
    try:
        logging.info("transforming the data")
        df_transformed = df \
            .withColumn("map_date", to_date(col("mapDate"))) \
            .withColumn("year", year(col("map_date"))) \
            .withColumn("dsci", col("dsci").cast("integer")) \
            .withColumn("fips", col("fips").cast("string")) \
            .dropna() \
            .select("county", "state", "fips", "dsci", "map_date", "year")
        logging.info("data transformed")
        return df_transformed
    except Exception as e:
        logging.error("no data to transform", exc_info=True)
        return None


def load_to_iceberg(df, database, table_name):
    '''
    Description: loading the data to an iceberg table format in aws for analytics
    '''
    if df is None: return
    try:
        logging.info(f"Ensuring database glue_catalog.{database} exists")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{database}")

        full_table_path = f"glue_catalog.{database}.{table_name}"
        logging.info(f"Writing data to {full_table_path}")

        # Note: Added fanout.enabled just like the weather job for stability
        df.writeTo(full_table_path) \
            .using("iceberg") \
            .partitionedBy("year") \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("write.spark.fanout.enabled", "true") \
            .createOrReplace()

        logging.info("Done loading to Iceberg")
    except Exception as e:
        logging.error("error on creating db/writing table", exc_info=True)


# --- Execution ---
if __name__ == "__main__":
    df_bronze = extract_bronze()
    df_silver = transform_s3_data(df_bronze)

    if df_silver:
        load_to_iceberg(df_silver, "ca_drought_rain", "ca_county_drought")

    spark.stop()