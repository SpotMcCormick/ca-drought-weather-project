from etl.spark_config import get_spark_session
import logging
import boto3
from pyspark.sql.functions import col, to_date, year
import yaml 
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[3]

with open(BASE_DIR / 'config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)
 



#logging configs
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler(BASE_DIR / 'logs/incremental-load.log'),
        logging.StreamHandler()
    ]
)

#spark config
spark = get_spark_session("silver-transformation-iceberg-drought")

#aws config
s3 = boto3.client('s3')
aws_bucket= config['aws']['aws_bucket']
aws_key = config['aws']['silver_keys']['us_drought_monitor_input']


#extract function
def extract_bronze():
    '''
    Description: extracting the bronze raw data from the s3 bucket
    '''
    try:
        logging.info("getting data from s3 bucket")
        response = s3.list_objects_v2(Bucket=aws_bucket, Prefix=aws_key)

        if 'Contents' not in response:
            logging.error(f"No files found at {aws_key}")
            raise ValueError("Bronze layer is empty")

        logging.info("getting most recent file")
        max_obj = max(response['Contents'], key=lambda x: x['LastModified'])

        s3_path = f"s3a://{aws_bucket}/{max_obj['Key']}"
        logging.info(f"making {s3_path} into a spark dataframe")

        df = spark.read.json(s3_path)
        return df
    except Exception as e:
        logging.error(f"error with extraction: {e}", exc_info=True)
        return None

#transform function
def transform_s3_data(df):
    '''
    Description: Transforming data of our json
    :param df: json file we are transforming
    :return: transformed dataframe
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

#load function
def load_to_iceberg(df, database, table_name):
    '''
    Description: loading the transformed data into iceberg table
    :param df: dataframe we are loading
    :param database: the database we are loading
    :param table_name: what the table we are loading
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
            .append()

        logging.info("Done appending to Iceberg")
    except Exception as e:
        logging.error("error on creating db/writing table", exc_info=True)


#example usage
if __name__ == "__main__":
    df_bronze = extract_bronze()
    df_silver = transform_s3_data(df_bronze)

    if df_silver:
        load_to_iceberg(df_silver, "ca_drought_rain", "ca_county_drought")

    spark.stop()