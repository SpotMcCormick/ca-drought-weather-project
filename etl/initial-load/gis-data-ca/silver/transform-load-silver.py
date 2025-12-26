import sys
import logging
import zipfile
import tempfile
import boto3
import geopandas as gpd
from pathlib import Path

# --- THE PATH FIX ---
sys.path.append('/run/media/jeremymccormick/ssd-storage/drought-etl/etl')

from config import get_spark_session

# --- Logging Setup ---
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    filename='/run/media/jeremymccormick/ssd-storage/drought-etl/logs/ca-gis-county-log',
    level=logging.INFO
)

# --- AWS & Spark Initialization ---
spark = get_spark_session("silver-transformation-iceberg-gis")

s3 = boto3.client('s3')
aws_bucket = 'drought-data-lake'
prefix = 'gis-ca-county/initial-load/bronze/data/'


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
        s3_key = max_obj['Key']

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)
            zip_path = temp_dir_path / "shapefile.zip"

            s3.download_file(aws_bucket, s3_key, str(zip_path))

            extract_dir = temp_dir_path / "extracted"
            extract_dir.mkdir()

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)

            shp_file = list(extract_dir.glob("**/*.shp"))[0]
            gdf = gpd.read_file(shp_file)
            return gdf

    except Exception as e:
        logging.error(f"error extracting bronze: {e}", exc_info=True)
        return None


def transform_silver(gdf):
    '''
    Description: transforms geodataframe to keep only specific columns
    '''
    if gdf is None: return None
    try:
        column_mapping = {
            'NAMELSAD': 'county_name',
            'INTPTLAT': 'lat',
            'INTPTLON': 'lon'
        }
        df = gdf.rename(columns=column_mapping)
        df = df[list(column_mapping.values())]
        df['lat'] = df['lat'].astype(float)
        df['lon'] = df['lon'].astype(float)
        return df
    except Exception as e:
        logging.error(f"error transforming data: {e}", exc_info=True)
        return None


def load_to_iceberg(df, database, table_name):
    '''
    Description: loads dataframe into iceberg table using glue catalog
    '''
    if df is None: return False
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{database}")
        full_table_name = f"glue_catalog.{database}.{table_name}"

        spark_df = spark.createDataFrame(df)

        spark_df.writeTo(full_table_name) \
            .using("iceberg") \
            .createOrReplace()

        logging.info(f"successfully loaded {full_table_name}")
        return True
    except Exception as e:
        logging.error(f"error loading to iceberg: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    gdf_bronze = extract_bronze()
    df_silver = transform_silver(gdf_bronze)
    load_to_iceberg(df_silver, 'ca_drought_rain', 'ca_counties')
    spark.stop()