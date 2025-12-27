import sys
sys.path.append('/run/media/jeremymccormick/ssd-storage/drought-etl/etl')

import logging
import zipfile
import tempfile
import boto3
import geopandas as gpd
from pathlib import Path


from config import get_spark_session

# logging config
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    filename='../../../../logs/ca-gis-county-log',
    level=logging.INFO
)

# spark donfig
spark = get_spark_session("silver-transformation-iceberg-gis")

#aws config
s3 = boto3.client('s3')
aws_bucket = 'drought-data-lake'
prefix = 'gis-ca-county/initial-load/bronze/data/'



#extract functions
def extract_bronze():
    '''
    Description: extracting the bronze raw data from the s3 bucket and turning the shapefile into a geopandas dataframe
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

#transform function
def transform_silver(gdf):
    '''
    Description: transforms geopandas dataframe to keep only specific columns as well as making better namming conventions
    :param gdf: the dataframe to transform
    :return: clean dataframe
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

#load function
def load_to_iceberg(df, database, table_name):
    '''
    Description: loading the transformed data into iceberg table
    :param df: dataframe we are loading
    :param database: the database we are loading
    :param table_name: what the table we are loading
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

#example useage
if __name__ == "__main__":
    gdf_bronze = extract_bronze()
    df_silver = transform_silver(gdf_bronze)
    load_to_iceberg(df_silver, 'ca_drought_rain', 'ca_counties')
    spark.stop()