import logging
import zipfile
import tempfile
import boto3
import geopandas as gpd
from pathlib import Path
import yaml
from etl.spark_config import get_spark_session

logging.basicConfig(level=logging.INFO) 

BASE_DIR = Path(__file__).resolve().parents[3]

# spark donfig
spark = get_spark_session("silver-transformation-iceberg-gis")

with open(BASE_DIR / 'config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)
logger = logging.getLogger(__name__)

#aws config
s3 = boto3.client('s3')
aws_bucket= config['aws']['aws_bucket']
aws_key = config['aws']['bronze_keys']['gis_silver_input']



#extract functions
def extract_bronze():
    '''
    Description: extracting the bronze raw data from the s3 bucket and turning the shapefile into a geopandas dataframe
    '''
    try:
        logger.info("getting data from s3 bucket")
        response = s3.list_objects_v2(Bucket=aws_bucket, Prefix=aws_key)

        if 'Contents' not in response:
            logger.error(f"No files found at {prefix}")
            raise ValueError("Bronze layer is empty")

        logger.info("getting most recent file")
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
        logger.error(f"error extracting bronze: {e}", exc_info=True)
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
        logger.error(f"error transforming data: {e}", exc_info=True)
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

        logger.info(f"successfully loaded {full_table_name}")
        return True
    except Exception as e:
        logger.error(f"error loading to iceberg: {e}", exc_info=True)
        return False

#usage
if __name__ == "__main__":
    gdf_bronze = extract_bronze()
    df_silver = transform_silver(gdf_bronze)
    load_to_iceberg(df_silver, 'ca_drought_rain', 'ca_counties')
    spark.stop()