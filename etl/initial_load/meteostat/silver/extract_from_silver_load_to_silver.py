import sys
import os

sys.path.append('/run/media/jeremymccormick/ssd-storage/drought-etl/etl')

import logging
import gc
import awswrangler as wr
import pandas as pd
from datetime import datetime, timedelta
from meteostat import Point, Daily, units
from pyspark.sql import functions as F
from pathlib import Path

from config import get_spark_session

root_dir = Path(__file__).parent.parent.parent.parent.parent



#logging config
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler(root_dir / 'logs/initial-load-weather.log'),
        logging.StreamHandler()
    ]
)

#spark config
spark = get_spark_session("silver-transformation-iceberg")

#params for query
start = datetime(2000, 1, 1)
end = datetime.now() - timedelta(days=7)


# extract functions
def extract_silver_data_to_dataframe(query, db, output) :
    '''
    Description: our CA shapefile has the boundries and the center lat, lon of each county. We are pulling that data to get the weather data.
    :param query: sql query from the athena db
    :param db: the database you are pulling from
    :param output: the output directory of your athena query
    :return: dataframe
    '''
    try:
        logging.info(f"executing query against database: {db}")
        df = wr.athena.read_sql_query(sql=query, database=db, s3_output=output, ctas_approach=False)
        logging.info(f"query successful: {len(df)} rows returned")
        return df
    except Exception as e:
        logging.error(f"error executing athena query: {e}", exc_info=True)
        raise


def extract_weather_data(df, start_date, end_date):
    '''
    Description: Extracting data from the meteostat python library. Looping through the coordinates of the dataframe.
    :param df: dataframe we are using to get coordinates from
    :param start_date: date of the historic weather data you want to start from
    :param end_date: date of the historic weather data you want to end from
    :return: dataframe with historical weather
    '''
    data_list = []
    logging.info("looping through weather data")
    for index, row in df.iterrows():
        try:
            point = Point(row['lat'], row['lon'])
            daily_data = Daily(point, start_date, end_date).convert(units.imperial).fetch()

            if daily_data.empty:
                continue

            df_weather = daily_data.reset_index()
            df_weather['county_name'] = row['county_name']
            data_list.append(df_weather)
        except Exception as e:
            logging.error(f"Error for getting weather data for {row['county_name']}: {e}")
            continue

    return pd.concat(data_list, ignore_index=True) if data_list else pd.DataFrame()

#transform fuction
def transform_weather_data(df):
    '''
    Description: transforming the meteostat data to make the naming convention better and partition it for iceberg.
    :param df: dataframe you want to transform
    :return: clean dataframe
    '''
    if df.empty:
        return df

    df = df.rename(columns={
        df.columns[0]: "date",
        "tavg": "temperature_average",
        "tmin": "temperature_min",
        "tmax": "temperature_max",
        "prcp": "precipitation",
        "wdir": "wind_direction",
        "wspd": "wind_speed",
        "wpgt": "peak_wind_gust",
        "pres": "pressure",
        "tsun": "daily_sun_minutes"
    })

    df['date'] = pd.to_datetime(df['date'])
    days_to_subtract = (df['date'].dt.dayofweek - 1) % 7
    df['map_date'] = df['date'] - pd.to_timedelta(days_to_subtract, unit='d')

    df['date'] = df['date'].dt.date
    df['map_date'] = df['map_date'].dt.date

    return df


def load_to_iceberg(df, database, table_name):
    '''
    Description: loading the transformed data into iceberg table
    :param df: dataframe we are loading
    :param database: the database we are loading
    :param table_name: what the table we are loading
    '''
    try:
        logging.info("Starting Iceberg Load process...")

        # Cleanup numeric types
        cols = df.select_dtypes(exclude=['datetime64[ns]', 'object']).columns
        df[cols] = df[cols].apply(pd.to_numeric, errors='coerce').astype(float)

        spark_df = spark.createDataFrame(df)
        for field in spark_df.schema.fields:
            if field.dataType.typeName() in ("double", "float"):
                c = field.name
                spark_df = spark_df.withColumn(
                    c, F.nanvl(F.col(c), F.lit(None).cast(field.dataType))
                )

        # Memory Management: wipe Pandas immediately
        del df
        gc.collect()

        # Add Year for partitioning
        spark_df = spark_df.withColumn("year", F.year("date"))

        # Optimize physical data layout
        spark_df = spark_df.repartition("county_name", "year") \
            .sortWithinPartitions("county_name", "year")

        logging.info(f"Creating database glue_catalog.{database} if not exists")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{database}")

        # Write to Iceberg
        full_table_path = f"glue_catalog.{database}.{table_name}"
        logging.info(f"Writing data to {full_table_path}...")

        spark_df.writeTo(full_table_path) \
            .using("iceberg") \
            .partitionedBy("county_name", "year") \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("write.spark.fanout.enabled", "true") \
            .createOrReplace()

        logging.info("Write complete.")

    except Exception as e:
        logging.error(f"Failed to load to Iceberg: {e}", exc_info=True)


# Run ETL
if __name__ == "__main__":
    query = "SELECT * FROM ca_counties"
    df_coords = extract_silver_data_to_dataframe(
        query=query,
        db='ca_drought_rain',
        output='s3://drought-data-lake/athena-query-results/'
    )

    weather_df = extract_weather_data(df_coords, start, end)
    final_df = transform_weather_data(weather_df)

    if not final_df.empty:
        print(final_df.head())
        load_to_iceberg(final_df, 'ca_drought_rain', 'ca_historic_weather')