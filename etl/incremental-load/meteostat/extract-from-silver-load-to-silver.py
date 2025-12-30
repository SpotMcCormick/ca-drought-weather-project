import logging
import gc
import awswrangler as wr
import pandas as pd
from datetime import datetime, timedelta
from meteostat import Point, Daily, units
from pyspark.sql import functions as F

from etl.config import get_spark_session

# Get logger (Airflow handles configuration)
logger = logging.getLogger(__name__)


def extract_silver_data_to_dataframe(query, db, output):
    '''
    Description: Get county coordinates from silver layer
    :param query: sql query from the athena db
    :param db: the database you are pulling from
    :param output: the output directory of your athena query
    :return: dataframe
    '''
    try:
        logger.info(f"Executing query against database: {db}")
        df = wr.athena.read_sql_query(sql=query, database=db, s3_output=output, ctas_approach=False)
        logger.info(f"Query successful: {len(df)} rows returned")
        return df
    except Exception as e:
        logger.error(f"Error executing athena query: {e}", exc_info=True)
        raise


def extract_weather_data(df, start_date, end_date):
    '''
    Description: Extract weather data from meteostat
    :param df: dataframe with county coordinates
    :param start_date: start date for weather data
    :param end_date: end date for weather data
    :return: dataframe with historical weather
    '''
    data_list = []
    logger.info(f"Extracting weather: {start_date.date()} to {end_date.date()} for {len(df)} counties")

    for index, row in df.iterrows():
        try:
            point = Point(row['lat'], row['lon'])
            daily_data = Daily(point, start_date, end_date).convert(units.imperial).fetch()

            if daily_data.empty:
                logger.warning(f"No data returned for {row['county_name']}")
                continue

            df_weather = daily_data.reset_index()
            df_weather['county_name'] = row['county_name']
            data_list.append(df_weather)

        except Exception as e:
            logger.error(f"Error getting weather data for {row['county_name']}: {e}")
            continue

    result = pd.concat(data_list, ignore_index=True) if data_list else pd.DataFrame()
    logger.info(f"Total weather records collected: {len(result)}")
    return result


def transform_weather_data(df):
    '''
    Description: Transform meteostat data
    :param df: dataframe to transform
    :return: clean dataframe
    '''
    if df.empty:
        logger.warning("Empty dataframe received for transformation")
        return df

    logger.info("Transforming weather data")

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

    logger.info(f"Transformation complete: {len(df)} records")
    return df


def load_to_iceberg(spark, df, database, table_name):
    '''
    Description: Load data to Iceberg with idempotency
    :param spark: active spark session
    :param df: dataframe to load
    :param database: database name
    :param table_name: table name
    '''
    try:
        logger.info("Starting Iceberg load process")

        # Cleanup numeric types
        cols = df.select_dtypes(exclude=['datetime64[ns]', 'object']).columns
        df[cols] = df[cols].apply(pd.to_numeric, errors='coerce').astype(float)

        # Convert to Spark
        spark_df = spark.createDataFrame(df)
        del df
        gc.collect()

        # Add Year for partitioning
        spark_df = spark_df.withColumn("year", F.year("date"))
        spark_df = spark_df.repartition("county_name", "year") \
            .sortWithinPartitions("county_name", "year")

        logger.info(f"Creating database glue_catalog.{database} if not exists")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{database}")

        full_table_path = f"glue_catalog.{database}.{table_name}"
        table_exists = spark.catalog.tableExists(full_table_path)

        if table_exists:
            # Delete dates we're loading to prevent duplicates
            dates_df = spark_df.select("date").distinct().collect()
            dates = [str(row['date']) for row in dates_df]
            date_list = "','".join(dates)

            logger.info(f"Deleting existing records for {len(dates)} dates to prevent duplicates")
            spark.sql(f"DELETE FROM {full_table_path} WHERE date IN ('{date_list}')")

            # Append
            logger.info(f"Appending to {full_table_path}")
            spark_df.writeTo(full_table_path).using("iceberg").append()
        else:
            # Create table
            logger.info(f"Creating new table {full_table_path}")
            spark_df.writeTo(full_table_path) \
                .using("iceberg") \
                .partitionedBy("county_name", "year") \
                .tableProperty("write.format.default", "parquet") \
                .tableProperty("write.spark.fanout.enabled", "true") \
                .create()

        logger.info("Iceberg load complete")

    except Exception as e:
        logger.error(f"Failed to load to Iceberg: {e}", exc_info=True)
        raise


def run_incremental_load():
    '''Orchestrate the incremental load'''
    spark = None

    try:
        logger.info("=" * 50)
        logger.info("Starting weather incremental load")
        logger.info("=" * 50)

        # Create spark session
        spark = get_spark_session("weather-incremental")
        logger.info("Spark session created")

        # Calculate dates (7 days ago for reliability)
        start = datetime.now() - timedelta(days=7)
        end = datetime.now() - timedelta(days=7)

        logger.info(f"Loading weather for: {start.date()} to {end.date()}")

        # Extract county coordinates
        query = "SELECT * FROM ca_counties"
        df_coords = extract_silver_data_to_dataframe(
            query=query,
            db='ca_drought_rain',
            output='s3://drought-data-lake/athena-query-results/'
        )
        logger.info(f"Got {len(df_coords)} counties")

        # Extract weather data
        weather_df = extract_weather_data(df_coords, start, end)

        if weather_df.empty:
            logger.error("No weather data collected - aborting")
            return

        logger.info(f"Extracted {len(weather_df)} weather records")

        # Transform
        final_df = transform_weather_data(weather_df)
        logger.info(f"Transformed {len(final_df)} records")

        # Load to Iceberg
        load_to_iceberg(spark, final_df, 'ca_drought_rain', 'ca_historic_weather')

        logger.info("✓ Incremental load completed successfully")

    except Exception as e:
        logger.error(f"✗ Incremental load FAILED: {e}", exc_info=True)
        raise

    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    run_incremental_load()