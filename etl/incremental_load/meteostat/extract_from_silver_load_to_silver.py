import logging
import gc
import awswrangler as wr
import pandas as pd
from datetime import datetime, timedelta
from meteostat import Point, Daily, units
from pyspark.sql import functions as F

Point.radius = 100000

from etl.config import get_spark_session

logger = logging.getLogger(__name__)



def extract_silver_data_to_dataframe(query, db, output):
    try:
        logger.info(f"Executing query against database: {db}")
        df = wr.athena.read_sql_query(sql=query, database=db, s3_output=output, ctas_approach=False)
        logger.info(f"Query successful: {len(df)} rows returned")
        return df
    except Exception as e:
        logger.error(f"Error executing athena query: {e}", exc_info=True)
        raise


def extract_weather_data(df, start_date, end_date):
    data_list = []
    logger.info(f"Extracting weather: {start_date.date()} for {len(df)} counties")

    for index, row in df.iterrows():
        try:
            point = Point(row['lat'], row['lon'])
            daily_data = Daily(point, start_date, end_date).convert(units.imperial).fetch()

            if daily_data.empty:
                continue

            df_weather = daily_data.reset_index()
            df_weather = df_weather[df_weather['time'].dt.date == start_date.date()]

            if df_weather.empty:
                continue

            df_weather['county_name'] = row['county_name']
            data_list.append(df_weather)

        except Exception as e:
            logger.error(f"Error getting weather data for {row['county_name']}: {e}")
            continue

    result = pd.concat(data_list, ignore_index=True) if data_list else pd.DataFrame()
    logger.info(f"Total weather records collected: {len(result)}")
    return result


def transform_weather_data(df):
    if df.empty:
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

    return df


def load_to_iceberg(spark, df, database, table_name):
    try:
        logger.info("Starting Iceberg load process")
        cols = df.select_dtypes(exclude=['datetime64[ns]', 'object']).columns
        df[cols] = df[cols].apply(pd.to_numeric, errors='coerce').astype(float)

        spark_df = spark.createDataFrame(df)
        for field in spark_df.schema.fields:
            if field.dataType.typeName() in ("double", "float"):
                c = field.name
                spark_df = spark_df.withColumn(
                    c, F.nanvl(F.col(c), F.lit(None).cast(field.dataType))
                )
        del df
        gc.collect()

        spark_df = spark_df.withColumn("year", F.year("date"))
        spark_df = spark_df.repartition("county_name", "year") \
            .sortWithinPartitions("county_name", "year")

        spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{database}")
        full_table_path = f"glue_catalog.{database}.{table_name}"

        if spark.catalog.tableExists(full_table_path):
            dates_df = spark_df.select("date").distinct().collect()
            dates = [str(row['date']) for row in dates_df]
            date_list = "','".join(dates)
            spark.sql(f"DELETE FROM {full_table_path} WHERE date IN ('{date_list}')")
            spark_df.writeTo(full_table_path).using("iceberg").append()
        else:
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
    spark = None
    try:
        spark = get_spark_session("weather-incremental")

        target_day = (datetime.now() - timedelta(days=14)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        df_coords = extract_silver_data_to_dataframe(
            query="SELECT * FROM ca_counties",
            db="ca_drought_rain",
            output="s3://drought-data-lake/athena-query-results/",
        )

        weather_df = extract_weather_data(df_coords, target_day, target_day)

        if weather_df.empty:
            logger.info("No weather rows this run; nothing to load.")
            return

        final_df = transform_weather_data(weather_df)
        load_to_iceberg(spark, final_df, "ca_drought_rain", "ca_historic_weather")
        logger.info("✓ Incremental load completed successfully")

    except Exception as e:
        logger.error(f"✗ Incremental load FAILED: {e}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    run_incremental_load()