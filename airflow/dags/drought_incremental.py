from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "jeremy",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

PROJECT_DIR = "/opt/project"

with DAG(
    dag_id="gis_monthly",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 3 1 * *",  # 1st of the month
    catchup=False,
    tags=["incremental", "gis", "monthly"],
) as gis_monthly:
    gis_extract = BashOperator(
        task_id="gis_extract_bronze",
        bash_command=f"cd {PROJECT_DIR} && python etl/incremental_load/gis-data-ca/extract-load-bronze.py",
    )

    gis_transform = BashOperator(
        task_id="gis_transform_silver",
        bash_command=f"cd {PROJECT_DIR} && python etl/incremental_load/gis-data-ca/transform-load-silver.py",
    )

    gis_extract >> gis_transform


with DAG(
    dag_id="drought_monitor_friday",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 4 * * 5",  # every friday
    catchup=False,
    tags=["incremental", "drought-monitor", "weekly"],
) as drought_monitor_friday:
    drought_extract = BashOperator(
        task_id="drought_extract_bronze",
        bash_command=f"cd {PROJECT_DIR} && python etl/incremental_load/us-drought-monitor/extract-load-bronze.py",
    )

    drought_transform = BashOperator(
        task_id="drought_transform_silver",
        bash_command=f"cd {PROJECT_DIR} && python etl/incremental_load/us-drought-monitor/transform-load-to-silver.py",
    )

    drought_extract >> drought_transform


with DAG(
    dag_id="meteostat_daily",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 5 * * *",  # daily 
    catchup=False,
    tags=["incremental", "meteostat", "daily"],
) as meteostat_daily:
    meteostat_task = BashOperator(
        task_id="meteostat_extract_transform_load",
        bash_command=f"cd {PROJECT_DIR} && python etl/incremental_load/meteostat/extract_from_silver_load_to_silver.py",
    )