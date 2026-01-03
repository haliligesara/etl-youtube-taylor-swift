from airflow import DAG
import pendulum

from datetime import datetime, timedelta

from api.video_stats import (
    get_playlist_id,
    get_video_ids,
    extract_video_data,
    save_to_json
)

from datawharehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality

# Define the local timezone
local_tz = pendulum.timezone("Europe/Berlin")

# Default Args
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
}

staging_schema = "staging"
core_schema = "core"

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='DAG to produce json file with raw data',
    schedule_interval='0 14 * * *',     # ✔ correct for most Airflow versions
    catchup=False,
    max_active_runs=1,                  # ✔ moved out of default_args
    dagrun_timeout=timedelta(hours=1),  # ✔ moved out of default_args
) as dag:

    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    playlist_id >> video_ids >> extract_data >> save_to_json_task

with DAG(
    dag_id='update_db',
    default_args=default_args,
    description='DAG to process JSON file and insert data to both staging and core schema',
    schedule_interval='0 15 * * *',     # ✔ correct for most Airflow versions
    catchup=False,
    max_active_runs=1,                  # ✔ moved out of default_args
    dagrun_timeout=timedelta(hours=1),  # ✔ moved out of default_args
) as dag:
    
    update_staging = staging_table()
    update_core = core_table()

    update_staging >> update_core

with DAG(
    dag_id='data_quality',
    default_args=default_args,
    description='DAG to check the data quality on both layers in the db',
    schedule_interval='0 16 * * *',     # ✔ correct for most Airflow versions
    catchup=False,
    max_active_runs=1,                  # ✔ moved out of default_args
    dagrun_timeout=timedelta(hours=1),  # ✔ moved out of default_args
) as dag:
    
    soda_validat_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    soda_validat_staging >> soda_validate_core