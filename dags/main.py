from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data,save_to_json

from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality

# Define the local timezone
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
staging_schema = "staging"
core_schema = "core"

#Define default arguments for the DAG
default_args = {
    'owner': 'dataengineers',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'emails': 'data@engineers.com',
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
    'dagrun_timeout': timedelta(hours=1),
    'start_date': datetime(2026, 1, 1, tzinfo=local_tz),
    #'end_date': datetime(2024, 6, 30)
}

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='A DAG to extract video data from YouTube and save it as JSON',
    schedule='0 14 * * *',  # Run daily at 14:00 (2 PM) local time
    catchup=False
) as dag:
    #define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(video_data)
    
    #define dependencies
    playlist_id >> video_ids >> video_data >> save_to_json_task

with DAG(
    dag_id='update_db',
    default_args=default_args,
    description='A DAG to update the staging and core tables in the data warehouse',
    schedule='0 15 * * *',  # Run daily at 15:00 (3 PM) local time
    catchup=False
) as dag:
    #define tasks
    update_staging = staging_table()
    update_core = core_table()
    
    #define dependencies
    update_staging >> update_core
    

with DAG(
    dag_id='data_quality',
    default_args=default_args,
    description='A DAG to check data quality using Soda',
    schedule='0 16 * * *',  # Run daily at 16:00 (4 PM) local time
    catchup=False
) as dag:
    #define tasks
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)
    
    #define dependencies
    soda_validate_staging >> soda_validate_core