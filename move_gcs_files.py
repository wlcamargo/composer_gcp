from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

default_args = {
    'owner': 'Wallace Camargo',
    'start_date': datetime(2024, 2, 23),
    'schedule_interval': '@daily'
}

with DAG(
        dag_id = 'move_gcs_files',
        default_args =default_args,
        schedule_interval=None,
        catchup=False,
        tags =['gcs', 'youtube']
) as dag:
    
    move_files_to_bkp = GCSToGCSOperator(
        task_id='move_files_to_bkp',
        source_bucket='staging-youtube',
        source_object='*',
        destination_bucket = 'processed-files-youtube',
        destination_object='',
        move_object = True
    )

    move_files_to_bkp