import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trends')

date = '{{ execution_date.strftime(\'%Y%m%d\') }}'

TYPE_FILES = {'queries': '_related_queries', 'topics': '_related_topics', 'trends': '_trends'}
INPUT_PART = "raw"
INPUT_FILETYPE = "parquet"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="gcs_to_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-project'],
) as dag:

    for types, extension in TYPE_FILES.items():
        move_files_gcs_task = GCSToGCSOperator(
            task_id=f'move_files_task_{types}',
            source_bucket=BUCKET,
            source_object=f'/{INPUT_PART}/{date}/*{extension}.{INPUT_FILETYPE}',
            destination_bucket=BUCKET,
            destination_object=f'{types}/',
            move_object=False,
                    )
        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bq_external_table_task_{types}",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{types}_external_table",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                    "sourceUris": [f"gs://{BUCKET}/{types}/*"],
                },
            },
        )

        move_files_gcs_task  >> bigquery_external_table_task
