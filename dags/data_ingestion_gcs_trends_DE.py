import os
import logging

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage

import pyarrow.csv as pv
import pyarrow.parquet as pq

import pandas as pd

from pytrends.request import TrendReq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

SEARCH_TERM = 'hörbuch'
GEO='DE'
TIMEFRAME='now 1-d'
HOST_LANGUAGE='de-'+ GEO

date = '{{ execution_date.strftime(\'%Y%m%d\') }}'
local_path_template = AIRFLOW_HOME + f'/{date}'
gcs_path_template = f'/raw/{date}/'


def download_trends(hl: str, local_csv: str, date: str):
    pytrend = TrendReq(hl=HOST_LANGUAGE)

    #get interest by region for your search terms
    pytrend.build_payload(kw_list=[SEARCH_TERM], timeframe=TIMEFRAME)

    # interest by region
    df = pytrend.interest_by_region(resolution='COUNTRY', inc_low_vol=False, inc_geo_code=True).reset_index()

    # Add date and export to .csv
    df['date'] = date
    df.to_csv(local_csv + GEO + "_trends.csv", sep = ';', index = False)

    # Related Topics
    related_topics = pytrend.related_topics()

    r = related_topics[SEARCH_TERM]['rising']
    r['results_type'] = 'rising'

    t = related_topics[SEARCH_TERM]['top']
    t['results_type'] = 'top'

    cols = ['topic_title', 'value','results_type']
    df = pd.concat([r[cols],t[cols]])
    df['date'] = date
    df.to_csv(local_csv + GEO + "_related_topics.csv", sep = ";", index = False)


    # Related Queries
    related_queries = pytrend.related_queries()

    r = related_queries[SEARCH_TERM]['rising']
    r['results_type'] = 'rising'

    t = related_queries[SEARCH_TERM]['top']
    t['results_type'] = 'top'

    cols = ['query', 'value', 'results_type']
    df = pd.concat([r[cols],t[cols]])
    df['date'] = date
    df.to_csv(local_csv + GEO + "_related_queries.csv", sep = ";", index = False)


def batch_to_parquet(date):
    filenames = [ filename for filename in os.listdir() if filename.endswith( '.csv' ) and filename.startswith(date) ]
    if len(filenames) == 0:
        logging.error("CSV files not found")
        return

    for f in filenames:
        table = pv.read_csv(f, parse_options=pv.ParseOptions(delimiter=";"))
        pq.write_table(table, f[:-3]+'parquet')


def upload_to_gcs(bucket, destination_object_name, date):
    client = storage.Client()
    bucket = client.bucket(bucket)

    filenames = [ filename for filename in os.listdir() if filename.endswith( '.parquet' ) and filename.startswith(date) ]
    if len(filenames) == 0:
        logging.error("PARQUET files not found")
        return

    for f in filenames:
        blob = bucket.blob(destination_object_name + f)
        blob.upload_from_filename(AIRFLOW_HOME + '/' + f) #TODO create distinct folders GCSToGCSOperator()

default_args = {
    "owner": "airflow",
    "start_date": days_ago(15),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_" + GEO,
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-project'],
) as dag:

    download_trends_task = PythonOperator(
        task_id="download_trends",
        python_callable=download_trends,
        op_kwargs={"hl": HOST_LANGUAGE,
                   "date": date,
                   "local_csv": local_path_template
                   },
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=batch_to_parquet,
        op_kwargs={"date": date
                   },
        )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "destination_object_name": gcs_path_template,
            "date": date,
        },
    )


    download_trends_task >> format_to_parquet_task >> local_to_gcs_task
