import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from scripts.extract_load import extract_load
from scripts.transform_data import transform_data
from scripts.convert_to_delta import delta_convert

# Default arguments for the DAG
default_args = {
    "owner": "t.nhatnguyen",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

###############################################
# Parameters & Arguments
###############################################
MINIO_ENDPOINT = os.environ['MINIO_ENDPOINT']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']
###############################################


with DAG("elt_pipeline", start_date=datetime(2024, 1, 1), schedule=None, default_args=default_args) as dag:

    start_pipeline = DummyOperator(
        task_id="start_pipeline"
    )

    extract_load = PythonOperator(
        task_id="extract_load",
        python_callable=extract_load,
        op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY}
    )

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY}
    )

    delta_convert = PythonOperator(
        task_id="delta_convert",
        python_callable=delta_convert,
        op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY}
    )

    end_pipeline = DummyOperator(
        task_id="end_pipeline"
    )

start_pipeline >> extract_load >> transform_data >> delta_convert >> end_pipeline
