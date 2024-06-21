import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from scripts.elt_pipeline import extract_load_to_datalake, transform_data
from scripts.convert_to_delta import main_convert

default_args = {
    "owner": "t.nhatnguyen",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("elt_pipeline", start_date=datetime(2024, 1, 1), schedule=None, default_args=default_args) as dag:

    start_pipeline = DummyOperator(
        task_id="start_pipeline"
    )

    extract_load = PythonOperator(
        task_id="extract_load",
        python_callable=extract_load_to_datalake
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data
    )

    convert_to_delta = PythonOperator(
        task_id="convert_to_delta",
        python_callable=main_convert
    )

    end_pipeline = DummyOperator(
        task_id="end_pipeline"
    )

start_pipeline >> extract_load >> transform >> convert_to_delta >> end_pipeline