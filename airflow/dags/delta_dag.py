import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from scripts.convert_to_delta import main_convert

# Default arguments for the DAG
default_args = {
    "owner": "t.nhatnguyen",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("delta_pipeline", start_date=datetime(2024, 1, 1), schedule=None, default_args=default_args) as dag:

    system_maintenance_task = BashOperator(
        task_id="system_maintenance_task",
        bash_command='echo "Install some pypi libs..."',
    )

    convert_to_delta = PythonOperator(
        task_id="convert_to_delta",
        python_callable=main_convert
    )

system_maintenance_task >> convert_to_delta