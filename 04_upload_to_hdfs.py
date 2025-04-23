from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import random
import csv

with DAG(
    dag_id = '04_upload_to_hdfs',
    description = 'upload to hdfs',
    start_date = datetime(2025, 1, 1),
    catchup = False,
    schedule = timedelta(minutes=1)
) as dag:
    t1 = PythonOperator(
        task_id = 'upload',
        python_callable = uploads
    )