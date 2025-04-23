from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def hello():
    pass

def bye():
    pass

with DAG(
    dag_id='02_python',
    description = 'python test',
    start_date = datetime(2025, 1, 1),
    catchup = False,
    schedule = timedelta(minutes=1)
) as dag:
    t1 = PythonOperator(
        task_id = 'hello',
        python_collable = hello 
    )

    t2 = PythonOperator(
        task_id = 'bye',
        python_collable = bye
    )
