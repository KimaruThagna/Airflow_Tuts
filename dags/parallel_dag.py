from airflow import DAG
from airflow.operators.bash import BashOperator

from random import randint
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False, # current runs do not depend on successful runs of the past dags
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG("DICE_ROLL_DAG",start_date=datetime(2021, 5, 1), # initial trigger
         schedule_interval="@daily",
         catchup=False,
            ) as dag:
    task_1 = BashOperator(
        task_id = "task_1",
        bash_command='sleep 3 && echo slept at {{ task_id }}'
    )