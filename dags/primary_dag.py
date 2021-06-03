from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

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
    'start_date': days_ago(2)
}

with DAG("PRIMARY_DAG",
         schedule_interval="@daily",
         catchup=False, 
         description=" performs data ingestion, triggers secondary dag and later performs analytics",
         default_args=default_args,
            ) as dag:
    
    ingestion = BashOperator(
        task_id = "task_1",
        bash_command='sleep 3 && echo "DATA INGESTION TASK at {{ task_id }}"'
    )
    
    trigger_target = TriggerDagRunOperator(
        task_id = 'trigger_target',
        trigger_dag_id= 'SECONDARY_DAG',
        execution_date= '{{ ds }}', # ensure the execution date of the secondary matches the primary
        reset_dag_run=True, # allows you to re run secondary dags later on since the execution date will be the same as that of primary dag
        wait_for_completion=True, # wait until secondary is done
        poke_interval=30 # poll every 30 seconds to see if secondary is running
            
    )
    
    analytics = BashOperator(
        task_id = "analytics",
        bash_command='sleep 3 && echo "analytics DONE at {{ task_id }}"'
    )
    
    
    ingestion >> trigger_target >> analytics
