from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import task

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

@task
def demo_python_task():
    print("Demo using decorators")
    
    
with DAG("DICE_ROLL_DAG",
         schedule_interval="@daily",
         catchup=False, 
         default_args=default_args,
            ) as dag:
    # create a task group where tasks will be grouped executed
    task_1 = BashOperator(
        task_id = "task_1",
        bash_command='sleep 3 && echo "slept at {{ task_id }}"'
    )
    
    with TaskGroup(group_id="group_1") as group_tasks:
        task_1 = BashOperator(
        task_id = "task_1", # similar task id but since its in group, it becomes group_id.task_id
        bash_command='sleep 3 && echo "slept at {{ task_id }}"'
    )
        
        task_1 = BashOperator(
        task_id = "task_2",
        bash_command='sleep 3 && echo "slept at {{ task_id }}"'
    )