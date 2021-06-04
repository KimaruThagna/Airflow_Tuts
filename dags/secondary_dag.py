from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import timedelta


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


def data_processing_task():
    print("Demo processing task")
    return "Demo processing task"
    
    
with DAG("SECONDARY_DAG",
         schedule_interval="@daily",
         catchup=False, 
         description=" performs processing and db cleanup",
         default_args=default_args,
            ) as dag:
    
    
    processing_task = PythonOperator(
            task_id="processing_task",
            python_callable=data_processing_task
        )
    
    db_cleanup_task = BashOperator(
        task_id = "task_2",
        bash_command='sleep 3 && echo "CLEANUP DONE at {{ task_id }}"'
    )
    
    
    data_processing_task >> db_cleanup_task