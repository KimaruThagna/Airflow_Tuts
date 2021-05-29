from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from random import randint
from datetime import datetime, timedelta

'''

The execution time in Airflow is not the actual run time, but rather the start timestamp of its schedule period.
For example, the execution time of the first DAG run is 2019–12–05 7:00:00, 
though it is executed on 2019–12–06. 


This dag will simulate a game of chance flow
Each user will roll the dice and the highest proceeds,
if the highest is above 4 out of a possible 6, the final output is a jackpot else, normal win

NOTES
by default, if a task has a return value, it is pushed to xcoms with the task id as the key
To have a custom key, use ti.xcom.push()

eg
def _training_model(ti):
    accuracy = uniform(0.1, 10.0)
    ti.xcom_push(key='model_accuracy', value=accuracy)
PULLING SEVERAL KEYS 
def _choose_best_model(ti):
    print('choose best model')
    #several values, same ID
    accuracies = ti.xcom_pull(key='model_accuracy', task_ids=['training_model_A', 'training_model_B', 'training_model_C'])
    
    
TRIGGER RULES

ALL_SUCCESS = 'all_success'
ALL_FAILED = 'all_failed'
ALL_DONE = 'all_done'
ONE_SUCCESS = 'one_success'
ONE_FAILED = 'one_failed'
DUMMY = 'dummy'

You can have an operator that runs after a branch operator where it should be triggered when
at least one of the tasks after the branch are executed This will need a special trigger rule such as
        trigger_rule=TriggerRule.ONE_SUCCESS,
'''

default_args = {
    'owner': 'airflow',
    'depends_on_past': False, # current runs do not depend on successful runs of the past
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

def _roll_dice():
    return randint(1, 6)

def _choose_winner(ti): # the task interface that allows you to pull 
    # data returned from other tasks by task ID
    results = ti.xcom_pull(task_ids=[
        'player_A',
        'player_B',
        'player_C'
    ])
    winner = max(results) # choose winning roll
    if (winner > 4):
        return 'jackpot'
    return 'normal'

def jackpot_bash():
    return """
            echo 'JACKPOT!!>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
        """


with DAG("DICE_ROLL_DAG", 
         start_date=datetime(2021, 6, 1), # initial trigger
         schedule_interval="@daily", # how often it will run. Setup using a cron expression
         catchup=False # only latest non triggered dag-run will be triggered. 
            ) as dag:

        player_A = PythonOperator( # operator wrapper for python functions
            task_id="player_A",
            python_callable=_roll_dice
        )

        player_B = PythonOperator(
            task_id="player_B",
            python_callable=_roll_dice
        )

        player_C = PythonOperator(
            task_id="player_C",
            python_callable=_roll_dice
        )

        choose_winner = BranchPythonOperator( # wrapper for an if else situation
            task_id="choose_winner",
            python_callable=_choose_winner
        )
        
        jackpot = BashOperator(
            task_id="jackpot",
            bash_command=jackpot_bash,
            xcom_push=False # dont push return value to xcoms
            
        )

        normal = BashOperator(
            task_id="normal",
            bash_command="echo 'NORMAL WIN/////////////////////////////////////'",
            xcoms_push=False
        )
        # setup dependendcy order
        # grouped tasks are in a list
        [player_A, player_B, player_C] >> choose_winner >> [jackpot,normal]
        
'''


 several_similar_dag_tasks_to_execute_in_parallel = [
        PythonOperator(
            task_id=f"task_{id}",
            python_callable=_process_task,
            op_kwargs={
                "id": task_id
            }
        ) for task_id in ['A', 'B', 'C']
    ]
'''
