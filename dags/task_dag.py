from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from random import randint
from datetime import datetime

'''
This dag will simulate a game of chance flow
Each user will roll the dice and the highest proceeds,
if the highest is above 4 out of a possible 6, the final output is a jackpot else, normal win
'''
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

    
with DAG("DICE_ROLL_DAG", 
         start_date=datetime(2021, 6, 1), # initial trigger
         schedule_interval="@daily", # how often it will run. Setup using a cron string
         catchup=False #
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

        choose_best_model = BranchPythonOperator( # wrapper for an if else situation
            task_id="choose_winner",
            python_callable=_choose_winner
        )
        
        jackpot = BashOperator(
            task_id="jackpot",
            bash_command="echo 'JACKPOT!!'"
        )

        normal = BashOperator(
            task_id="normal",
            bash_command="echo 'NORMAL WIN'"
        )
