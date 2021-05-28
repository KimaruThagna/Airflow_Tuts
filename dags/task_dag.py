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
        return 'JACKPOT!!'
    return 'NORMAL WIN'
