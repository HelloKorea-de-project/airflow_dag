"""
This module contains an example DAG for demonstrating basic functionality.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id='hello_world',
    start_date=datetime(2022, 5, 5),
    catchup=False,
    tags=['example'],
    schedule='0 2 * * *')


def print_hello():
    """
    Prints hello and returns the string "hello!"
    """
    print("hello!")
    return "hello!"


def print_goodbye():
    """
    Prints goodbye and returns the string "goodbye!"
    """
    print("goodbye!")
    return "goodbye!"


print_hello = PythonOperator(
    task_id='print_hello',
    # python_callable param points to the function you want to run
    python_callable=print_hello,
    # dag param points to the DAG that this task is a part of
    dag=dag)


print_goodbye = PythonOperator(
    task_id='print_goodbye',
    python_callable=print_goodbye,
    dag=dag)


# Assign the order of the tasks in our DAG
print_hello >> print_goodbye
