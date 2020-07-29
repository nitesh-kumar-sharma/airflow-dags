import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


def print_world():
    print('world_2')


default_args = {
    'start_date': dt.datetime(2020, 7, 28),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}


with DAG('tutorial_v02',
         default_args=default_args,
         schedule_interval='*/5 * * * *',
         ) as dag:

    print_hello = BashOperator(task_id='print_hello',
                               bash_command='echo "hello"')
    sleep = BashOperator(task_id='sleep',
                         bash_command='sleep 5')
    print_world = PythonOperator(task_id='print_world',
                                 python_callable=print_world)


print_hello >> sleep >> print_world