from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_airflow_init',
    default_args=default_args,
    description='A simple dbt and Airflow init DAG',
    schedule_interval='@once',
)


# Define dbt commands as bash commands
dbt_seed = BashOperator(
    task_id='dbt_seed',
   # bash_command='cd ../../dbt_airflow && dbt seed --profiles-dir .',
   bash_command='pwd --profiles-dir .',
    dag=dag,
)


# Set up dependencies between tasks
dbt_seed