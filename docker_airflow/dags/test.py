from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# Define the DAG
dag = DAG(
    dag_id='print_current_directory',
    description='Print the current directory',
    start_date=datetime(2023, 7, 14),
    schedule_interval=None,
    catchup=False
)

# Task to print the current directory
print_directory_task = BashOperator(
    task_id='print_directory',
    bash_command='cd ../../dbt_airflow && pwd',
    dag=dag
)

print_sub_directory_task = BashOperator(
    task_id='activate_conda_environment',
    bash_command='cd ../../dbt_airflow && dir',
    dag=dag
)

# Define dbt commands as bash commands
dbt_seed = BashOperator(
    task_id='dbt_seed',
    bash_command='cd ../../dbt_airflow && dbt seed --profiles-dir .',
    dag=dag,
)



# Set the task dependencies
print_directory_task >> print_sub_directory_task >> dbt_seedg

