from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'start_date': datetime(2023, 7, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'create_python_environment_and_install_dbt',
    default_args=default_args,
    schedule_interval=None,
)

create_python_env = BashOperator(
    task_id='create_python_environment',
    bash_command='python -m venv myenv && source myenv/bin/activate && pip install dbt-postgres && cd ../../dbt_airflow && dbt --version ',
    dag=dag,
)

# activate_env = BashOperator(
#     task_id='activate_environment',
#     bash_command='source myenv/bin/activate',
#     dag=dag,
# )

# install_dbt = BashOperator(
#     task_id='install_dbt',
#     bash_command='pip install dbt',
#     dag=dag,
# )

# install_dbt_postgres = BashOperator(
#     task_id='install_dbt_postgres',
#     bash_command='pip install dbt-postgres',
#     dag=dag,
# )

create_python_env 
##>> activate_env >> install_dbt >> install_dbt_postgres
