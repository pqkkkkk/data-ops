from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dbt_transform',
    default_args=default_args,
    description='Run dbt models',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'sqlserver'],
)

# Define tasks using BashOperator to execute dbt commands in the dbt container


dbt_run_bronze = BashOperator(
    task_id='dbt_run_bronze',
    bash_command='docker exec data-ops-dbt-1 bash -c "dbt deps && dbt run --select tag:bronze"',
    dag=dag,
)
dbt_run_silver = BashOperator(
    task_id='dbt_run_silver',
    bash_command='docker exec data-ops-dbt-1 bash -c "dbt deps && dbt run --select tag:silver"',
    dag=dag,
)
dbt_run_gold = BashOperator(
    task_id='dbt_run_gold',
    bash_command='docker exec data-ops-dbt-1 bash -c "dbt deps && dbt run --select tag:gold"',
    dag=dag,
)
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='docker exec data-ops-dbt-1 dbt test',
    dag=dag,
)

# Set task dependencies
dbt_run_bronze >> dbt_run_silver >> dbt_run_gold >> dbt_test