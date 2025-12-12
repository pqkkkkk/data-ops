from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# from airflow.operators.python import PythonOperator
# from airflow.utils.email import send_email


def notify_success(context):
    """
    Send success notification
    """
    subject = f"✅ DAG Success: {context['dag'].dag_id}"
    body = f"""
    DAG: {context['dag'].dag_id}
    Task: {context['task'].task_id}
    Execution Time: {context['execution_date']}
    Log URL: {context['task_instance'].log_url}
    """
    print(subject)
    print(body)
    # For actual email, uncomment:
    # send_email(to=['your-email@example.com'], subject=subject, html_content=body)


def notify_failure(context):
    """
    Send failure notification
    """
    subject = f"❌ DAG Failed: {context['dag'].dag_id}"
    body = f"""
    DAG: {context['dag'].dag_id}
    Task: {context['task'].task_id}
    Execution Time: {context['execution_date']}
    Error: {context['exception']}
    Log URL: {context['task_instance'].log_url}
    """
    print(subject)
    print(body)
    # For actual email, uncomment:
    # send_email(to=['your-email@example.com'], subject=subject, html_content=body)


default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email": ["your-email@example.com"],
    "email_on_failure": False,  # Skip for now
    "email_on_retry": False,  # Skip for now
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    # 'on_failure_callback': notify_failure,
    # 'on_success_callback': notify_success,
}

dag = DAG(
    "dbt_transform",
    default_args=default_args,
    description="Run dbt models with Bronze, Silver, Gold layers",
    # schedule_interval='0 2 * * *',  # Run daily at 2 AM
    schedule_interval='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "sqlserver", "etl"],
)

# Source freshness check
dbt_source_freshness = BashOperator(
    task_id="dbt_source_freshness",
    bash_command="docker exec data-ops-dbt-1 dbt source freshness --profiles-dir .",
    dag=dag,
)

# Install dependencies
dbt_deps = BashOperator(
    task_id="dbt_deps",
    bash_command="docker exec data-ops-dbt-1 dbt deps",
    dag=dag,
)

# Bronze layer
dbt_run_bronze = BashOperator(
    task_id="dbt_run_bronze",
    bash_command="docker exec data-ops-dbt-1 dbt run --select tag:bronze --profiles-dir .",
    dag=dag,
)

# Test bronze layer
dbt_test_bronze = BashOperator(
    task_id="dbt_test_bronze",
    bash_command="docker exec data-ops-dbt-1 dbt test --select tag:bronze --profiles-dir .",
    dag=dag,
)

# Silver layer
dbt_run_silver = BashOperator(
    task_id="dbt_run_silver",
    bash_command="docker exec data-ops-dbt-1 dbt run --select tag:silver --profiles-dir .",
    dag=dag,
)

# Test silver layer
dbt_test_silver = BashOperator(
    task_id="dbt_test_silver",
    bash_command="docker exec data-ops-dbt-1 dbt test --select tag:silver --profiles-dir .",
    dag=dag,
)

# Gold layer
dbt_run_gold = BashOperator(
    task_id="dbt_run_gold",
    bash_command="docker exec data-ops-dbt-1 dbt run --select tag:gold --profiles-dir .",
    dag=dag,
)

# Test gold layer
dbt_test_gold = BashOperator(
    task_id="dbt_test_gold",
    bash_command="docker exec data-ops-dbt-1 dbt test --select tag:gold --profiles-dir .",
    dag=dag,
)

# Generate documentation
dbt_docs_generate = BashOperator(
    task_id="dbt_docs_generate",
    bash_command="docker exec data-ops-dbt-1 dbt docs generate --profiles-dir .",
    dag=dag,
)

# Set task dependencies
dbt_source_freshness >> dbt_deps
dbt_deps >> dbt_run_bronze >> dbt_test_bronze
dbt_test_bronze >> dbt_run_silver >> dbt_test_silver
dbt_test_silver >> dbt_run_gold >> dbt_test_gold
dbt_test_gold >> dbt_docs_generate
