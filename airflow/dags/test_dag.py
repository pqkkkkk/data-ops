# airflow/dags/test_dag.py - Thêm một dòng dài vượt quá 120 ký tự để black phải format lại.
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Dòng này có 137 ký tự, black sẽ format lại và flake8 sẽ báo lỗi nếu không có black
VERY_LONG_VARIABLE_NAME_THAT_EXCEEDS_THE_MAX_LINE_LENGTH_TO_TRIGGER_BLACK_FORMATTING_AND_FLAKE8_CHECKING = (
    1
)


with DAG(
    dag_id="ci_test_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ci_test"],
) as dag:
    task = BashOperator(
        task_id="echo_test",
        bash_command='echo "Running CI test task"',
    )
