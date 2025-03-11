from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import pendulum

# Directed Acyclic Graph
with DAG(
    "Seoul",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 3, 10, tz="Asia/Seoul"),
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    b1 = BashOperator(
        task_id="b_1",
        bash_command="""
        echo "date ==================> $(date)"
        echo "data_interval_start ===> {{ data_interval_start }}"
        echo "data_interval_end =====> {{ data_interval_end }}"
        echo "logical_date =====> {{ logical_date }}"
        echo "prev_start_date_success =====> {{ prev_start_date_success }}"
        echo "prev_end_date_success =====> {{ prev_end_date_success }}"
        """
    )
    b2_1 = BashOperator(task_id="b_2_1", bash_command="echo 2_1")
    b2_2 = BashOperator(task_id="b_2_2", bash_command="echo 2_2")
    
    start >> b1 >> [b2_1, b2_2] >> end