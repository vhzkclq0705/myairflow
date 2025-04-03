from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DAG_ID = "wiki_proc"

with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=5,
    description="wiki data processing",
    schedule="0 * * * *",
    start_date=datetime(2024, 3, 1),
    end_date=datetime(2025, 4, 1),
    catchup=True,
    tags=["spark", "submit", "wiki"],
) as dag:
    SPARK_HOME = "/Users/joon/swcamp4/app/spark-3.5.1-bin-hadoop3"
    BASE_PATH = "/home/joon/code/wiki"
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    process_data = BashOperator(
        task_id="wiki.proc",
        bash_command="""
            ssh -i ~/.ssh/gcp-joon-key joon@34.22.98.70 \
            '$BASE_PATH/run.sh {{ ts_nodash }} $BASE_PATH/processing.py'
        """,
        env={
            "BASE_PATH": BASE_PATH
        }
    )
    
    start >> end