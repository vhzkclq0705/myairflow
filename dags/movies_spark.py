from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

DAG_ID = "movie_spark"

with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=5,
    description="movie spark submit",
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 2),
    catchup=True,
    tags=["spark", "submit", "movie"],
) as dag:
    SPARK_HOME = "/Users/joon/swcamp4/app/spark-3.5.1-bin-hadoop3"
    SCRIPT_BASE = "/Users/joon/swcamp4/code/myairflow/pyspark"
    META_PATH = "/Users/joon/swcamp4/data/movie_spark/meta/_SUCCESS"
    RAW_BASE = "/Users/joon/swcamp4/data/data/movie_after/dailyboxoffice"
    
    def check_exists_meta():
        import os
        if os.path.exists(META_PATH):
            return append_meta.task_id
        else:
            return create_meta.task_id
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")
    
    exists_meta = BranchPythonOperator(
        task_id="exists.meta",
        python_callable=check_exists_meta
    )
    
    append_meta = BashOperator(
        task_id="append.meta",
        bash_command="""
            $SPARK_HOME/bin/spark-submit \
            $SCRIPT_BASE/movie_meta.py $RAW_BASE/dt={{ ds_nodash }} append $META_PATH
        """,
        env={
            "SPARK_HOME": SPARK_HOME,
            "SCRIPT_BASE": SCRIPT_BASE,
            "META_PATH": META_PATH,
            "RAW_BASE": RAW_BASE
        }
    )
    
    create_meta = BashOperator(
        task_id="create.meta",
        bash_command="""
            $SPARK_HOME/bin/spark-submit \
            $SCRIPT_BASE/movie_meta.py $RAW_BASE/dt={{ ds_nodash }} create $META_PATH
        """,
        env={
            "SPARK_HOME": SPARK_HOME,
            "SCRIPT_BASE": SCRIPT_BASE,
            "META_PATH": META_PATH,
            "RAW_BASE": RAW_BASE
        }
    )
    
    start >> exists_meta >> append_meta >> end
    exists_meta >> create_meta >> end