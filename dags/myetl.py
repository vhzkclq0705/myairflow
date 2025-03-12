from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.trigger_rule import TriggerRule
import pendulum

def convert_csv_to_parquet(timestamp):
    import pandas as pd
    dir_path = "/Users/joon/data/"
    df = pd.read_csv(f"{dir_path}{timestamp}/data.csv")
    df.to_parquet(f"{dir_path}{timestamp}/data.parquet", engine='pyarrow')

def save_agg_csv(timestamp):
    import pandas as pd
    dir_path = "/Users/joon/data/"
    df = pd.read_parquet(f"{dir_path}{timestamp}/data.parquet", engine='pyarrow')
    gdf = df.groupby(["name", "value"]).size().reset_index(name="count")
    gdf.to_csv(f"{dir_path}{timestamp}/agg.csv", index=False)

# Directed Acyclic Graph
with DAG(
    "myetl",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 3, 12, tz="Asia/Seoul"),
) as dag:    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    make_data = BashOperator(
        task_id="make_data",
        bash_command="""
        /Users/joon/airflow/make_data.sh /Users/joon/data/{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H') }}
        """
    )
    
    load_data = PythonVirtualenvOperator(
        task_id="load_data",
        python_callable=convert_csv_to_parquet,
        op_args=["{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H') }}"],
        requirements=["pandas", "pyarrow"]
    )
    
    agg_data = PythonVirtualenvOperator(
        task_id="agg_data",
        python_callable=save_agg_csv,
        op_args=["{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H') }}"],
        requirements=["pandas", "pyarrow"]
    )
    
    start >> make_data >> load_data >> agg_data >> end
    
if __name__ == "__main__":
    dag.test()