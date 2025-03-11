from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import pendulum

# Directed Acyclic Graph
with DAG(
    "Seoul",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 3, 11, tz="Asia/Seoul"),
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    b1 = BashOperator(
        task_id="b_1",
        bash_command="""
        echo "date =================================>   $(date)"
        echo "data_interval_start ==================>   {{ data_interval_start.in_tz('Asia/Seoul') }}"
        echo "data_interval_end ====================>   {{ data_interval_end.in_tz('Asia/Seoul') }}"
        echo "logical_date =========================>   {{ logical_date.in_tz('Asia/Seoul') }}"
        echo "ds ===================================>   {{ ds }}"
        echo "ds_nodash ============================>   {{ ds_nodash }}"
        echo "ts ===================================>   {{ ts }}"
        echo "ts_nodash_with_tz ====================>   {{ ts_nodash_with_tz }}"
        echo "ts_nodash ============================>   {{ ts_nodash }}"
        
        echo "prev_data_interval_start_success =====>   {{ prev_data_interval_start_success.in_tz('Asia/Seoul') }}"
        echo "prev_data_interval_end_success =======>   {{ prev_data_interval_end_success.in_tz('Asia/Seoul') }}"
        echo "prev_start_date_success ==============>   {{ prev_start_date_success.in_tz('Asia/Seoul') }}"
        echo "prev_end_date_success ================>   {{ prev_end_date_success.in_tz('Asia/Seoul') }}"
        
        echo "inlets ===============================>   {{ inlets }}"
        echo "inlet_events =========================>   {{ inlet_events }}"
        echo "outlets ==============================>   {{ outlets }}"
        echo "outlet_events ========================>   {{ outlet_events }}"
        echo "dag ==================================>   {{ dag }}"
        echo "task =================================>   {{ task }}"
        echo "macros ===============================>   {{ macros }}"
        echo "task_instance ========================>   {{ task_instance }}"
        echo "ti ===================================>   {{ ti }}"
        echo "params ===============================>   {{ params }}"
        echo "var.value ============================>   {{ var.value }}"
        echo "var.json =============================>   {{ var.json }}"
        echo "conn =================================>   {{ conn }}"
        echo "task_instance_key_str ================>   {{ task_instance_key_str }}"
        echo "run_id ===============================>   {{ run_id }}"
        echo "dag_run ==============================>   {{ dag_run }}"
        echo "test_mode ============================>   {{ test_mode }}"
        echo "map_index_template ===================>   {{ map_index_template }}"
        echo "expanded_ti_count ====================>   {{ expanded_ti_count }}"
        echo "triggering_dataset_events ============>   {{ triggering_dataset_events }}"
    
        echo "==============Deprecated==============="
        echo "execution_date =======================>   {{ execution_date }}"
        echo "next_execution_date ==================>   {{ next_execution_date }}"
        echo "next_ds ==============================>   {{ next_ds }}"
        echo "next_ds_nodash =======================>   {{ next_ds_nodash }}"
        echo "prev_execution_date ==================>   {{ prev_execution_date }}"
        echo "prev_ds ==============================>   {{ prev_ds }}"
        echo "prev_ds_nodash =======================>   {{ prev_ds_nodash }}"
        echo "yesterday_ds_nodash ==================>   {{ yesterday_ds_nodash }}"
        echo "tomorrow_ds ==========================>   {{ tomorrow_ds }}"
        echo "tomorrow_ds_nodash ===================>   {{ tomorrow_ds_nodash }}"
        echo "prev_execution_date_success ==========>   {{ prev_execution_date_success }}"
        echo "conf =================================>   {{ conf }}"
        """
    )
    b2_1 = BashOperator(task_id="b_2_1", bash_command="echo 2_1")
    b2_2 = BashOperator(task_id="b_2_2", bash_command="echo 2_2")
    
    start >> b1 >> [b2_1, b2_2] >> end