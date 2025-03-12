from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import pendulum

def generate_bash_commands(vars: list):
    max_len = max(map(len, vars))
    return "\n".join([f'echo "{var}{" " * (max_len - len(var))} : ===> {{{{ {var} }}}}"' for var in vars])

def print_kwargs(**kwargs):
    # Discord notification
    dag_id = kwargs['dag'].dag_id
    task_id = kwargs['task'].task_id
    time = kwargs['data_interval_start'].in_tz('Asia/Seoul').strftime("%Y%m%d%H")
    msg = f"<{dag_id}> <{task_id}> <{time}> OK / Jerry"
    from myairflow.notify import send_noti
    send_noti(msg)
    
# Directed Acyclic Graph
with DAG(
    "seoul",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 3, 11, tz="Asia/Seoul"),
) as dag:
    data_vars = [
    "data_interval_start", "data_interval_end", "logical_date", "ds", "ds_nodash",
    "ts", "ts_nodash_with_tz", "ts_nodash", "prev_data_interval_start_success",
    "prev_data_interval_end_success", "prev_start_date_success", "prev_end_date_success",
    "inlets", "inlet_events", "outlets", "outlet_events", "dag", "task", "macros",
    "task_instance", "ti", "params", "var.value", "var.json", "conn", "task_instance_key_str",
    "run_id", "dag_run", "map_index_template", "expanded_ti_count", "triggering_dataset_events"
    ]
    deprecated_vars = [
    "execution_date", "next_execution_date", "next_ds", "next_ds_nodash",
    "prev_execution_date", "prev_ds", "prev_ds_nodash", "yesterday_ds", "yesterday_ds_nodash",
    "tomorrow_ds", "tomorrow_ds_nodash", "prev_execution_date_success", "conf"
    ]
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    b_1 = BashOperator(
        task_id="b_1",
        bash_command=f"""
        echo "date ====================> $(date)"
        {generate_bash_commands(data_vars)}
        """
    )
    b_2_1 = BashOperator(
        task_id="b_2_1",
        bash_command=generate_bash_commands(deprecated_vars)
    )
    b_2_2 = BashOperator(
        task_id="b_2_2",
        bash_command='echo "data_interval_start: {{ data_interval_start.in_tz(\'Asia/Seoul\') }}"'
    )
    
    mkdir = BashOperator(
        task_id="mkdir",
        bash_command="""
        echo "data_interval_start: {{ data_interval_start.in_tz('Asia/Seoul') }}"
        mkdir -p "~/data/seoul/{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H') }}"
        """
    )
    
    send_notification = PythonOperator(
        task_id="send_notification",
        python_callable=print_kwargs
    )
    
    start >> b_1 >> [b_2_1, b_2_2] >> mkdir >> [end, send_notification]
    
if __name__ == "__main__":
    dag.test()