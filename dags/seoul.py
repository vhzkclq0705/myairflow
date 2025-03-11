from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import pendulum

def generate_bash_commands(col: list):
    max_len = max(map(lambda x: len(x), col))
    cmds = [f'echo "{c}{" " * (max_len - len(c))} : ===> {{{{ {c} }}}}"' for c in col]
    return "\n".join(cmds)

# Directed Acyclic Graph
with DAG(
    "seoul",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 3, 11, tz="Asia/Seoul"),
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    cols_b1 = [
    "data_interval_start", "data_interval_end", "logical_date", "ds", "ds_nodash",
    # "exception",
    "ts", "ts_nodash_with_tz", "ts_nodash", "prev_data_interval_start_success",
    "prev_data_interval_end_success", "prev_start_date_success", "prev_end_date_success",
    "inlets", "inlet_events", "outlets", "outlet_events", "dag", "task", "macros",
    "task_instance", "ti", "params", "var.value", "var.json", "conn", "task_instance_key_str",
    "run_id", "dag_run", "map_index_template", "expanded_ti_count", "triggering_dataset_events"
    ]
    cmds_b1 = generate_bash_commands(cols_b1)
    b_1 = BashOperator(
        task_id="b_1",
        bash_command=f"""
        echo "date ====================> `date`"
        {cmds_b1}
        """
    )
    
    cols_b2_1 = [
    "execution_date",
    "next_execution_date", "next_ds", "next_ds_nodash",
    "prev_execution_date", "prev_ds", "prev_ds_nodash",
    "yesterday_ds", "yesterday_ds_nodash",
    "tomorrow_ds", "tomorrow_ds_nodash",
    "prev_execution_date_success",
    "conf"
    ]
    cmds_b2_1 = generate_bash_commands(cols_b2_1)
    b_2_1 = BashOperator(
        task_id="b_2_1",
        bash_command=f"""
        {cmds_b2_1}
        """
    )
    b_2_2 = BashOperator(
        task_id="b_2_2",
        bash_command=f"""
        echo "data_interval_start: {{ data_interval_start.in_tz('Asia/Seoul') }}"
        """
    )
    
    mkdir = BashOperator(
        task_id="mkdir",
        bash_command="""
        echo "data_interval_start: {{ data_interval_start.in_tz('Asia/Seoul') }}"
        
        LOG_DIR=~/data/seoul/{{ data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d') }}
        LOG_FILE="$LOG_DIR/task_{{ task_instance.task_id }}.log"
        
        mkdir -p "$LOG_DIR"
        echo "로그 기록 (data_interval_start: {{ data_interval_start }})" >> "$LOG_FILE"
        """
    )
    
    send_notification = BashOperator(
        task_id="send_notification",
        bash_command="""
        curl -H "Content-Type: application/json" -X POST -d '{"content": "폴더 생성 실패 by Jerry"}' "https://discordapp.com/api/webhooks/$WEBHOOK_ID/$WEBHOOK_TOKEN"
        """,
        env={
            "WEBHOOK_ID": Variable.get("WEBHOOK_ID"),
            "WEBHOOK_TOKEN": Variable.get("WEBHOOK_TOKEN")
        },
        trigger_rule=TriggerRule.ONE_FAILED
    )
    
    start >> b_1 >> [b_2_1, b_2_2] >> mkdir >> [end, send_notification]