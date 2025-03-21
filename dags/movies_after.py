from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonVirtualenvOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
import os

DAG_ID = "movies_after"

with DAG(
    DAG_ID,
    default_args={
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=5,
    description='movie',
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2025, 1, 1),
    catchup=True,
    tags=['api', 'movie', 'sensor'],
) as dag:
    REQUIREMENTS=["git+https://github.com/vhzkclq0705/movies-etl.git@0.6.7"]
    BASE_DIR = os.path.expanduser(f"~/swcamp4/data/{DAG_ID}")
    
    def fn_gen_meta(ds_nodash, base_path):
        from movies_etl.api.after import save_meta
        save_path = save_meta(ds_nodash, base_path)
        
        print("::group::gen meta..")
        print("ds_nodash ---> ", ds_nodash)
        print("save_path ---> ", save_path)
        print("::endgroup::")
        
        
    def fn_gen_movie(ds_nodash, base_path):
        from movies_etl.api.after import gen_movie
        save_path = gen_movie(ds_nodash, base_path)
        
        print("::group::gen meta..")
        print("ds_nodash ---> ", ds_nodash)
        print("save_path ---> ", save_path)
        print("::endgroup::")
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    check_done = FileSensor(
        task_id="check_done",
        filepath="/Users/joon/swcamp4/data/movies/done/dailyboxoffice/{{ ds_nodash }}/_DONE",
        fs_conn_id="fs_after_movies",
        poke_interval=180, # 3분 주기 체크
        timeout=3600, # 1 시간 후 타임아웃
        mode="reschedule", # 리소스를 점유하지 않고 절약하는 방식
    )
    
    gen_meta = PythonVirtualenvOperator(
        task_id="gen.meta",
        python_callable=fn_gen_meta,
        requirements=REQUIREMENTS,
        system_site_packages=False,
        op_kwargs={
            "base_path": BASE_DIR
        }
    )
    
    gen_movie = PythonVirtualenvOperator(
        task_id="gen.movie",
        python_callable=fn_gen_movie,
        requirements=REQUIREMENTS,
        system_site_packages=False,
        op_kwargs={
            "base_path": BASE_DIR
        }
    )
    
    make_done = BashOperator(
        task_id="make.done",
        bash_command="""
        DONE_BASE=$BASE_DIR/done
        echo $DONE_BASE
        mkdir -p $DONE_BASE/{{ ds_nodash }}
        touch $DONE_BASE/{{ ds_nodash }}/_DONE
        """,
        env={'BASE_DIR': BASE_DIR},
        append_env=True
    )
    
    start >> check_done >> gen_meta >> gen_movie >> make_done >> end