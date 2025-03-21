from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonVirtualenvOperator, BranchPythonOperator
import os

with DAG(
    'movie',
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
    tags=['api', 'movie'],
) as dag:
    REQUIREMENTS=["git+https://github.com/vhzkclq0705/movies-etl.git@0.6.7"]
    BASE_DIR=os.path.expanduser("~/swcamp4/data/movies/dailyboxoffice")
    
    def brach_func(ds_nodash):
        check_path = (f"{BASE_DIR}/dt={ds_nodash}")
        if os.path.exists(check_path):
            return "rm.dir"
        else:
            return "get.start", "echo.task"
    
    def common_get_data(ds_nodash, url_param, base_dir):
        from dotenv import load_dotenv
        import os
        
        env_path = os.path.expanduser("~/swcamp4/code/myairflow/.env")
        load_dotenv(env_path, override=True)
        os.environ["MOVIES_API_KEY"] = os.getenv("MOVIES_API_KEY")
        
        from movies_etl.api.call import call_api, list2df, save_df
        data = call_api(ds_nodash, url_param)
        df = list2df(data, ds_nodash, url_param)
        save_path = save_df(df, base_dir, ['dt'] + list(url_param.keys()))
        
        print("::group::movie df save...")
        print("save_path ---> " + save_path)
        print("url_param ---> ", url_param)
        print("ds_nodash ---> " + ds_nodash)
        print("::endgroup::")
    
    def fn_merge_data(ds_nodash, base_dir):
        from movies_etl.api.call import merge_df
        df = merge_df(ds_nodash, base_dir)
            
        print("::group::movie df merge..")
        print("ds_nodash ---> " + ds_nodash)
        print("movies df ---> \n", df)
        print("::endgroup::")
    
    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=brach_func,
        op_kwargs={
           "ds_nodash": "{{ ds_nodash }}"
        }
    )
    
    multi_y = PythonVirtualenvOperator(
        task_id='multi.y',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={
            "url_param": {"multiMovieYn": "Y"},
            "base_dir": BASE_DIR
        }
    )

    multi_n = PythonVirtualenvOperator(
        task_id='multi.n',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={
            "url_param": {"multiMovieYn": "N"},
            "base_dir": BASE_DIR
        }
    )

    nation_k = PythonVirtualenvOperator(
        task_id='nation.k',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={
            "url_param": {"repNationCd": "K"},
            "base_dir": BASE_DIR
        }
    )

    nation_f = PythonVirtualenvOperator(
        task_id='nation.f',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={
            "url_param": {"repNationCd": "F"},
            "base_dir": BASE_DIR
        }
    )
    
    no_param = PythonVirtualenvOperator(
        task_id='no.param',
        python_callable=common_get_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={
            "url_param": {},
            "base_dir": BASE_DIR
        }
    )

    rm_dir = BashOperator(
        task_id='rm.dir',
        bash_command=f'rm -rf ${BASE_DIR}/dt={{{{ ds_nodash }}}}',
    )

    echo_task = BashOperator(
        task_id='echo.task',
        bash_command="echo 'task'"
    )
    
    merge_data = PythonVirtualenvOperator(
        task_id='merge.data',
        python_callable=fn_merge_data,
        system_site_packages=False,
        requirements=REQUIREMENTS,
        op_kwargs={
            "base_dir": BASE_DIR
        }
    )
    
    make_done = BashOperator(
        task_id="make.done",
        bash_command="""
        DONE_BASE_PATH=/Users/joon/swcamp4/data/movies/done/dailyboxoffice
        mkdir -p $DONE_BASE_PATH/{{ ds_nodash }}
        touch $DONE_BASE_PATH/{{ ds_nodash }}/_DONE
        """
    )
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    get_start = EmptyOperator(
        task_id="get.start",
        trigger_rule="all_done"
    )
    get_end = EmptyOperator(task_id="get.end")
    
    start >> branch_op
    branch_op >> rm_dir >> get_start
    branch_op >> get_start
    branch_op >> echo_task
    get_start >> [no_param, multi_n, multi_y, nation_f, nation_k] >> get_end
    get_end >> merge_data >> make_done >> end