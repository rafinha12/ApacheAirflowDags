from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
import json
import random

def create_dag(dag_id,
               schedule,
               default_args,
               conf):
    dag = DAG(dag_id, default_args=default_args, schedule_interval=schedule)
    with dag:
        init = DummyOperator(
            task_id='Init',
            dag=dag
        )
        clear = DummyOperator(
            task_id='clear',
            dag=dag
        )
        for table in conf:
            tab = DummyOperator(
                task_id=table,
                dag=dag
            )
            init >> tab >> clear
        return dag
start = 1
end = random.randint(3, 8)
camerasInt = range(start, end + 1)
cameras =  [str(x) for x in camerasInt]

conf = cameras
schedule = '@daily'
dag_id = 'cameras' + len(cameras)
args = {
    'owner': 'BigDataETL',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['bigdataetl@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'concurrency': 1,
    'max_active_runs': 1
}
globals()[dag_id] = create_dag(dag_id, schedule, args, conf)