from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
import json
import random

def create_dag(dag_id,
               schedule,
               default_args):
    dag = DAG(dag_id, default_args=default_args, schedule_interval=schedule)
    with dag:
        start = 1
        end = random.randint(3, 30)
        camerasInt = range(start, end + 1)
        cameras =  [str(x) for x in camerasInt]
        init = DummyOperator(
            task_id='Init',
            dag=dag
        )
        clear = DummyOperator(
            task_id='clear',
            dag=dag
        )
        for camera in cameras:
            
            tab = KubernetesPodOperator(
            image="hello-world",
            random_name_suffix = True,
            labels={"foo": "bar"},
            name="airflow-test-pod"+ str(camera),
            task_id=str(camera),
            in_cluster=True, # if set to true, will look in the cluster, if false, looks for file
            cluster_context='minikube', # is ignored when in_cluster is set to True
            is_delete_operator_pod=True,
            get_logs=True,
            dag = dag)
            
            init >> tab >> clear
        return dag



schedule = '@daily'
dag_id = 'cameras'
args = {
    'owner': 'BigDataETL',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['bigdataetl@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'concurrency': 10,
    'max_active_runs': 1
}
globals()[dag_id] = create_dag(dag_id, schedule, args)