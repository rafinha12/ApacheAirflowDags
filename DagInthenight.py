from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import configuration as conf
from airflow.utils.trigger_rule import TriggerRule
import json
import random



def create_dag(dag_id,
               schedule,
               default_args):
    dag = DAG(dag_id, default_args=default_args, schedule_interval=schedule)
    namespace = conf.get('kubernetes', 'NAMESPACE')


    if namespace =='default':
        config_file = '/home/user/.kube/config'
        in_cluster=False
    else:
        in_cluster=True
        config_file=None
    with dag:
        start = 1
        end = 20#random.randint(3, 30)
        camerasInt = range(start, end + 1)
        cameras =  [str(x) for x in camerasInt]
        init = DummyOperator(
            task_id='Init',
            dag=dag
        )
        clear = DummyOperator(
            task_id='clear',
            #trigger_rule=TriggerRule.ALL_SUCCESS,
            dag=dag
        )
        for camera in cameras:
            
            tab = KubernetesPodOperator(
            namespace=namespace,
            image="grimmzaraki/rtsp-example",
            random_name_suffix = True,
            labels={"foo": "bar"},
            name="airflow-test-pod"+ str(camera),
            task_id=str(camera),
            cmds = ["dotnet","EjemploRTSP.dll"],
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
    'retry_delay': timedelta(minutes=1),
    'concurrency': 3,
    'max_active_runs': 3
}
globals()[dag_id] = create_dag(dag_id, schedule, args)