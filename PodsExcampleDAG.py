from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import configuration as conf
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
import random
import pendulum

default_args = {
'owner': 'Rafa',
'retries': 3,
'retry_delay': timedelta(minutes=1),
'concurrency': 3,
'max_active_runs': 3
}

namespace = conf.get('kubernetes', 'NAMESPACE')


if namespace =='default':
    config_file = '/home/user/.kube/config'
    in_cluster=False
else:
    in_cluster=True
    config_file=None

dag = DAG(
        dag_id='example_kubernetes_podsitatore',
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        concurrency=4, 
        max_active_runs=2,
        catchup=False,
        schedule_interval="@daily",
        tags=['example', 'example2'],
        default_args=default_args
        )

with dag:
    start = 1
    end = 3# random.randint(7, 23)
    camerasInt = range(start, end + 1)
    cameras =  [str(x) for x in camerasInt]
    brancheo = len(cameras)
    run_this_first = DummyOperator(
        task_id='run_this_first',
        dag = dag
    )
    
    
    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=lambda: cameras,
        
        dag = dag
    )
    join = DummyOperator(
        task_id='join',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        
        dag = dag
    )

    for camera in cameras:
        
        
        k = KubernetesPodOperator(
            namespace=namespace,
            image="grimmzaraki/ejemplo-docker:1.0",
            random_name_suffix = True,
            labels={"foo": "bar"},
            name="airflow-test-pod"+ str(camera),
            task_id=str(camera),
            in_cluster=in_cluster, # if set to true, will look in the cluster, if false, looks for file
            cluster_context='minikube', # is ignored when in_cluster is set to True
            config_file=config_file,
            is_delete_operator_pod=True,
            get_logs=True,
            dag = dag
        )
        
        
        # Label is optional here, but it can help identify more complex branches
        branching >>  k >> join