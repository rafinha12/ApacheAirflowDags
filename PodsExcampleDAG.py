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
        retries = 3,
        retry_delay =  timedelta(minutes=1),
        )

with dag:
    start = 1
    end = random.randint(7, 12)
    camerasInt = range(start, end + 1)
    cameras =  [str(x) for x in camerasInt]
    brancheo = len(cameras)
    run_this_first = DummyOperator(
        task_id='run_this_first',
    )
    
    
    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=lambda: cameras,
    )
    run_this_first >> branching
    join = DummyOperator(
        task_id='join',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    for camera in cameras:
        
        
        k = KubernetesPodOperator(
            namespace=namespace,
            image="hello-world",
            labels={"foo": "bar"},
            name="airflow-test-pod",
            task_id=str(camera),
            in_cluster=in_cluster, # if set to true, will look in the cluster, if false, looks for file
            cluster_context='minikube', # is ignored when in_cluster is set to True
            config_file=config_file,
            is_delete_operator_pod=True,
            get_logs=True)
        
        # Label is optional here, but it can help identify more complex branches
        branching >>  k >> join