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
        dag_id='example_kubernetes_podito',
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule_interval="* */10 * * *",
        tags=['example', 'example2'],
        )

with dag:
    run_this_first = DummyOperator(
        task_id='run_this_first',
    )
    start = 1
    end = random.randint(3, 8)
    camerasInt = range(start, end + 1)
    cameras =  [str(x) for x in camerasInt]
    
    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=lambda: cameras,
    )
    run_this_first >> branching
    join = DummyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    for camera in cameras:
        
        t = DummyOperator(
            task_id=str(camera),
        )
        k = KubernetesPodOperator(
            namespace=namespace,
            image="hello-world",
            labels={"foo": "bar"},
            name="airflow-test-pod",
            task_id="task-"+ camera,
            in_cluster=in_cluster, # if set to true, will look in the cluster, if false, looks for file
            cluster_context='minikube', # is ignored when in_cluster is set to True
            config_file=config_file,
            is_delete_operator_pod=True,
            get_logs=True)
        
        # Label is optional here, but it can help identify more complex branches
        branching >> Label(str(camera)) >> t >> k >> join