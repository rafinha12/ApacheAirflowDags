from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import configuration as conf
from airflow.utils.dates import days_ago
import random

default_args = {
    'owner': 'user',
    'start_date': days_ago(5),
    'email': ['user@airflow.de'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

namespace = conf.get('kubernetes', 'NAMESPACE')

if namespace =='default':
    config_file = '/home/user/.kube/config'
    in_cluster=False
else:
    in_cluster=True
    config_file=None

dag = DAG('example_kubernetes_pod',
           schedule_interval="* */10 * * *",
          default_args=default_args)

with dag:
    
    start = 1
    end = random.randint(3, 8)
    camerasInt = range(start, end + 1)
    cameras =  [str(x) for x in camerasInt]
    for camera in cameras:
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