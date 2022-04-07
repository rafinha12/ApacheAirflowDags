#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example DAG demonstrating the usage of the BranchPythonOperator."""

import random

import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.edgemodifier import Label
from airflow import configuration as conf
from airflow.utils.trigger_rule import TriggerRule
import random
from datetime import datetime, timedelta



namespace = conf.get('kubernetes', 'NAMESPACE')

if namespace =='default':
    config_file = '/home/user/.kube/config'
    in_cluster=False
else:
    in_cluster=True
    config_file=None
    
default_args = {
'owner': 'Rafa',
'retries': 3,
'retry_delay': timedelta(minutes=1),
'concurrency': 3,
'max_active_runs': 3
}

with DAG(
    dag_id='example_Rafaeñ',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval="@daily",
    tags=['example', 'example2'],
    default_args=default_args
) as dag:
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
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
   
    for camera in cameras:

        t = DummyOperator(
            task_id=str(camera),
        )

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
        branching >> Label(str(camera)) >> t >> k >> join
