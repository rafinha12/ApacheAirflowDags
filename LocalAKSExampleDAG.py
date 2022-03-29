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
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
import random


with DAG(
    dag_id='example_Rafaeñ',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval="@daily",
    tags=['example', 'example2'],
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

        dummy_follow = DummyOperator(
            task_id='follow_' + str(camera),
        )

        # Label is optional here, but it can help identify more complex branches
        branching >> Label(str(camera)) >> t >> dummy_follow >> join
