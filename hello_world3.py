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
"""
This is an example dag for using the Kubernetes Pod Operator.
"""
import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.operators.bash_operator import BashOperator


##
# Persistent Volume Configuration
##

args = {
    'owner': 'airflow',
    'start_date': days_ago(2)
}


##
# Hello World 3
##
with DAG(
    dag_id='hello_world3',
    default_args=args,
    schedule_interval=None,
    tags=['example'],
) as dag:

    # Lazily ls volume mount
    start_task = KubernetesPodOperator(
        task_id="df",
        name = "kubetest",
        namespace='default',
        image="ubuntu:18.04",
        cmds=["pwd"],
        is_delete_operator_pod=True,
        resources={'limit_memory': '256Mi', 'limit_cpu': 0.3}
    )
    
    hello_task = BashOperator(
        task_id="hello_this_task",
        bash_command='echo hello world'
    )

    # Order for pipeline to do stuff
    ## ls mount > create files > write to files
    start_task >> hello_task
    
