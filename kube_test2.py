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
This is an example dag for using the Kubernetes Executor.
"""
import os

from airflow import DAG
from libs.helper import print_stuff
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.docker_operator import DockerOperator


args = {
    'owner': 'airflow',
    'start_date': days_ago(2)
}

with DAG(
    dag_id='kube_test2',
    default_args=args,
    schedule_interval=None,
    tags=['example'],
) as dag:

    tolerations = [{
        'key': 'dedicated',
        'operator': 'Equal',
        'value': 'airflow'
    }]

    # You don't have to use any special KubernetesExecutor configuration if you don't want to
    start_task = PythonOperator(
        task_id="start_task",
        python_callable=print_stuff
    )

    # But you can if you want to
    one_task = DockerOperator(
        task_id="one_task",
        image="ubuntu:16.04",
        command="echo 10",
        executor_config={"KubernetesExecutor": {"image": "airflow/ci:latest"}}
    )

    start_task >> [one_task]