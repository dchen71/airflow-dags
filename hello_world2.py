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


##
# Persistent Volume Configuration
##

## Input Volume
volume_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'pvc-competitions-airflow2'
      }
    }

volume = Volume(name='input-mount', configs=volume_config)
volume_mount = VolumeMount('input-mount',
                            mount_path='/mnt/azure',
                            sub_path=None,
                            read_only=True)


## Output Volume
volume2_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'pvc-competitions-airflow3'
      }
    }

volume2 = Volume(name='output-mount', configs=volume2_config)
volume2_mount = VolumeMount('output-mount',
                            mount_path='/mnt/azure2',
                            sub_path=None,
                            read_only=False)


args = {
    'owner': 'airflow',
    'start_date': days_ago(2)
}


##
# Hello World 2
##
with DAG(
    dag_id='hello_world2',
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
        cmds=["ls"],
        arguments=["/mnt/azure"],
        volumes=[volume, volume2],
        volume_mounts=[volume_mount, volume2_mount],
        is_delete_operator_pod=True,
        resources={'limit_memory': '256Mi', 'limit_cpu': 0.3}
    )
    
    create_files = KubernetesPodOperator(
        task_id="create_files",
        name = "kubetest",
        namespace='default',
        image="airflow1.azurecr.io/beaver:18.04",
        cmds=["/bin/bash", "-c", "cat /mnt/azure/circe.txt | while read line; do touch /mnt/azure2/$line.txt; done"],
        volumes=[volume, volume2],
        volume_mounts=[volume_mount, volume2_mount],
        is_delete_operator_pod=True
    )

    write_files = KubernetesPodOperator(
        task_id="write_files",
        name = "kubetest",
        namespace='default',
        image="airflow1.azurecr.io/dingo:19.04",
        cmds=["/bin/bash", "-c", "cat /mnt/azure/circe.txt | while read line; do echo hello world $line >> /mnt/azure2/$line.txt; done"],
        volumes=[volume, volume2],
        volume_mounts=[volume_mount, volume2_mount],
        is_delete_operator_pod=True
    )

    # Order for pipeline to do stuff
    ## ls mount > create files > write to files
    start_task >> create_files >> write_files
    
