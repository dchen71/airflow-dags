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
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


##
# Persistent Volume Configuration
##

"""
Configuration for PVC claim
Arguments:
claimName (string): Name of the PVC claim in kubernetes
"""
volume_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'pvc-competitions-airflow2'
      }
    }

volume = Volume(name='airflow2', configs=volume_config)

"""
Configuration for Volume Mounting location from PVC
Arguments:
name (string): Name of the PVC volume request
mount_path (string): Mount directory in the pod
sub_path (string): Sub path based on the mount directory
read_only (boolean): If the mount is read only or not
"""
volume_mount = VolumeMount('airflow2',
                            mount_path='/mnt/azure',
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
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=False,
        resources={'limit_memory': '256Mi', 'limit_cpu': 0.3}
    )
    
    test_task = BashOperator(
        task_id="test_tasks",
        bash_command="df -h",
        executor_config={"KubernetesExecutor": {
                "image": "ubuntu:latest",
                "volumes": [
                    {
                        "name": 'airflow2', 
                        "persistentVolumeClaim": {"claimName": 'pvc-competitions-airflow2'}
                    }
                ],
                "volume_mounts": [
                    {
                        'mountPath': "/mnt/azure/",
                        'name': "airflow2" 
                    }
                ]
            }
        }
    )

    def command_tasks():
        """
        Read file and trigger dags to build hellow worlds
        """
        print(os.listdir("/mnt/azure/"))
        circe = open("/mnt/azure/circe.txt", "r")
        for line in circe:
            print(line)

    master_task = PythonOperator(
        task_id="trigger_tasks",
        python_callable=command_tasks,
        executor_config={"KubernetesExecutor": {
                "image": "python:latest",
                "volumes": [
                    {
                        "name": 'airflow2', 
                        "persistentVolumeClaim": {"claimName": 'pvc-competitions-airflow2'}
                    }
                ],
                "volume_mounts": [
                    {
                        'mountPath': "/mnt/azure/",
                        'name': "airflow2" 
                    }
                ]
            }
        }
    )

    example_task2 = KubernetesPodOperator(
        task_id="pwd",
        name = "kubetest",
        namespace='default',
        image="ubuntu:18.04",
        cmds=["pwd"],
        #volumes=[volume],
        #volume_mounts=[volume_mount],
        is_delete_operator_pod=True
    )

    example_task3 = KubernetesPodOperator(
        task_id="echo",
        name = "kubetest",
        namespace='default',
        image="ubuntu:18.04",
        cmds=["echo"],
        arguments=["hello world", '{{params.example_var}}', '$(EXAMPLE_VAR2)'],
        #volumes=[volume],
        #volume_mounts=[volume_mount],
        is_delete_operator_pod=True,
        #secrets = [secret_env],
        params={'example_var': 'roger'},
        env_vars={'EXAMPLE_VAR2': 'roger'}
    )

    # Order for pipeline to do stuff
    ## start pipeline > list of 2 tasks > converge
    start_task >> test_task >> master_task
    
