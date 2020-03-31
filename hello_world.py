##
# Hello WORLD!
##

import os

from airflow import DAG
from libs.helper import print_stuff
from airflow.utils.dates import days_ago
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.operators.dummy_operator import DummyOperator

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
        'claimName': 'airflow1data'
      }
    }

volume = Volume(name='airflow1data', configs=volume_config)

"""
Configuration for Volume Mounting location from PVC
Arguments:
name (string): Name of the PVC volume request
mount_path (string): Mount directory in the pod
sub_path (string): Sub path based on the mount directory
read_only (boolean): If the mount is read only or not
"""
volume_mount = VolumeMount('airflow1data',
                            mount_path='/mnt/azure',
                            sub_path=None,
                            read_only=False)

args = {
    'owner': 'airflow',
    'start_date': days_ago(2)
}

##
# hello
##
with DAG(
    dag_id='hello_world',
    default_args=args,
    schedule_interval=None,
    tags=['example'],
) as dag:

    
    """
    Example Task using KubernetesPodOperator
    This will start up a new Pod(Container) for each instance
    https://airflow.apache.org/docs/1.10.1/kubernetes.html
    Arguments:
    name (string): Name of the pod for kubernetes
    namespace (string): Name of namespace everything resides in. Default is 'default'
    image (string): Name of the docker image. Defaults to dockerhub but can point to private container registries
    cmds (list): List of strings for commands to run in the container
    arguments (list): List of strings for commands to run in container based on cmds
    volumes (list): List of Volume objects containing which volumes will be mounted
    volume_mounts (list): List of VolumeMount objects containing mount locations to the container
    is_delete_operator_pod (boolean): Delete pod when done. Should be true always.
    secrets (list): List of secret objects
    env_vars (dict): Dictionary of potential environmental variables
    resources (dict): Dictionary containing the limits of CPUs and Memory or requests for certain amount of CPU/Memory. Mi for megabyte. Gi for gigabyte.
    xcom_push (bool): If true, return the output from the end of the container as a variable
    params (dict): Parameters used for jinja2 to template in the arguments
    """

    start = DummyOperator(task_id='start_dag', dag=dag)

    start1 = KubernetesPodOperator(
        task_id="df",
        name = "aloha2",
        namespace='default',
        image="_/ubuntu:latest",
        cmds=["df -h"],
        arguments=["/mnt/azure"],
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=True,
        resources={'limit_memory': '256Mi', 'limit_cpu': 0.3}
    )

    start_task = KubernetesPodOperator(
        task_id="aloha",
        name = "aloha",
        namespace='default',
        image="airflow1.azurecr.io/python:v1",
        cmds=["echo hello world >> /mnt/azure/hello_world.txt"],
        arguments=["/mnt/azure"],
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=True,
        resources={'limit_memory': '256Mi', 'limit_cpu': 0.3}
    )

    end = DummyOperator(task_id='end_dag', dag=dag)
    

    start >> start1 >> start_task >> end
    
