"""
RNASeq 1
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

# Input sample table
input_sample_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'pvc-input'
      }
    }

input_sample_volume = Volume(name='input-sample-mount', configs=input_sample_config)
input_sample_mount = VolumeMount(name='input-sample-mount',
                                mount_path='/rnaseq',
                                sub_path=None,
                                read_only=True)

## Reference Volume
input_ref_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'pvc-references'
      }
    }

input_ref_volume = Volume(name='reference-mount', configs=input_ref_config)
input_ref_mount = VolumeMount(name='reference-mount',
                              mount_path='/rnaseq/ref',
                              sub_path=None,
                              read_only=True)

# Input Data Volume
input_data_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'pvc-input'
      }
    }

input_data_volume = Volume(name='input-mount', configs=input_data_config)
input_data_mount = VolumeMount(name='input-mount',
                                mount_path='/rnaseq/data',
                                sub_path=None,
                                read_only=True)

### Output Volume
output_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'pvc-output'
      }
    }

output_volume = Volume(name='output-mount', configs=output_config)
output_mount = VolumeMount(name='output-mount',
                            mount_path='/rnaseq/output',
                            sub_path=None,
                            read_only=False)


args = {
    'owner': 'airflow',
    'start_date': days_ago(2)
}


##
# RNA Seq 1
##
with DAG(
    dag_id='rnaseq1',
    default_args=args,
    schedule_interval=None,
    tags=['example'],
) as dag:

    mount_input_sample = KubernetesPodOperator(
        task_id="mount_input_sample",
        name = "rnaseq1_pipeline",
        namespace='default',
        image="ubuntu:18.04",
        cmds=["df -h"],
        volumes=[input_sample_volume],
        volume_mounts=[input_sample_mount],
        resources={'request_memory':'24Gi', 'limit_memory': '30G', 'request_cpu': '4', 'limit_cpu': '4'},
        is_delete_operator_pod=False
    )

    mount_input_ref = KubernetesPodOperator(
        task_id="mount_input_ref",
        name = "rnaseq1_pipeline",
        namespace='default',
        image="ubuntu:18.04",
        cmds=["df -h"],
        volumes=[input_ref_volume],
        volume_mounts=[input_ref_mount],
        resources={'request_memory':'24Gi', 'limit_memory': '30G', 'request_cpu': '4', 'limit_cpu': '4'},
        is_delete_operator_pod=False
    )

    mount_data = KubernetesPodOperator(
        task_id="mount_data",
        name = "rnaseq1_pipeline",
        namespace='default',
        image="ubuntu:18.04",
        cmds=["df -h"],
        volumes=[input_data_volume],
        volume_mounts=[input_data_mount],
        resources={'request_memory':'24Gi', 'limit_memory': '30G', 'request_cpu': '4', 'limit_cpu': '4'},
        is_delete_operator_pod=False
    )

    mount_output = KubernetesPodOperator(
        task_id="mount_output",
        name = "rnaseq1_pipeline",
        namespace='default',
        image="ubuntu:18.04",
        cmds=["df -h"],
        volumes=[output_volume],
        volume_mounts=[output_mount],
        resources={'request_memory':'24Gi', 'limit_memory': '30G', 'request_cpu': '4', 'limit_cpu': '4'},
        is_delete_operator_pod=False
    )


    mount_all = KubernetesPodOperator(
        task_id="mount_all",
        name = "rnaseq1_pipeline",
        namespace='default',
        image="ubuntu:18.04",
        cmds=["pwd"],
        volumes=[input_sample_volume, input_ref_volume, input_data_volume, output_volume],
        volume_mounts=[input_sample_mount, input_ref_mount, input_data_mount, output_mount],
        resources={'request_memory':'24Gi', 'limit_memory': '30G', 'request_cpu': '4', 'limit_cpu': '4'},
        is_delete_operator_pod=False
    )

    rna_seq = KubernetesPodOperator(
        task_id="rna_seq_fat",
        name = "rnaseq1_pipeline",
        namespace='default',
        image="dchen71/rna:202003",
        #cmds=["/bin/bash /rnaseq/scripts/rnaseq2020.sh"],
        volumes=[input_sample_volume, input_ref_volume, input_data_volume, output_volume],
        volume_mounts=[input_sample_mount, input_ref_mount, input_data_mount, output_mount],
        resources={'request_memory':'24Gi', 'limit_memory': '30G', 'request_cpu': '4', 'limit_cpu': '4'},
        is_delete_operator_pod=False
    )

    # Order for pipeline to do stuff
    ## ls mount > create files > write to files
    mount_input_sample >> mount_input_ref >> mount_data >> mount_output >> mount_all >> rna_seq
    
