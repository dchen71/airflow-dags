"""
FASTQC for RNA-seq
"""
import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

##
# Persistent Volume Configuration
# Input and reference Volume
input_ref_config= {
    'persistentVolumeClaim':
    {
        'claimName': 'blob-storage-pvc-input'
    }
}

input_ref_mount = VolumeMount(
    name='input-ref-mount',
    mount_path='/mnt/input',
    sub_path=None,
    read_only=True
)
input_ref_volume = Volume(name='input-ref-mount', configs=input_ref_config)

# Output Volume
output_config= {
    'persistentVolumeClaim':
    {
        'claimName': 'blob-storage-pvc-output'
    }
}

output_mount = VolumeMount(
    name='output-mount',
    mount_path='/mnt/output',
    sub_path=None,
    read_only=False)
output_volume = Volume(name='output-mount', configs=output_config)

args = {
    'owner': 'airflow',
    'start_date': days_ago(2)
}

##
# RNA-seq using Fastqc
##
with DAG(
    dag_id='rnaseq_fastqc',
    default_args=args,
    schedule_interval=None,
    tags=['fastqc'],
) as dag:

    # Parse main file name without extensions
    # As xcom_push is True, the last line written to stdout, the filename
    # without suffix, will be pushed to an XCom
    parse_filename = BashOperator(
        task_id = 'parse_filename',
        bash_command = "filename={{ dag_run.conf['read1_name'] }}; echo ${filename%%_R1*}",
        xcom_push = True
    )

    # FASTQC
    ## Create fastqc empty directory
    create_fastqc_dir = KubernetesPodOperator(
        task_id="create_fastqc_dir",
        name = "rnaseq_create_fastqc_dir",
        namespace='default',
        image="ubuntu:18.04",
        cmds=["mkdir"],
        arguments=["-p", "/mnt/output/biao/{{ti.xcom_pull(task_ids='parse_filename')}}/fastqc"],
        volumes=[output_volume],
        volume_mounts=[output_mount],
        resources = {'request_cpu': '50m', 'request_memory': '50Mi'},
        is_delete_operator_pod=True
    )

    ## FASTQC
    run_fastqc = KubernetesPodOperator(
        task_id="run_fastqc",
        name = "rnaseq_fastqc",
        namespace='default',
        image="quay.io/biocontainers/fastqc:0.11.9--0",
        cmds=["fastqc"],
        arguments=[
            "/mnt/data/rnaseq_data/{{ dag_run.conf['read1_name'] }}",
            "/mnt/data/rnaseq_data/{{ dag_run.conf['read2_name'] }}",
            "-o", "/mnt/output/biao/{{ti.xcom_pull(task_ids='parse_filename')}}/fastqc",
            "-t", "2"
        ],
        volumes=[input_ref_volume, output_volume],
        volume_mounts=[input_ref_mount, output_mount],
        resources = {'request_cpu': '2'},
        is_delete_operator_pod=True
    )

    ## Dummies
    be_done = DummyOperator(
        task_id = "done"
    )

    parse_filename >> create_fastqc_dir >> run_fastqc >> be_done