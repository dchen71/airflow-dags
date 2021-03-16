"""
Humann3
"""
import os

from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

##
# Persistent Volume Configuration
##


## Reference Volume
input_ref_mount = VolumeMount(name='reference-mount',
                              mount_path='/mnt/references',
                              sub_path=None,
                              read_only=True)
input_ref_volume = Volume(name='reference-mount', configs={'persistentVolumeClaim':{'claimName': 'pvc-references'}})

# Input Data Volume
input_data_mount = VolumeMount(name='input-mount',
                                mount_path='/mnt/data',
                                sub_path=None,
                                read_only=True)
input_data_volume = Volume(name='input-mount', configs={'persistentVolumeClaim':{'claimName': 'pvc-input'}})

# Temp Data Volume
temp_data_mount = VolumeMount(name='temp-mount',
                                mount_path='/mnt/temp',
                                sub_path=None,
                                read_only=False)
temp_data_volume = Volume(name='temp-mount', configs={'persistentVolumeClaim':{'claimName': 'pvc-airflow1datatemp'}})

### Output Volume
output_mount = VolumeMount(name='output-mount',
                            mount_path='/mnt/output',
                            sub_path=None,
                            read_only=False)
output_volume = Volume(name='output-mount', configs={'persistentVolumeClaim':{'claimName': 'pvc-output'}})


args = {
    'owner': 'airflow',
    'email': ['asdpfjsdapofjspofjsdapojfspoj@sdfpojsdfpofjsd.io'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 5),
    'start_date': days_ago(2)
}


##
# Microbiome Humann3
##
with DAG(
    dag_id='humann3',
    default_args=args,
    schedule_interval=None,
    tags=['alpha'],
) as dag:

    # Parse main file name without extensions
    parse_filename = BashOperator(
            task_id = 'parse_filename',
            bash_command = "filename={{ dag_run.conf['read1_name'] }}; echo ${filename%%.*}",
            xcom_push = True
    )

    # Move to temp Azure File folder for processing
    create_temp = KubernetesPodOperator(
        task_id="create_temp",
        name = "humann3_create_temp_dir",
        namespace='default',
        image="ubuntu:18.04",
        cmds=["/bin/bash"],
        arguments=["-c","printf '{\"dir\": \"%s\"}' $(mktemp -d -p /mnt/temp) > /airflow/xcom/return.json"],
        volumes=[temp_data_volume],
        volume_mounts=[temp_data_mount],
        resources = {'request_cpu': '50m', 'request_memory': '50Mi'},
        is_delete_operator_pod=True,
        do_xcom_push = True
    )
    
    # Create base folder for sample
    create_base_output_dir = KubernetesPodOperator(
        task_id="create_output_dir",
        name = "humann3_create_output_dir",
        namespace='default',
        image="ubuntu:18.04",
        cmds=["mkdir"],
        arguments=["{{ti.xcom_pull(task_ids = 'create_temp')['dir']}}/{{ti.xcom_pull(task_ids = 'parse_filename')}}"],
        volumes=[temp_data_volume],
        volume_mounts=[temp_data_mount],
        resources = {'request_cpu': '50m', 'request_memory': '50Mi'},
        is_delete_operator_pod=True
    )    

    ## Run KneadData
    run_kneaddata = KubernetesPodOperator(
        task_id="run_kneaddata",
        name = "humann3_kneaddata",
        namespace='default',
        image="biobakery/kneaddata",
        cmds=["kneaddata"],
        arguments=["--input", "/mnt/data/{{ dag_run.conf['read1_name'] }}",
		"--input", "/mnt/data/{{ dag_run.conf['read2_name'] }}",
		"-db", "/mnt/reference/Homo_sapiens_db"
        "--output", "{{ti.xcom_pull(task_ids = 'create_temp')['dir']}}/{{ti.xcom_pull(task_ids = 'parse_filename')}}/kneaddata_output"
        ],
        volumes=[input_data_volume, temp_data_volume],
        volume_mounts=[input_data_mount, temp_data_mount],
        resources = {'request_cpu': '1'},
        is_delete_operator_pod=True
    )
	
	## Run KneadData
    run_humann3 = KubernetesPodOperator(
        task_id="run_humann3",
        name = "humann3",
        namespace='default',
        image="biobakery/humann",
        cmds=["humann"],
        arguments=["--input", "{{ti.xcom_pull(task_ids = 'create_temp')['dir']}}/{{ti.xcom_pull(task_ids = 'parse_filename')}}/kneaddata_output",
        "--output", "{{ti.xcom_pull(task_ids = 'create_temp')['dir']}}/{{ti.xcom_pull(task_ids = 'parse_filename')}}/humann_output"
        ],
        volumes=[input_data_volume, temp_data_volume],
        volume_mounts=[input_data_mount, temp_data_mount],
        resources = {'request_cpu': '1'},
        is_delete_operator_pod=True
    )

    # Move data after done processing to long term blob storage
    copy_data_to_storage = KubernetesPodOperator(
        task_id="copy_data_to_storage",
        name = "humann3_upload_to_storage",
        namespace='default',
        image="ubuntu:18.04",
        cmds=["cp"],
        arguments=["-r", "{{ti.xcom_pull(task_ids = 'create_temp')['dir']}}/{{ti.xcom_pull(task_ids = 'parse_filename')}}", "/mnt/output"],
        volumes=[temp_data_volume, output_volume],
        volume_mounts=[temp_data_mount, output_mount],
        resources = {'request_cpu': '2', 'request_memory': '20Gi'},
        is_delete_operator_pod=True
    )

    # Delete file share temp data
    cleanup_temp = KubernetesPodOperator(
        task_id="cleanup_temp",
        name = "humann3_cleanup_temp",
        namespace='default',
        image="ubuntu:18.04",
        cmds=["rm"],
        arguments=["-rf", "{{ti.xcom_pull(task_ids = 'create_temp')['dir']}}"],
        volumes=[temp_data_volume],
        volume_mounts=[temp_data_mount],
        resources = {'request_cpu': '1', 'request_memory': '1Gi'},
        is_delete_operator_pod=True
    )

    ## Dummies
    do_alignments = DummyOperator(
        task_id = "do_alignments"
    )

    do_qc_and_quantification = DummyOperator(
        task_id = "do_qc_and_quantification"
    )

    be_done = DummyOperator(
        task_id = "done"
    )

    start = DummyOperator(
        task_id = "start"
    )

    start >> [parse_filename, create_temp] >> create_base_output_dir >> run_kneaddata >> run_humann3 >> cleanup_temp >> be_done