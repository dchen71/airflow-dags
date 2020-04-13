"""
RNASeq 2
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

## Reference Volume
input_ref_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'pvc-competitions-airflow2'
      }
    }

input_ref_volume = Volume(name='reference-mount', configs=input_ref_config)
input_ref_mount = VolumeMount('reference-mount',
                              mount_path='/mnt/references',
                              sub_path=None,
                              read_only=True)

# Input Data Volume
input_data_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'pvc-competitions-airflow2'
      }
    }

input_data_volume = Volume(name='input-mount', configs=input_data_config)
input_data_mount = VolumeMount('input-mount',
                                mount_path='/rnaseq/data',
                                sub_path=None,
                                read_only=True)

## Output Volume
output_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'pvc-competitions-airflow3'
      }
    }

output_volume = Volume(name='output-mount', configs=output_config)
output_mount = VolumeMount('output-mount',
                            mount_path='/mnt/output',
                            sub_path=None,
                            read_only=False)


args = {
    'owner': 'airflow',
    'start_date': days_ago(2)
}


##
# RNA Seq 2
##
with DAG(
    dag_id='rnaseq2',
    default_args=args,
    schedule_interval=None,
    tags=['example'],
) as dag:

    # Parse main file name without extensions
    parse_filename = BashOperator(
            task_id = 'parse_filename',
            bash_command = "filename={{ dag_run.conf['read1_name'] }}; echo ${filename%%.*}",
            xcom_push = True
            )

    # Create base folder for sample
    create_base_output_dir = KubernetesPodOperator(
        task_id="create_output_dir",
        name = "rnaseq2_create_output_dir",
        namespace='default',
        image="ubuntu",
        cmds=["/bin/bash -c mkdir /mnt/output/{{ti.xcom_pull(task_ids = 'parse_filename')}}/star"],
        volumes=[input_ref_config, input_data_volume, output_volume],
        volume_mounts=[input_ref_mount, input_data_mount, output_mount],
        is_delete_operator_pod=True
    )    

    # STAR
    ## Create star empty directory
    create_star_dir = KubernetesPodOperator(
        task_id="create_star_directory",
        name = "rnaseq2_create_star_dir",
        namespace='default',
        image="ubuntu",
        cmds=["/bin/bash -c mkdir /mnt/output/{{ti.xcom_pull(task_ids = 'parse_filename')}}/star"],
        volumes=[input_ref_config, input_data_volume, output_volume],
        volume_mounts=[input_ref_mount, input_data_mount, output_mount],
        is_delete_operator_pod=True
    )

    ## STAR
    run_star = KubernetesPodOperator(
        task_id="run_star",
        name = "rnaseq2_star",
        namespace='default',
        image="star",
        cmds=["star --genomeDir /mnt/references/ref/star_gencode_v33_index " + 
        "--runThreadN $(nproc) " +
        "--readFilesCommand zcat " + 
        "--readFilesIn {{ dag_run.conf['read1_name'] }} {{ dag_run.conf['read2_name'] }} " + 
        "--outputNamePrefix /mnt/output/{{ti.xcom_pull(task_ids = 'parse_filename')}}/star " +
        "--outSAMunmapped Within "  +
        "--outSAMtype BAM SortedByCoordinate " +
        "--quantMode TranscriptomeSAM GeneCounts"],
        volumes=[input_ref_config, input_data_volume, output_volume],
        volume_mounts=[input_ref_mount, input_data_mount, output_mount],
        is_delete_operator_pod=True
    )

    # SALMON
    ## Create salmon empty directory
    create_salmon_dir = KubernetesPodOperator(
        task_id="create_salmon_directory",
        name = "rnaseq2_create_salmon_dir",
        namespace='default',
        image="ubuntu",
        cmds=["/bin/bash -c mkdir /mnt/output/{{ti.xcom_pull(task_ids = 'parse_filename')}}/salmon"],
        volumes=[input_ref_config, input_data_volume, output_volume],
        volume_mounts=[input_ref_mount, input_data_mount, output_mount],
        is_delete_operator_pod=True
    )

    ## SALMON
    run_salmon = KubernetesPodOperator(
        task_id="run_salmon",
        name = "rnaseq2_salmon",
        namespace='default',
        image="salmon",
        cmds=["salmon quant " +
        "-i /mnt/references/ref/salmon_gencode_v33_index " +
        "-l A " +
        "-1 {{ dag_run.conf['read1_name'] }} " +
        "-2 {{ dag_run.conf['read2_name'] }} " +
        "-p ${nproc} " +
        "-g /mnt/references/ref/gencode.v4.annotation.gtf " +
        "-o /mnt/output/{{ti.xcom_pull(task_ids = 'parse_filename')}}/salmon"],
        volumes=[input_ref_config, input_data_volume, output_volume],
        volume_mounts=[input_ref_mount, input_data_mount, output_mount],
        is_delete_operator_pod=True
    )

    # FastQC
    ## Create fastqc empty directory
    create_fastqc_dir = KubernetesPodOperator(
        task_id="create_fastqc_directory",
        name = "rnaseq2_create_fastqc_dir",
        namespace='default',
        image="ubuntu",
        cmds=["/bin/bash -c mkdir /mnt/output/{{ti.xcom_pull(task_ids = 'parse_filename')}}/fastqc"],
        volumes=[input_ref_config, input_data_volume, output_volume],
        volume_mounts=[input_ref_mount, input_data_mount, output_mount],
        is_delete_operator_pod=True
    )

    ## Run FastQC
    run_fastqc = KubernetesPodOperator(
        task_id="run_fastqc",
        name = "rnaseq2_fastqc",
        namespace='default',
        image="fastqc",
        cmds=["fastqc " +
        "{{ dag_run.conf['read1_name'] }} " +
        "{{ dag_run.conf['read2_name'] }} " +
        "-t $(nproc) " +
        "-o /mnt/output/{{ti.xcom_pull(task_ids = 'parse_filename')}}/fastqc"],
        volumes=[input_ref_config, input_data_volume, output_volume],
        volume_mounts=[input_ref_mount, input_data_mount, output_mount],
        is_delete_operator_pod=True
    )

    # Samtools sort
    run_samtools = KubernetesPodOperator(
        task_id="run_samtools_sort",
        name = "rnaseq2_samtools",
        namespace='default',
        image="samtools",
        cmds=["samtools sort " +
        "-n " +
        "-o /mnt/output/{{ti.xcom_pull(task_ids = 'parse_filename')}}/out.sortedByName.bam " +
        "-m 7G " +
        "-@ $(nproc) " +
        "/mnt/output/{{ti.xcom_pull(task_ids = 'parse_filename')}}/star/Aligned.sortedByCoord.out.bam"],
        volumes=[input_ref_config, input_data_volume, output_volume],
        volume_mounts=[input_ref_mount, input_data_mount, output_mount],
        is_delete_operator_pod=True
    )

    # Qualimap
    ## Create qualimap empty directory
    create_qualimap_dir = KubernetesPodOperator(
        task_id="create_qualimap_directory",
        name = "rnaseq2_create_qualimap_dir",
        namespace='default',
        image="ubuntu",
        cmds=["/bin/bash -c mkdir /mnt/output/{{ti.xcom_pull(task_ids = 'parse_filename')}}/fastqc"],
        volumes=[input_ref_config, input_data_volume, output_volume],
        volume_mounts=[input_ref_mount, input_data_mount, output_mount],
        is_delete_operator_pod=True
    )

    ## Qualimap
    run_qualimap = KubernetesPodOperator(
        task_id="run_qualimap",
        name = "rnaseq2_qualimap",
        namespace='default',
        image="qualimap",
        cmds=["qualimap rnaseq " +
        "-bam /mnt/output/{{ti.xcom_pull(task_ids = 'parse_filename')}}/out.sortedName.bam " +
        "-gtf /mnt/references/ref/gencode.v33.annotation.gtf  " +
        "--java-mem-size=60G " +
        "-pe " +
        "-s -outdir /mnt/output/{{ti.xcom_pull(task_ids = 'parse_filename')}}/qualimap"],
        volumes=[input_ref_config, input_data_volume, output_volume],
        volume_mounts=[input_ref_mount, input_data_mount, output_mount],
        is_delete_operator_pod=True
    )

    # GATK
    ## Create GATK tmp directory
    create_gatk_dir = KubernetesPodOperator(
        task_id="create_gatk_directory",
        name = "rnaseq2_create_gatk_dir",
        namespace='default',
        image="gatk",
        cmds=["/bin/bash -c mkdir /mnt/output/{{ti.xcom_pull(task_ids = 'parse_filename')}}/tmp"],
        volumes=[input_ref_config, input_data_volume, output_volume],
        volume_mounts=[input_ref_mount, input_data_mount, output_mount],
        is_delete_operator_pod=True
    )

    ## Run GATK
    run_gatk = KubernetesPodOperator(
        task_id="run_gatk",
        name = "rnaseq2_gatk",
        namespace='default',
        image="gatk",
        cmds=["gatk " +
        "--java-options \"-Xmx7G\" " +
        "EstimateLibraryComplexity " +
        "-I /mnt/output/{{ti.xcom_pull(task_ids = 'parse_filename')}}/star/Aligned.sortedByCoord.out.bam " +
        "-pe " +
        "--TMP_DIR /mnt/output/{{ti.xcom_pull(task_ids = 'parse_filename')}}/tmp"],
        volumes=[input_ref_config, input_data_volume, output_volume],
        volume_mounts=[input_ref_mount, input_data_mount, output_mount],
        is_delete_operator_pod=True
    )

    # rseqc
    ## Create rseqc empty directory
    create_rseqc_dir = KubernetesPodOperator(
        task_id="create_rseqc_directory",
        name = "rnaseq2_create_rseqc_dir",
        namespace='default',
        image="ubuntu",
        cmds=["/bin/bash -c mkdir /mnt/output/{{ti.xcom_pull(task_ids = 'parse_filename')}}/rseqc"],
        volumes=[input_ref_config, input_data_volume, output_volume],
        volume_mounts=[input_ref_mount, input_data_mount, output_mount],
        is_delete_operator_pod=True
    )

    ## Run Rseqc
    run_rseqc = KubernetesPodOperator(
        task_id="run_rseqc",
        name = "rnaseq2_rseqc",
        namespace='default',
        image="rseqc",
        cmds=["geneBody_coverage.py " +
        "-r /mnt/references/ref/gencode.v33.annotation.bed " +
        "-i /mnt/output/{{ti.xcom_pull(task_ids = 'parse_filename')}}/star/Aligned.sortedByCoord.out.bam " +
        "-o /mnt/output/{{ti.xcom_pull(task_ids = 'parse_filename')}}/rseqc"],
        volumes=[input_ref_config, input_data_volume, output_volume],
        volume_mounts=[input_ref_mount, input_data_mount, output_mount],
        is_delete_operator_pod=True
    )

    parse_filename >> create_base_output_dir >> create_star_dir >> star >> create_salmon_dir >> run_salmon >> create_fastqc_dir >> run_fastqc >> run_samtools >> create_qualimap_dir >> run_qualimap >> create_gatk_dir >> run_gatk >> create_rseqc_dir >> run_rseqc
