from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='example_bash_operator',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

# Run KneadData to preprocess data
kneaddata = DockerOperator(
        task_id = 'python',
        image = 'library/python:latest',
        api_version = 'auto',
        command = 'print("hello world")',
        #docker_url = 'unix://var/run/docker.sock',
        network_mode = 'bridge',
        dag = dag
        )


if __name__ == "__main__":
    dag.cli()