from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='example wait',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
)

# Run KneadData to preprocess data
bash = BashOperator(
        task_id = 'bash',
        bash_command = 'sleep 300',
        dag = dag
        )


if __name__ == "__main__":
    dag.cli()