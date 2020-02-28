import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import configuration as conf




args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='example_kubernetes_pod',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
)

#namespace = conf.get('kubernetes', 'NAMESPACE')
namespace = 'default'
compute_resource = {'request_cpu': '800m', 'request_memory': '3Gi', 'limit_cpu': '800m', 'limit_memory': '3Gi'}

# This will detect the default namespace locally and read the 
# environment namespace when deployed to Astronomer.
if namespace =='default':
    config_file = '/usr/local/airflow/include/.kube/config'
    in_cluster=False
else:
    in_cluster=True
    config_file=None


with dag:
    k = KubernetesPodOperator(
        namespace=namespace,
        image="hello-world",
        labels={"foo": "bar"},
        name="airflow-test-pod",
        task_id="task-one",
        in_cluster=in_cluster, # if set to true, will look in the cluster, if false, looks for file
        cluster_context='docker-for-desktop', # is ignored when in_cluster is set to True
        config_file=config_file,
        resources=compute_resource,
        is_delete_operator_pod=True,
        get_logs=True)