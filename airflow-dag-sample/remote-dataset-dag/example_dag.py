from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from docker import APIClient
from graphgrid_provider.operators.graphgrid_docker import \
    GraphGridDockerOperator, GraphGridMount

DOCKER_URL = "tcp://socat:2375"
SOURCE = "{{ ti.xcom_pull(task_ids='create_volume') }}"
dataset_filepath = '../embedded-dataset-dag/dataset_example.jsonl'
access_key = "minio"
secret_access_key = "minio123"
filename = 'sample_dataset'
models_to_train = '["named_entity_recognition", "part_of_speech_tagging"]'


def read_by_line():
    infile = open(
        dataset_filepath,
        'r', encoding='utf8')
    for line in infile:
        yield line.encode()


default_args = {
    'owner': 'GraphGrid',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2015, 6, 1),
}

dag = DAG('train_model_with_sdk',
          default_args=default_args,
          description='train a model using sdk calls',
          schedule_interval="@daily",
          catchup=False,
          user_defined_macros=dict()
          )


def create_volume() -> str:
    """Create volume to propagate data between tasks

    :return: The name of the created persistent volume claim

    """
    client = APIClient(base_url=DOCKER_URL)
    volume = client.create_volume()
    return volume.get("Name")


def delete_volume(claim_name: str) -> None:
    """

    Delete the volume that propagated data between tasks

    :param claim_name: The name of the claim to delete

    """
    client = APIClient(base_url=DOCKER_URL)
    client.remove_volume(name=claim_name)


t_create_volume = PythonOperator(python_callable=create_volume,
                                 task_id='create_volume', dag=dag)

t_0 = GraphGridDockerOperator(task_id='download_dataset',
                              dag=dag,
                              mounts=[GraphGridMount(target="/volumes/",
                                                     source=SOURCE)],
                              image="graphgrid-sdk-python-examples",
                              command=["download_dataset",
                                       "--access_key", access_key,
                                       "--secret_access_key", secret_access_key,
                                       "--bucket", "com-graphgrid-datasets",
                                       "--dataset_key",
                                       "4l1heG30Et7NFUMO6ZCvphPhbzsvQKfvuBTWh4GGjntL/sample_dataset.jsonl",
                                       "--endpoint_url", "http://minio:9000"],
                              auto_remove=True,
                              )

t_1 = GraphGridDockerOperator(task_id='save_dataset',
                              dag=dag,
                              mounts=[GraphGridMount(target="/volumes/",
                                                     source=SOURCE)],
                              image="graphgrid-sdk-python-examples",
                              command=["save_dataset",
                                       "--dataset_filepath", dataset_filepath,
                                       "--filename", filename],
                              auto_remove=True,
                              )

t_2 = GraphGridDockerOperator(task_id='train_and_promote',
                              dag=dag,
                              mounts=[GraphGridMount(target="/volumes/",
                                                     source=SOURCE)],
                              image="graphgrid-sdk-python-examples",
                              command=["train_and_promote",
                                       "--models_to_train", models_to_train,
                                       "--datasetId",
                                       "{{ ti.xcom_pull(task_ids='save_dataset') }}",
                                       "--no_cache", 'false',
                                       "--gpu", 'false',
                                       "--autopromote", 'true'],
                              auto_remove=True,
                              )

t_delete_volume = PythonOperator(python_callable=delete_volume,
                                 task_id='delete_volume',
                                 dag=dag, op_kwargs={"claim_name": SOURCE},
                                 trigger_rule="all_done")

t_0.set_upstream(t_create_volume)
t_1.set_upstream(t_0)
t_2.set_upstream(t_1)
t_delete_volume.set_upstream(t_2)
