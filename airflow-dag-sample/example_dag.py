from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from docker import APIClient
from airflow import DAG

from graphgrid_provider.operators.graphgrid_docker import \
    GraphGridDockerOperator, GraphgridMount


DOCKER_URL = "tcp://socat:2375"


def read_by_line():
    infile = open(
        "../dataset_example.jsonl",
        'r', encoding='utf8')
    for line in infile:
        yield line.encode()


# DAG_ID = "nlp_model_training"

# sdk = GraphGridSdk(SdkBootstrapConfig(
#     access_key='a3847750f486bd931de26c6e683b1dc4',
#     secret_key='81a62cea53883f4a163a96355d47656e',
#     url_base='localhost',
#     is_docker_context=False))

# training_request_body: TrainRequestBody = TrainRequestBody(model="named-entity-recognition",
#                                                            datasets="sample-dataset.jsonl",
#                                                            no_cache=False, gpu=False)

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

t_0 = GraphGridDockerOperator(task_id='configure_sdk',
                              dag=dag,
                              mounts=[GraphgridMount(target="/config/", source="{{ ti.xcom_pull(task_ids='create_volume') }}")],
                              image="graphgrid-sdk-python-examples",
                              command=["configure_sdk",
                                       "--access_key", 'a3847750f486bd931de26c6e683b1dc4',
                                       "--secret_key", '81a62cea53883f4a163a96355d47656e',
                                       "--url_base", "localhost",
                                       "--is_docker_context", True],
                              auto_remove=True,
                              do_xcom_push=True
                              )

t_1 = GraphGridDockerOperator(task_id='save_dataset',
                              dag=dag,
                              mounts=[GraphgridMount(target="/config/", source="{{ ti.xcom_pull(task_ids='create_volume') }}")],
                              image="graphgrid-sdk-python-examples",
                              command=["save_dataset",
                                       "--sdk", "{{ ti.xcom_pull(task_ids='configure_sdk') }}",
                                       "--read_by_line", read_by_line(),
                                       "--dataset_name", "sample-dataset",
                                       "--overwrite", False],
                              auto_remove=True,
                              )

t_2 = GraphGridDockerOperator(task_id='train_and_promote',
                              dag=dag,
                              mounts=[GraphgridMount(target="/config/", source="{{ ti.xcom_pull(task_ids='create_volume') }}")],
                              image="graphgrid-sdk-python-examples",
                              command=["start_training",
                                       "--sdk", "{{ ti.xcom_pull(task_ids='configure_sdk') }}",
                                       "--model_type", "named-entity-recognition",
                                       "--datasets", "sample-dataset.jsonl",
                                       "--no_cache", False,
                                       "--gpu", False],
                              auto_remove=True,
                              )

t_delete_volume = PythonOperator(python_callable=delete_volume,
                                 task_id='delete_volume',
                                 dag=dag, op_kwargs={"claim_name": "{{ ti.xcom_pull(task_ids='create_volume') }}"},
                                 trigger_rule="all_done")

t_0.set_upstream(t_create_volume)
t_1.set_upstream(t_0)
t_2.set_upstream(t_1)
t_delete_volume.set_upstream(t_2)


