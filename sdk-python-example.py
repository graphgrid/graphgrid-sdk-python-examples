import time

from graphgrid_sdk.ggcore.config import SdkBootstrapConfig
from graphgrid_sdk.ggcore.sdk_messages import GetJobStatusResponse, \
    JobTrainResponse, SaveDatasetResponse, GetJobResultsResponse, PromoteModelResponse
from graphgrid_sdk.ggsdk.sdk import GraphGridSdk


# stream dataset in
def read_by_line():
    infile = open(
        "dataset_example.jsonl",
        'r', encoding='utf8')
    for line in infile:
        yield line.encode()


DAG_ID = "nlp_model_training"

# setup bootstrap config
bootstrap_conf = SdkBootstrapConfig(
    access_key='a3847750f486bd931de26c6e683b1dc4',
    secret_key='81a62cea53883f4a163a96355d47656e',
    url_base='localhost',
    is_docker_context=False)

# Initialize the SDK
sdk = GraphGridSdk(bootstrap_conf)

# save training dataset (streamed)
dataset_response: SaveDatasetResponse = sdk.save_dataset(read_by_line(),
                                                         "sample-dataset",
                                                         overwrite=True)

# Train a new model
training_request_body = {"model": "named-entity-recognition",
                         "datasets": "sample-dataset.jsonl",
                         "no_cache": False,
                         "GPU": False}
train_response: JobTrainResponse = sdk.job_train(training_request_body, DAG_ID)

# Track training job status
job_status: GetJobStatusResponse = sdk.get_job_status(DAG_ID,
                                                      train_response.dag_run_id)
while job_status.state != "success" and job_status.state != "failed":
    print("...running dag...")
    time.sleep(10)
    job_status: GetJobStatusResponse = sdk.get_job_status(DAG_ID,
                                                          train_response.dag_run_id)

# Training has finished
print("Dag training/eval/model upload has finished.")

# Get training job results
job_results: GetJobResultsResponse = sdk.get_job_results(DAG_ID,
                                                         train_response.dag_run_id)

# Promote updated model
promote_model_response: PromoteModelResponse = \
      sdk.promote_model(job_results.saved_model_name, "named-entity-recognition")

# Promotion is complete
print("Model has been promoted.")
