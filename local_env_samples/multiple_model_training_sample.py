import time

from graphgrid_sdk.ggcore.config import SdkBootstrapConfig
from graphgrid_sdk.ggcore.sdk_messages import GetJobStatusResponse, \
    JobTrainResponse, SaveDatasetResponse, GetJobResultsResponse, PromoteModelResponse
from graphgrid_sdk.ggcore.training_request_body import TrainRequestBody
from graphgrid_sdk.ggsdk.sdk import GraphGridSdk


# Stream dataset in
def read_by_line():
    infile = open(
        "dataset_example.jsonl",
        'r', encoding='utf8')
    for line in infile:
        yield line.encode()


DAG_ID = "nlp_model_training"

# Setup bootstrap config
bootstrap_conf = SdkBootstrapConfig(
    access_key='a3847750f486bd931de26c6e683b1dc4',
    secret_key='81a62cea53883f4a163a96355d47656e',
    url_base='localhost',
    is_docker_context=False)

# Initialize the SDK
sdk = GraphGridSdk(bootstrap_conf)

# Save training dataset (streamed)
dataset_response: SaveDatasetResponse = sdk.save_dataset(read_by_line(),
                                                         "sample-dataset",
                                                         overwrite=True)

# Train new models
ner_training_request_body: TrainRequestBody = TrainRequestBody(model="named-entity-recognition",
                                                               datasets="sample-dataset.jsonl",
                                                               no_cache=False, GPU=False)
pos_training_request_body: TrainRequestBody = TrainRequestBody(model="pos-tagging", datasets="sample-dataset.jsonl",
                                                               no_cache=False,
                                                               GPU=False)

ner_train_response: JobTrainResponse = sdk.job_train(ner_training_request_body, DAG_ID)
time.sleep(2)  # a temp directory is created for each job with name based on timestamp.
# This sleep ensures each job has its own temp directory
pos_train_response: JobTrainResponse = sdk.job_train(pos_training_request_body, DAG_ID)

# Track training job status
ner_job_status: GetJobStatusResponse = sdk.get_job_status(DAG_ID,
                                                          ner_train_response.dag_run_id)
pos_job_status: GetJobStatusResponse = sdk.get_job_status(DAG_ID,
                                                          pos_train_response.dag_run_id)
while (ner_job_status.state != "success" and ner_job_status.state != "failed") or \
        (pos_job_status.state != "success" and pos_job_status.state != "failed"):
    print("...running dag...")
    time.sleep(10)
    if ner_job_status.state != "success" and ner_job_status.state != "failed":
        ner_job_status: GetJobStatusResponse = sdk.get_job_status(DAG_ID,
                                                                  ner_train_response.dag_run_id)
    if pos_job_status.state != "success" and pos_job_status.state != "failed":
        pos_job_status: GetJobStatusResponse = sdk.get_job_status(DAG_ID,
                                                                  pos_train_response.dag_run_id)

# Training has finished
print("Dag training/eval/model upload has finished.")

# Get training job results
ner_job_results: GetJobResultsResponse = sdk.get_job_results(DAG_ID,
                                                             ner_train_response.dag_run_id)
pos_job_results: GetJobResultsResponse = sdk.get_job_results(DAG_ID,
                                                             pos_train_response.dag_run_id)

# Promote updated models
if ner_job_status.state == "success":
    # todo: add check for whether this model is better than currently loaded model
    promote_ner_model_response: PromoteModelResponse = \
        sdk.promote_model(ner_job_results.saved_model_name, "named-entity-recognition")
if pos_job_status.state == "success":
    # todo: add check for whether this model is better than currently loaded model
    promote_pos_model_response: PromoteModelResponse = \
        sdk.promote_model(pos_job_results.saved_model_name, "pos-tagging")

# Promotion is complete
print("Models have been promoted.")
