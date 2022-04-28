import time

from graphgrid_sdk.ggcore.config import SdkBootstrapConfig
from graphgrid_sdk.ggcore.sdk_messages import SaveDatasetResponse
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

# List all models available in the dataset to be trained
models_to_train = ["translation", "sentiment-binary", "sentiment-categorical", "keyphrase-extraction",
                   "named-entity-recognition", "pos-tagging", "relation-extraction", "coreference-resolution"]
num_models = len(models_to_train)

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
train_request_bodies = []
for model in models_to_train:
    train_request_bodies.append(
        TrainRequestBody(model=model, datasets="sample-dataset.jsonl", no_cache=False, GPU=False))

job_train_responses = []
for i in range(num_models):
    job_train_responses.append(sdk.job_train(train_request_bodies[i], DAG_ID))
    time.sleep(2)  # a temp directory is created for each job with name based on timestamp.
    # This sleep ensures each job has its own temp directory

# Track training job status
job_status_responses = []
for i in range(num_models):
    job_status_responses.append((sdk.get_job_status(DAG_ID, job_train_responses[i].dag_run_id)))

completed_runs = 0
while completed_runs < num_models:
    print("...running dag...")
    time.sleep(10)
    for status in job_status_responses:
        if status.state != "success" and status.state != "failed":
            status_idx = job_status_responses.index(status)
            job_status_responses[status_idx] = sdk.get_job_status(DAG_ID,
                                                                  job_train_responses[status_idx].dag_run_id)
            if job_status_responses[status_idx].state == "success" or \
                    job_status_responses[status_idx].state == "failed":
                completed_runs = completed_runs + 1

# Training has finished
print("Dag training/eval/model upload has finished.")

# Get training job results
job_results_responses = []
for i in range(num_models):
    job_results_responses.append(sdk.get_job_results(DAG_ID, job_train_responses[i].dag_run_id))

# Promote updated models
for i in range(num_models):
    if job_results_responses[i].state == "success":
        # todo: add check for whether this model is better than currently loaded model
        sdk.promote_model(job_results_responses[i].saved_model_name, models_to_train[i])

# Promotion is complete
print("Models have been promoted.")
