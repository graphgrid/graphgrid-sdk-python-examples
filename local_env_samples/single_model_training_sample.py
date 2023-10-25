import time

from graphgrid_sdk.ggcore.config import SdkBootstrapConfig
from graphgrid_sdk.ggcore.sdk_messages import NMTStatusResponse, \
    NMTTrainResponse, SaveDatasetResponse, PromoteModelResponse
from graphgrid_sdk.ggcore.sdk_messages import TrainRequestBody
from graphgrid_sdk.ggcore.utils import NlpModel
from graphgrid_sdk.ggsdk.sdk import GraphGridSdk


# Stream dataset in
def read_by_line():
    infile = open(
        "dataset_example.jsonl",
        'r', encoding='utf8')
    for line in infile:
        yield line.encode()


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
                                                         "sample-dataset")

# Train a new model
training_request_body: TrainRequestBody = TrainRequestBody(model=NlpModel.NAMED_ENTITY_RECOGNITION,
                                                           dataset_id=dataset_response.dataset_id,
                                                           no_cache=False, gpu=False)
train_response: NMTTrainResponse = sdk.nmt_train(training_request_body)

# Track training status
nmt_status: NMTStatusResponse = sdk.nmt_status(train_response.dagRunId)
while nmt_status.state != "success" and nmt_status.state != "failed":
    print("...running dag...")
    time.sleep(10)
    nmt_status: NMTStatusResponse = sdk.nmt_status(train_response.dagRunId)

if nmt_status.state == "failed":
    raise Exception("Dag failed: ", nmt_status.exception)

# Training has finished
print("Dag training/eval/model upload has finished.")

# Promote updated model
# todo: add check for whether this model is better than currently loaded model
promote_model_response: PromoteModelResponse = \
    sdk.promote_model(nmt_status.savedModelName, "named-entity-recognition")

# Promotion is complete
print("Model has been promoted.")
