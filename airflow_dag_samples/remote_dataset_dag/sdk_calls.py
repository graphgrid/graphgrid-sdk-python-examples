import typing

import boto3
import fire
from graphgrid_sdk.ggcore.sdk_messages import SaveDatasetResponse
from graphgrid_sdk.ggsdk.sdk import GraphGridSdk


class Pipeline:

    def save_dataset(self, access_key: str, secret_access_key: str, bucket: str, dataset_key: str, endpoint_url: str):
        sdk = GraphGridSdk()

        s3 = boto3.client('s3', endpoint_url=endpoint_url, aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)

        def read_dataset():
            data = s3.get_object(Bucket=bucket, Key=dataset_key)
            contents = data['Body'].read()
            return contents.decode("utf-8")

        yield_function = read_dataset()
        dataset_response: SaveDatasetResponse = sdk.save_dataset(yield_function,
                                                                 "training_dataset.jsonl")
        if dataset_response.status_code != 200:
            raise Exception("Failed to save dataset: ",
                            dataset_response.exception)

        return dataset_response.dataset_id

    def train_and_promote(self, models_to_train: list,
                          dataset_id: str,
                          no_cache: typing.Optional[bool],
                          gpu: typing.Optional[bool], autopromote: bool):
        sdk = GraphGridSdk()

        def success_handler(args):
            return

        def failed_handler(args):
            return

        sdk.nmt_train_pipeline(models_to_train, dataset_id, no_cache, gpu,
                               autopromote, success_handler, failed_handler)


def main():
    fire.Fire(Pipeline)


if __name__ == '__main__':
    main()
