import typing
import os

import boto3
import fire
from graphgrid_sdk.ggcore.sdk_messages import SaveDatasetResponse
from graphgrid_sdk.ggsdk.sdk import GraphGridSdk




class Pipeline:

    def download_dataset(self, access_key: str, secret_access_key: str, bucket: str, dataset_key: str, endpoint_url: str):
        s3 = boto3.client('s3', endpoint_url=endpoint_url, aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)
        s3.download_file(bucket, dataset_key, os.path.join(os.getcwd(), "training_dataset.jsonl"))

    def save_dataset(self, dataset_filepath: str, filename: str):
        sdk = GraphGridSdk()

        def read_by_line(dataset_filepath):
            infile = open(
                dataset_filepath,
                'r', encoding='utf8')
            for line in infile:
                yield line.encode()

        yield_function = read_by_line(dataset_filepath)
        dataset_response: SaveDatasetResponse = sdk.save_dataset(yield_function,
                                                                 filename)
        if dataset_response.status_code != 200:
            raise Exception("Failed to save dataset: ",
                            dataset_response.exception)

        return dataset_response.datasetId

    def train_and_promote(self, models_to_train: list,
                          datasetId: str,
                          no_cache: typing.Optional[bool],
                          gpu: typing.Optional[bool], autopromote: bool):
        sdk = GraphGridSdk()

        def success_handler(args):
            return

        def failed_handler(args):
            return

        sdk.nmt_train_pipeline(models_to_train, datasetId, no_cache, gpu,
                               autopromote, success_handler, failed_handler)


def main():
    fire.Fire(Pipeline)


if __name__ == '__main__':
    main()
