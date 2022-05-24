import typing

import fire
from graphgrid_sdk.ggcore.sdk_messages import SaveDatasetResponse
from graphgrid_sdk.ggsdk.sdk import GraphGridSdk


class Pipeline:

    def save_dataset(self, dataset_filepath: str, dataset_name: str,
                     overwrite: bool):
        sdk = GraphGridSdk()

        def read_by_line(dataset_filepath):
            infile = open(
                dataset_filepath,
                'r', encoding='utf8')
            for line in infile:
                yield line.encode()

        yield_function = read_by_line(dataset_filepath)
        dataset_response: SaveDatasetResponse = sdk.save_dataset(yield_function,
                                                                 dataset_name,
                                                                 overwrite)
        if dataset_response.status_code != 200:
            raise Exception("Failed to save dataset: ",
                            dataset_response.exception)

    def train_and_promote(self, models_to_train: list,
                          datasets: typing.Union[str, list],
                          no_cache: typing.Optional[bool],
                          gpu: typing.Optional[bool], autopromote: bool):
        sdk = GraphGridSdk()

        def success_handler(args):
            return

        def failed_handler(args):
            return

        sdk.nmt_train_pipeline(models_to_train, datasets, no_cache, gpu,
                               autopromote, success_handler, failed_handler)


def main():
    fire.Fire(Pipeline)


if __name__ == '__main__':
    main()
