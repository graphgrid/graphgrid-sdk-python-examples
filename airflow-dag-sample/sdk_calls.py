import fire
import typing
from graphgrid_sdk.ggcore.config import SdkBootstrapConfig
from graphgrid_sdk.ggcore.sdk_messages import SaveDatasetResponse
from graphgrid_sdk.ggsdk.sdk import GraphGridSdk


class Pipeline:

    def configure_sdk(self, access_key, secret_key, url_base, is_docker_context):
        conf = SdkBootstrapConfig(
            access_key=access_key,
            secret_key=secret_key,
            url_base=url_base,
            is_docker_context=is_docker_context)
        return GraphGridSdk(conf)

    def save_dataset(self, sdk: GraphGridSdk, read_by_line: callable, dataset_name: str, overwrite: bool):
        dataset_response: SaveDatasetResponse = sdk.save_dataset(read_by_line,
                                                                 dataset_name,
                                                                 overwrite)
        if dataset_response.status_code != 200:
            raise Exception("Failed to save dataset: ", dataset_response.exception)


    def train_and_promote(self, sdk: GraphGridSdk, models_to_train: list, datasets: typing.Union[str, list],
                          no_cache: typing.Optional[bool], gpu: typing.Optional[bool], autopromote: bool,
                          success_handler: typing.Optional[callable], failed_handler: typing.Optional[callable]):
        sdk.nmt_train_pipeline(models_to_train, datasets, no_cache, gpu, autopromote, success_handler, failed_handler)


def main():
    fire.Fire(Pipeline)


if __name__ == '__main__':
    main()
