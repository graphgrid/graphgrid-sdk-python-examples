# GraphGrid SDK Python Examples
Example usage of GraphGrid Python SDK

# Clone this repository
The first step to clone this repository. Using this bash command clone the repository.

```bash
git clone https://github.com/graphgrid/graphgrid-sdk-python-examples.git
```


# Start your Connected Data Platform (CDP)
The next step is to start running a local CDP deployment.

Download CDP version 2.0 from https://www.graphgrid.com/cdp-downloads/
and visit https://docs.graphgrid.com/2.0/#/ for more information.


# Write a DAG in python
Now we must write our own custom DAG in python. 
We use the GraphGridDockerOperator as the base of our DAG. 
```python


```

# Setup a dockerfile

# Build DAG docker image
We need to build a docker image based on our dockerfile. 
This image is what Airflow uses when the DAG is triggered.   

```bash
docker build -t sdk-tutorial -f sdk_tutorial.dockerfile .
```

# Upload your DAG
With our DAG image built we now need to upload it to Airflow.

```bash
graphgrid airflow upload </path/to/DAG.py>
```

_Airflow may take up to 1 minute to add new DAGs to the Webserver UI._

# Kick off your DAG
We can start training NLP models!
We'll show off two ways to trigger our DAG, either with the GraphGrid SDK or directly on the Airflow browser.

### SDK
You can trigger a directly through the GraphGrid SDK.

```python

import graphgrid-sdk

sdk = GraphGridSdk(SdkBootstrapConfig(
    access_key='abc123',
    secret_key='xyz123',
    url_base="localhost")
)

sdk.job_run( dag_id="custom-dag", request_body={})
```



### Airflow Browser
You can also trigger a DAG by going to your local Airflow browser (CDP defaults this to `localhost:8081`
and signing in with username/password `airflow`.

From the home screen you should see your custom DAG and the `nlp-model-training` DAG. 
You can manually trigger your custom DAG by hitting the green arrow under the `Actions` column. 