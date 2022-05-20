# GraphGrid SDK Python Examples
Example usage of GraphGrid Python SDK

# Clone this repository
The first step to clone this repository. Using this bash command clone the repository.

```bash
git clone https://github.com/graphgrid/graphgrid-sdk-python-examples.git
```

# Start your Connected Data Platform (CDP)
The next step is to start running a local CDP deployment.

Download CDP ai-edition (version 2.0) from https://www.graphgrid.com/cdp-downloads/
and visit https://docs.graphgrid.com/2.0/#/ for more information.

# Build DAG docker image
We need to build a docker image based on our dockerfile. 
This image is what Airflow uses when the DAG is triggered.   

```bash
docker build -t sdk-graphgrid-sdk-python-examples -f graphgrid-sdk-python-example.dockerfile .
```

# Upload your DAG
With our DAG image built we now need to upload it to Airflow.

From within your CDP directory run the graphgrid command:

```bash
./bin/graphgrid airflow upload <PATH/TO/example_dag.py>
```

Be sure to replace `<PATH/TO/example_dag.py>` with the actual path to your DAG python file.

_Airflow may take up to 1 minute to add new DAGs to the Webserver UI._

# Kick off your DAG
We can start training NLP models!

You can use the Airflow Webserver browser to trigger your DAG (CDP defaults this to `localhost:8081`
and signing in with username/password `airflow`. 

From the home screen you should see your custom DAG (`train_model_with_sdk`) and the `nlp_model_training` DAG.

You can manually trigger your custom DAG by hitting the arrow under the `Actions` column.

You can monitor the jobs manually through the Airflow Webserver, 
or you can learn about how to use the [**GraphGrid SDK**](https://docs.graphgrid.com/2.0/#/) to programmatically monitor and interact with your model trainings.


# Done!

Following these steps you will have trained two new models,
a named entity recognition (ner) model and a part-of-speech tagging (pos) model.



These can be loaded in and used with the NLP Module of CDP.


# Visit the GraphGrid Docs site

Visit the [**GraphGrid docs site**](https://docs.graphgrid.com/2.0/#/) for more detailed information 
on running your custom NLP model training jobs and learning about the CDP platform.