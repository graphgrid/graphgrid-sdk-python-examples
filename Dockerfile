FROM python:3.8-slim

RUN apt update && apt -y install && apt -y upgrade \
    bash
RUN pip install --upgrade pip
RUN apt-get -y install git

WORKDIR /graphgrid-sdk-python-examples
COPY airflow-dag-sample/sdk_calls.py sdk_calls.py
COPY airflow-dag-sample/requirements.txt requirements.txt
COPY airflow-dag-sample/dataset_example.jsonl dataset_example.jsonl

RUN python3 -m pip install -r requirements.txt

ENTRYPOINT ["python3", "sdk_calls.py"]
CMD ["--help"]

