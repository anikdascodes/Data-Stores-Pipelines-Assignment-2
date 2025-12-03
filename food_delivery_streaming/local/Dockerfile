# spark image for the pipeline
# Anik Das - 2025EM1100026

FROM apache/spark-py:latest

USER root

# install uv for python packages
RUN apt-get update && apt-get install -y \
    curl \
    && curl -LsSf https://astral.sh/uv/install.sh | sh \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.local/bin:$PATH"

WORKDIR /opt/spark/work-dir

COPY requirements.txt .

# install deps
RUN uv pip install --system --no-cache -r requirements.txt

COPY ./producers ./producers
COPY ./consumers ./consumers
COPY ./configs ./configs
COPY ./scripts ./scripts
COPY ./db ./db

RUN mkdir -p /datalake/output/orders \
    /datalake/checkpoints/orders \
    /datalake/lastprocess/orders \
    && chmod -R 777 /datalake

ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

RUN chmod +x ./scripts/*.sh

CMD ["tail", "-f", "/dev/null"]