# Image for the streaming-demo producer + sink (Stages 1–2): pyiceberg to write Iceberg, and a
# pure-Python Kafka client (kafka-python, no native deps) to talk to Redpanda. One image, two
# entrypoints (selected by the compose `command`).
FROM python:3.12-slim
RUN pip install --no-cache-dir "pyiceberg[pyarrow]>=0.7" "kafka-python>=2.0"
COPY generator.py sink.py /pipeline/
WORKDIR /pipeline
