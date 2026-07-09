FROM python:3.12-slim
RUN pip install --no-cache-dir "pyiceberg[pyarrow]>=0.7" numpy
COPY load_es.py /load_es.py
ENTRYPOINT ["python", "/load_es.py"]
