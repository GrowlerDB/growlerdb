FROM python:3.12-slim
RUN pip install --no-cache-dir "pyiceberg[pyarrow]>=0.7"
COPY seed.py /seed.py
ENTRYPOINT ["python", "/seed.py"]
