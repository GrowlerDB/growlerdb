FROM python:3.12-slim
RUN pip install --no-cache-dir "pyiceberg[pyarrow]>=0.7"
COPY seed.py /seed.py
# Generator telemetry helper (uncompressed-corpus-bytes /metrics) — importable by the mounted
# workload corpus.py and the generator wrapper (both add "/" to sys.path). Stdlib only.
COPY genmetrics.py /genmetrics.py
ENTRYPOINT ["python", "/seed.py"]
