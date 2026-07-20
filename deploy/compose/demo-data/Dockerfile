FROM python:3.12-slim
RUN pip install --no-cache-dir "pyiceberg[pyarrow]>=0.7"
COPY load_arxiv.py /load_arxiv.py
COPY build_arxiv_slice.py /build_arxiv_slice.py
ENTRYPOINT ["python", "/load_arxiv.py"]
