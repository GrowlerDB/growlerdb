FROM python:3.12-slim
RUN pip install --no-cache-dir "pyiceberg[pyarrow]>=0.7"
COPY load_movies.py /load_movies.py
COPY build_movies_slice.py /build_movies_slice.py
ENTRYPOINT ["python", "/load_movies.py"]
