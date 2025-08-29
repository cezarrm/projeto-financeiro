FROM apache/airflow:2.7.1-python3.11

USER airflow
RUN pip install --no-cache-dir boto3 pandas requests pyarrow fastparquet

USER airflow