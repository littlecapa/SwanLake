FROM apache/airflow:2.7.2-python3.9
USER root
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
USER airflow
