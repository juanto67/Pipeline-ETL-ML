FROM apache/airflow:3.1.0-python3.10

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root src/dags/ /opt/airflow/dags/