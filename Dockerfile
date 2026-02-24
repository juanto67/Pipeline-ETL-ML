FROM apache/airflow:3.1.0-python3.10

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt