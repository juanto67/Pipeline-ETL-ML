from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}


dag = DAG(
    dag_id="etl_ml_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "ml"],
)


extract_task = BashOperator(
    task_id="extract_data",
    bash_command="python /opt/airflow/src/etl/extract.py",
    dag=dag,
)

transform_task = BashOperator(
    task_id="transform_data",
    bash_command="python /opt/airflow/src/etl/transform.py",
    dag=dag,
)

load_task = BashOperator(
    task_id="load_data",
    bash_command="python /opt/airflow/src/etl/load.py",
    dag=dag,
)


extract_task >> transform_task >> load_task