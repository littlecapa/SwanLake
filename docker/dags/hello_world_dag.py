from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
from libs.logging_lib import setup_logger
from libs.config_lib import read_chess_config

logger = setup_logger(__name__)

def log_all_xcoms(**kwargs):
    logger.info(f"Hello World DAG")
    # Load configuration from the JSON file
    config = read_chess_config()
    logger.info(f"Configuration: {config}")
    ti = kwargs['ti']
    """Logs XComs for all tasks in the current DAG run."""
    tasks = ti.xcom_pull(task_ids="unzip_iccf_archive")
    logger.info(f"XComs for DAG: {tasks}")
    if tasks is None:
        logger.info("No XComs found.")
        return
    for task_id in tasks:
        logger.info(f"Task-ID: {task_id}")
        xcom_value = ti.xcom_pull(task_ids=task_id)
        logger.info(f"Task ID: {task_id}, XCom Value: {xcom_value}")

# Define the DAG
dag = DAG(
    'hello_world_dag',
    description='Hello World',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

log_xcom_task = PythonOperator(
    task_id='log_all_xcoms',
    python_callable=log_all_xcoms,
    provide_context=True,
    dag=dag,
)