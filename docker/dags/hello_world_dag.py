from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from libs.logging_lib import setup_logger
logger = setup_logger(__name__) 

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    'hello_world_dag',  # DAG ID
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,  # Do not run past scheduled tasks
) as dag:

    # Define a Python function for the task
    def say_hello():
        logger.info("Start_task 'say_hello'") 

    # Create a PythonOperator task
    hello_task = PythonOperator(
        task_id='say_hello',  # Task ID
        python_callable=say_hello,  # Python function to execute
    )

    # Task dependencies (not needed here as there's only one task)
    hello_task
