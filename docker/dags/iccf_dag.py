import os
import logging
import zipfile
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from libs.requests_lib import download_zip_file
from libs.config_lib import read_chess_config
from libs.logging_lib import setup_logger
from libs.file_lib import unzip_file
logger = setup_logger(__name__) 


# Load configuration from the JSON file
config = read_chess_config()

VOLUME_ICCF = os.getenv(config.get("VOLUME_ICCF"))
VOLUME_IMPORT = os.getenv(config.get("VOLUME_IMPORT"))
logger.info(f"Volumes: {VOLUME_ICCF}, {VOLUME_IMPORT}")
if not VOLUME_ICCF or not VOLUME_IMPORT:
    raise ValueError(f"Environment variable {config.get('VOLUME_ICCF')} or {config.get('VOLUME_IMPORT')} is not set.")

BASE_URL_ICCF = config.get("BASE_URL_ICCF") 
DOWNLOAD_FOLDER = os.path.join(VOLUME_ICCF, config.get("SUB_FOLDER_ZST"))
UNZIP_FOLDER = os.path.join(VOLUME_IMPORT, config.get("FOLDER_IMPORT"))
logger.info(f"Download folder: {DOWNLOAD_FOLDER}, Unzip folder: {UNZIP_FOLDER}")
DEFAULT_LAST_YEAR = 2024
DEFAULT_LAST_MONTH = 11

def get_last_downloaded_date(**kwargs):
    """
    Retrieve the last downloaded year and month from Airflow's XCom.
    If not found, use default values.
    """
    ti = kwargs['ti']
    last_year = ti.xcom_pull(key='iccf_last_year', task_ids='check_and_download_iccf')
    last_month = ti.xcom_pull(key='iccf_last_month', task_ids='check_and_download_iccf')

    if last_year is None or last_month is None:
        return DEFAULT_LAST_YEAR, DEFAULT_LAST_MONTH

    return int(last_year), int(last_month)

def download_iccf_archive(**kwargs):
    """
    Check for a new ICCF archive and download it if available.
    """
    logger
    ti = kwargs['ti']
    last_year, last_month = get_last_downloaded_date(**kwargs)

    # Calculate the next month and year
    next_month = last_month + 1
    next_year = last_year
    if next_month > 12:
        next_month = 1
        next_year += 1

    # Format the filename and URL
    next_file = f"archive{next_year}{next_month:02d}.zip"
    url = f"{BASE_URL_ICCF}/{next_file}"

    try:
        download_zip_file(url, DOWNLOAD_FOLDER, file_name=next_file)
        ti.xcom_push(key='iccf_last_year', value=next_year)
        ti.xcom_push(key='iccf_last_month', value=next_month)
        ti.xcom_push(key='downloaded_file_path', value=DOWNLOAD_FOLDER)
        return
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        raise Exception(e)

def unzip_iccf_archive(**kwargs):
    """
    Unzip the downloaded ICCF archive.
    """
    logger.info("Unzipping ICCF archive.")
    ti = kwargs['ti']
    downloaded_file_path = ti.xcom_pull(key='downloaded_file_path', task_ids='check_and_download_iccf')

    if not downloaded_file_path:
        logger.info("No new file to unzip.")
        return

    try:
        unzip_file(downloaded_file_path, UNZIP_FOLDER)
    except Exception as e:
        raise

# Define the DAG
dag = DAG(
    'iccf_dag',
    description='Download and process ICCF archives',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

# Task to check and download ICCF archives
check_and_download_task = PythonOperator(
    task_id='check_and_download_iccf',
    python_callable=download_iccf_archive,
    provide_context=True,
    dag=dag,
)

# Task to unzip ICCF archives
unzip_task = PythonOperator(
    task_id='unzip_iccf_archive',
    python_callable=unzip_iccf_archive,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
check_and_download_task >> unzip_task
