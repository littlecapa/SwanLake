import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from libs.requests_lib import download_zip_file
from libs.config_lib import read_chess_config
from libs.logging_lib import setup_logger
from libs.zip_lib import unzip_file

logger = setup_logger(__name__) 

# Load configuration from the JSON file
config = read_chess_config()

VOLUME_IMPORT = os.getenv(config.get("VOLUME_IMPORT"))

BASE_URL_ICCF = config.get("BASE_URL_ICCF") 
DOWNLOAD_FOLDER = os.getenv(config.get("VOLUME_DOWNLOAD_ICCF"))
IMPORT_VOLUME = os.getenv(config.get("VOLUME_IMPORT"))
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
    last_month, last_year = get_month_year_keys(ti)

    if last_year is None or last_month is None:
        logger.info(f"No Month/Year Keys stored; using Defaults")
        return DEFAULT_LAST_MONTH, DEFAULT_LAST_YEAR

    return int(last_month), int(last_year)

def get_filename(month, year):
    filename = f"archive{year}{month:02d}.zip"
    logger.info(f"Filename: {filename}")
    return filename

def get_next_month_year(**kwargs):
    month, year = get_last_downloaded_date(**kwargs)
    next_month = month + 1
    next_year = year
    if next_month > 12:
        next_month = 1
        next_year += 1 
    logger.info(f"Next Month/Year: {next_month}/{next_year}")
    return next_month, next_year

def get_month_year_keys(ti):
    year = ti.xcom_pull(key='iccf_last_year', task_ids='check_and_download_iccf')
    month = ti.xcom_pull(key='iccf_last_month', task_ids='check_and_download_iccf')
    logger.info(f"Keys {month} {year}")
    return month, year

def push_keys(ti, month, year):
    ti.xcom_push(key='iccf_last_year', value=year)
    ti.xcom_push(key='iccf_last_month', value=month)

def download_iccf_archive(**kwargs):
    """
    Check for a new ICCF archive and download it if available.
    """
    ti = kwargs['ti']
    next_year, next_month = get_next_month_year(**kwargs)
    next_file = get_filename(next_month, next_year)
    url = f"{BASE_URL_ICCF}/{next_file}"

    try:
        download_zip_file(url, DOWNLOAD_FOLDER, file_name=next_file)
        push_keys(ti, next_month, next_year, DOWNLOAD_FOLDER)
        return
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        raise Exception(e)

def unzip_iccf_archive(**kwargs):
    """
    Unzip the downloaded ICCF archive.
    """
    month, year = get_next_month_year(**kwargs)
    next_archive_name = get_filename(month, year)
    logger.info(f"Unzipping ICCF archive. Looking for {next_archive_name}")
    ti = kwargs['ti']
    downloaded_file = os.path.join(DOWNLOAD_FOLDER, next_archive_name)

    try:
        unzip_file(downloaded_file, UNZIP_FOLDER)
        push_keys(ti, month, year)
        logger.info(f"Unzipping ICCF archive {next_archive_name} successfull")
        return
    except Exception as e:
        logger.info(f"Error unzipping ICCF archive {next_archive_name} {e}")
        raise Exception(e)

# Define the DAG
dag = DAG(
    'iccf_dag',
    description='Download and process ICCF archives',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

# Task to check and download ICCF archives
#check_and_download_task = PythonOperator(
#    task_id='check_and_download_iccf',
#    python_callable=download_iccf_archive,
#    provide_context=True,
#    dag=dag,
#)

# Task to unzip ICCF archives
unzip_task = PythonOperator(
    task_id='unzip_iccf_archive',
    python_callable=unzip_iccf_archive,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
#check_and_download_task >> unzip_task
