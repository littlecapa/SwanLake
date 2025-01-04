import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'libs')))

import json
import logging
import zipfile
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from urllib.request import urlretrieve, URLError
from datetime import datetime
from requests_lib import download_zip_file

# Get logger
logger = logging.getLogger(__name__)

# Get the container's hostname
container_name = os.getenv('HOSTNAME')  # This returns the container's hostname

# Log the container name
logging.info(f"Running on container: {container_name}")

# Load configuration from the JSON file
CONFIG_PATH = '/opt/airflow/dags/configs/config.json'
try:
    with open(CONFIG_PATH) as config_file:
        config = json.load(config_file)
except FileNotFoundError:
    raise Exception(f"Configuration file not found at {CONFIG_PATH}")

# Retrieve and validate values from the config
BASE_URL = config.get("BASE_URL")
logger.info(f"BASE_URL: {BASE_URL}")
if not BASE_URL:
    raise ValueError("BASE_URL is missing in the configuration file.")

VOLUME_TWIC = os.getenv(config.get("VOLUME_TWIC"))
logger.info(f"VOLUME_TWIC: {VOLUME_TWIC}, {config}")
if not VOLUME_TWIC:
    raise ValueError(f"Environment variable {config.get('VOLUME_TWIC')} is not set.")

TWIC_FOLDER = os.path.join(VOLUME_TWIC, config.get("TWIC_FOLDER"))
logger.info(f"TWIC_FOLDER: {TWIC_FOLDER}")
if not os.path.exists(TWIC_FOLDER):
    logger.info(f"Creating folder {TWIC_FOLDER}")
    os.makedirs(TWIC_FOLDER, exist_ok=True)
else:
    logger.info(f"Folder {TWIC_FOLDER} already exists") 

MAX_MISSING_TWIC = int(config.get("MAX_MISSING_TWIC", 1))

def download_twic_archive(twic_number, **kwargs):
    """
    Downloads a TWIC archive and stores its number in XCom if successful.
    """
    logger.info(f"Start Downloading TWIC archive {twic_number}")  
    ti = kwargs['ti']
    try:
        url = f"{BASE_URL}{twic_number}g.zip"
        file_path = os.path.join(TWIC_FOLDER, f"twic{twic_number}.zip")
        logger.info(f"Attempting to download {url}")
        download_zip_file(url, file_path, config.get("User-Agent"))
        logger.info(f"Successfully downloaded archive {twic_number} to {file_path}")
        ti.xcom_push(key=f"twic_{twic_number}_file_path", value=file_path)
        ti.xcom_push(key='latest_twic_number', value=twic_number)
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        raise FileNotFoundError(f"TWIC archive {twic_number} not found at {url}.")


def unzip_twic_archive(twic_number, **kwargs):
    """
    Unzips a TWIC archive.
    """
    logger.info(f"Start unzip TWIC archive {twic_number}")  
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key=f"twic_{twic_number}_file_path", task_ids='download_all_twic_archives')

    if not file_path:
        raise ValueError(f"No file path found for TWIC {twic_number} in XCom.")

    extract_path = os.path.join(TWIC_FOLDER, f"twic{twic_number}")
    logger.info(f"Extracting {file_path} to {extract_path} {twic_number}")  
    os.makedirs(extract_path, exist_ok=True)

    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        logger.info(f"Successfully extracted TWIC {twic_number} to {extract_path}")
    except zipfile.BadZipFile as e:
        logger.error(f"Error extracting {file_path}: {e}")
        raise

def find_and_download_archives(**kwargs):
    """
    Iteratively attempts to download TWIC archives starting from `start_twic_number`.
    Stops if `MAX_MISSING_TWIC` consecutive archives are missing.
    """
    logger.info(f"Start Finding TWIC archive")
    ti = kwargs['ti']  # Airflow task instance
    missing_count = 0
    current_twic_number = ti.xcom_pull(key='latest_twic_number', task_ids='download_all_twic_archives', default=1565)

    while missing_count < MAX_MISSING_TWIC:
        try:
            download_twic_archive(current_twic_number, **kwargs)
            missing_count = 0  # Reset missing count on success
            current_twic_number += 1
        except Exception:
            missing_count += 1
            logger.warning(f"Archive {current_twic_number} is missing. Missing count: {missing_count}")
        
    ti.xcom_push(key='latest_twic_number', value=current_twic_number)
    logger.info(f"Stopped downloading after {MAX_MISSING_TWIC} consecutive missing archives for TWIC {current_twic_number}.")

# Set up the DAG
dag = DAG(
    'download_and_unzip_twic_archives',
    description='Download and unzip TWIC archives starting from a specific number',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

# Task to download TWIC archives
download_archives_task = PythonOperator(
    task_id='download_all_twic_archives',
    python_callable=find_and_download_archives,
    op_args=[],  # Starting TWIC number
    provide_context=True,
    dag=dag,
)

# Task to unzip TWIC archives
unzip_task = PythonOperator(
    task_id='unzip_twic_archive',
    python_callable=unzip_twic_archive,
    op_args=[1565],  # Replace with dynamic handling for different archives
    provide_context=True,
    dag=dag,
)

# Define task dependencies
download_archives_task >> unzip_task
