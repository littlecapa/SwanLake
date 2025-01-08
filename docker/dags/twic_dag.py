import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'libs')))

import zipfile
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from urllib.request import urlretrieve, URLError
from datetime import datetime
from libs.requests_lib import download_zip_file
from libs.config_lib import read_chess_config
from libs.file_lib import ensure_folder_exists, del_file, unzip_file

from libs.logging_lib import setup_logger
logger = setup_logger(__name__) 
# Load configuration from the JSON file
config = read_chess_config()

# Retrieve and validate values from the config
BASE_URL_TWIC = config.get("BASE_URL_TWIC")
logger.info(f"BASE_URL: {BASE_URL_TWIC}")
if not BASE_URL_TWIC:
    raise ValueError("BASE_URL is missing in the configuration file.")

VOLUME_TWIC = os.getenv(config.get("VOLUME_TWIC"))
logger.info(f"VOLUME_TWIC: {VOLUME_TWIC}, {config}")
if not VOLUME_TWIC:
    raise ValueError(f"Environment variable {config.get('VOLUME_TWIC')} is not set.")

TWIC_FOLDER = os.path.join(VOLUME_TWIC, config.get("TWIC_FOLDER"))
zst_folder = os.path.join(TWIC_FOLDER, config.get("SUB_FOLDER_ZST"))
pgn_folder = os.path.join(TWIC_FOLDER, config.get("SUB_FOLDER_UNZIPPED"))

for folder in [zst_folder, pgn_folder]:
    ensure_folder_exists(folder)

MAX_MISSING_TWIC = int(config.get("MAX_MISSING_TWIC", 1))
DEFAULT_NEXT_TWIC_NR = int(config.get("START_TWIC_NR", 1565))

def get_next_twic_nr(ti):
    """
    Retrieve the last downloaded year and month from Airflow's XCom.
    If not found, use default values.
    """
    last_twic_nr = ti.xcom_pull(key='last_twic_nr', task_ids='download_and_unzip_twic_archives', default=DEFAULT_NEXT_TWIC_NR)
    return last_twic_nr+1

def download_twic_archive(twic_number, **kwargs):
    """
    Downloads a TWIC archive and stores its number in XCom if successful.
    """
    logger.info(f"Start Downloading TWIC archive {twic_number}")  
    ti = kwargs['ti']
    try:
        url = f"{BASE_URL_TWIC}{twic_number}g.zip"
        file_path = os.path.join(zst_folder, f"twic{twic_number}.zip")
        logger.info(f"Attempting to download {url}")
        download_zip_file(url, file_path, config.get("User-Agent"))
        logger.info(f"Successfully downloaded archive {twic_number} to {file_path}")
        ti.xcom_push(key=f"twic_{twic_number}_file_path", value=file_path)
        ti.xcom_push(key='latest_twic_nr', value=twic_number)
        unzip_twic_archive(twic_number, **kwargs)
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        raise FileNotFoundError(f"TWIC archive {twic_number} not found at {url}.")

def unzip_twic_archive(twic_number, **kwargs):
    """
    Unzips a TWIC archive and deletes the archive after successful extraction.
    """
    logger.info(f"Start unzip TWIC archive {twic_number}")  
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key=f"twic_{twic_number}_file_path", task_ids='download_all_twic_archives')

    if not file_path:
        raise ValueError(f"No file path found for TWIC {twic_number} in XCom.")

    extract_path = pgn_folder
    logger.info(f"Extracting {file_path} to {extract_path} {twic_number}")  
    os.makedirs(extract_path, exist_ok=True)

    try:
        unzip_file(file_path, extract_path)
        return
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
    current_twic_number = get_next_twic_nr(ti)
    go_on = True
    while go_on:
        try:
            download_twic_archive(current_twic_number, **kwargs)
            current_twic_number += 1
        except Exception:
            go_on = False
            logger.warning(f"Archive {current_twic_number} is missing. Stopping Downloading")
        
    ti.xcom_push(key='latest_twic_number', value=current_twic_number-1)
    logger.info(f"Stopped downloading after TWIC {current_twic_number-1}.")

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
    provide_context=True,
    dag=dag,
)