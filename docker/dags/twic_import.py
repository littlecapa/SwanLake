import os
import json
import glob
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from libs.config_lib import read_chess_config
from libs.file_lib import concat_files

# Get logger
from libs.logging_lib import setup_logger
logger = setup_logger(__name__) 

# Load configuration from the JSON file
config = read_chess_config()

VOLUME_TWIC = os.getenv(config.get("VOLUME_TWIC"))
VOLUME_TWIC_IMPORT = os.getenv(config.get("VOLUME_IMPORT"))
logger.info(f"Volumes: {VOLUME_TWIC}, {VOLUME_TWIC_IMPORT}")
if not VOLUME_TWIC or not VOLUME_TWIC_IMPORT:
    raise ValueError(f"Environment variable {config.get('VOLUME_TWIC')} or {config.get('VOLUME_IMPORT')} is not set.")

TWIC_FOLDER = os.path.join(VOLUME_TWIC, config.get("TWIC_FOLDER"))
pgn_folder = os.path.join(TWIC_FOLDER, config.get("SUB_FOLDER_UNZIPPED"))

import_folder = os.path.join(VOLUME_TWIC_IMPORT, config.get("FOLDER_IMPORT"))

def concat_pgn_files(**kwargs):
    """
    Concatenate all PGN files in the `pgn` subfolder into one file,
    store the new file in the `import` subfolder, and delete the original files.
    """
    logger.info("Starting PGN concatenation process")

    # Find all PGN files in the subfolder
    pgn_files = glob.glob(os.path.join(pgn_folder, "*.pgn"))

    if not pgn_files:
        logger.info("No PGN files found in the folder. Terminating process.")
        return

    # Create output filename based on current date
    timestamp = datetime.now().strftime("%Y_%m_%d")
    output_file = os.path.join(import_folder, f"twic_import_{timestamp}.pgn")

    logger.info(f"Output file will be: {output_file}")

    # Concatenate files
    concat_files(output_file, pgn_files)

# Set up the DAG
dag = DAG(
    'pgn_import',
    description='Concatenate and import PGN files',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

# Task to concatenate PGN files
concat_pgn_task = PythonOperator(
    task_id='concat_pgn_files',
    python_callable=concat_pgn_files,
    provide_context=True,
    dag=dag,
)
