#
# Downloads from: https://database.lichess.org/#evals
#

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import subprocess
import json
from libs.file_lib import check_for_file, del_file, can_create_file
from libs.logging_lib import setup_logger
logger = setup_logger(__name__) 
# Load configuration from the JSON file
from libs.config_lib import read_chess_config, read_lichess_config
config = read_chess_config()
liconfig = read_lichess_config()

# Define constants
DOWNLOAD_FOLDER = liconfig.get("DOWNLOAD_FOLDER")
UNZIP_FOLDER = os.path.join(os.getenv(liconfig.get("UNZIP_VOLUME")),liconfig.get("UNZIP_FOLDER"))

OUTPUT_FOLDER = "b"
CHUNK_SIZE = liconfig.get("CHUNK_SIZE")

def unzst_and_split(**kwargs):
    volume = os.getenv(liconfig.get("UNZIP_VOLUME"))
    logger.info(f"Download folder: {DOWNLOAD_FOLDER}, Unzip folder: {UNZIP_FOLDER}, {volume}")
    """Unzst the file and split it into smaller chunks."""
    filename = check_for_file(DOWNLOAD_FOLDER, "lichess*eval.jsonl.zst")
    if not filename:
        logger.info(f"No file to process. Folder:{DOWNLOAD_FOLDER}")
        return
    
    if not can_create_file(UNZIP_FOLDER):
        logger.error(f"Cannot write to folder {UNZIP_FOLDER}")
        return

    input_path = os.path.join(DOWNLOAD_FOLDER, filename)
    output_base = os.path.join(UNZIP_FOLDER, filename.replace(".zst", ""))

    
    # Call the shell script
    script_path = "./scripts/unzst.sh"
    chunk_size = str(CHUNK_SIZE)

    try:
        subprocess.run(
            [script_path, input_path, UNZIP_FOLDER, chunk_size, output_base],
            check=True
        )
        logger.info(f"File {filename} successfully decompressed and split.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error occurred during file processing: {e}")


def align_json_files(**kwargs):
    if 1==1:
        return
    """Align JSON objects across files and count them."""
    json_files = [f for f in os.listdir(OUTPUT_FOLDER) if f.startswith("part_")]
    total_json_objects = 0

    buffer = ""
    for filename in json_files:
        input_path = os.path.join(OUTPUT_FOLDER, filename)
        temp_path = os.path.join(OUTPUT_FOLDER, f"temp_{filename}")

        with open(input_path, 'r') as infile, open(temp_path, 'w') as outfile:
            for line in infile:
                buffer += line.strip()
                try:
                    json_obj = json.loads(buffer)
                    json.dump(json_obj, outfile)
                    outfile.write("\n")
                    buffer = ""
                    total_json_objects += 1
                except json.JSONDecodeError:
                    continue  # Wait for the rest of the JSON object

        os.replace(temp_path, input_path)

    logger.info(f"Total JSON objects across all files: {total_json_objects}")
    return total_json_objects

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'li_eval',
    default_args=default_args,
    description='A workflow to evaluate files in li_eval',
    schedule_interval=None,
)


unzst_and_split_task = PythonOperator(
    task_id='unzst_and_split',
    python_callable=unzst_and_split,
    provide_context=True,
    dag=dag,
)

align_json_files_task = PythonOperator(
    task_id='align_json_files',
    python_callable=align_json_files,
    provide_context=True,
    dag=dag,
)

# Define the task dependencies
unzst_and_split_task >> align_json_files_task
