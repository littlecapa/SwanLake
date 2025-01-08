import os
import shutil
import zipfile
import tempfile
import random
import string

from libs.logging_lib import setup_logger
logger = setup_logger(__name__)

def unzip_file(archive_path, target_folder):
    """
    Unzips an archive into a target folder with validation and cleanup.
    
    Args:
        archive_path (str): Path to the zip archive.
        target_folder (str): Path to the folder where files should be copied.
    
    Raises:
        FileNotFoundError: If the archive does not exist.
        ValueError: If any unzipped file is 0 bytes.
    """
    # Check if the archive exists
    if not os.path.exists(archive_path):
        raise FileNotFoundError(f"The archive '{archive_path}' does not exist.")
    
    # Create a random folder for extraction
    random_folder_name = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    temp_extract_path = os.path.join(tempfile.gettempdir(), random_folder_name)
    os.makedirs(temp_extract_path, exist_ok=True)
    logger.info(f"Temp Dir: {temp_extract_path}")

    try:
        # Unzip the archive into the temporary folder
        with zipfile.ZipFile(archive_path, 'r') as zip_ref:
            zip_ref.extractall(temp_extract_path)
        
        # Check that all files are bigger than 0 bytes
        for root, _, files in os.walk(temp_extract_path):
            for file in files:
                logger.info(f"Extracted File: {file}")
                file_path = os.path.join(root, file)
                if os.path.getsize(file_path) == 0:
                    raise ValueError(f"File '{file_path}' is empty (0 bytes).")
        
        # Ensure the target folder exists
        os.makedirs(target_folder, exist_ok=True)
        logger.info(f"Copy Files to {target_folder}")
        # Copy files to the target folder
        for root, dirs, files in os.walk(temp_extract_path):
            for file in files:
                src_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(root, temp_extract_path)
                dest_file_path = os.path.join(target_folder, relative_path, file)
                os.makedirs(os.path.dirname(dest_file_path), exist_ok=True)
                shutil.copy2(src_file_path, dest_file_path)
        
        # Delete the archive
        os.remove(archive_path)
    except Exception as e:
        logger.error(f"Extract Error: {e}")
        print(f"Error during unzip operation: {e}")
        raise
    finally:
        # Clean up: delete the temporary folder
        if os.path.exists(temp_extract_path):
            shutil.rmtree(temp_extract_path)
