import os, zipfile

from libs.logging_lib import setup_logger
logger = setup_logger(__name__)

def ensure_folder_exists(folder_path, create=True):
    if not os.path.exists(folder_path):
        if create:
            os.makedirs(folder_path)
            logger.info(f"Folder {folder_path} created.")
            return
        logger.error(f"Folder {folder_path} already exists.")
        raise
    else:
        logger.info(f"Folder {folder_path} already exists.")

def del_file(file):
    os.remove(file)
    logger.info(f"Deleted {file}")

def concat_files(output_file, input_files, delete_input_files=True):
    # Concatenate files
    with open(output_file, "w") as outfile:
        for file in input_files:
            logger.info(f"Adding {file} to {output_file}")
            with open(file, "r") as infile:
                outfile.write(infile.read())

    logger.info("Concatenation complete. Deleting original PGN files.")

    if delete_input_files:
        for file in input_files:
            del_file(file)

def unzip_file(file_path, extract_path, delete_zip=True):
    # Unzip file
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    logger.info(f"Unzipped {file_path} to {extract_path}")
    if delete_zip:
        del_file(file_path)