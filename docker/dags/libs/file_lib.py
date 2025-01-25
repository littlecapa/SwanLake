import os, zipfile, fnmatch

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

def check_for_file(folder, filename_pattern):
    """Check if a new .zst file is in the input folder."""
    if not os.path.isdir(folder):
        logger.error(f"Folder {folder} does not exist.")
        raise FileNotFoundError(f"Folder {folder} does not exist.")
    for filename in os.listdir(folder):
        if fnmatch.fnmatch(filename, filename_pattern):  # Matches the pattern
            logger.info(f"New file found: {filename}")
            return filename  # Return the name of the new file
    logger.info("No new file found.")
    return None

def can_create_file(folder_path):
    """Check if a file can be created in the given folder."""
    test_file = os.path.join(folder_path, ".test_write_permission")
    try:
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        return True
    except (IOError, OSError):
        logger.error(f"Cannot write to folder {folder_path}")
        raise PermissionError(f"Cannot write to folder {folder_path}")
