import json

from libs.logging_lib import setup_logger
logger = setup_logger(__name__)

# Load configuration from the JSON file
CONFIG_PATH_CHESS  = '/opt/airflow/dags/configs/config_chess.json'

def read_chess_config():
    try:
        with open(CONFIG_PATH_CHESS) as config_file:
            config = json.load(config_file)
            if config.get("BASE_URL_TWIC") is None:
                logger.error(f"BASE_URL TWIC is missing in the configuration file {CONFIG_PATH_CHESS}")
                raise ValueError("BASE_URL TWIC is missing in the configuration file.")
            logger.info(f"Read Chess Config {CONFIG_PATH_CHESS} successfull.")
            return config
    except FileNotFoundError:
        logger.error(f"Reading Chess Config {CONFIG_PATH_CHESS} not successfull.")
        raise Exception(f"Configuration file not found at {CONFIG_PATH_CHESS}")
