import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from libs.logging_lib import setup_logger

logger = setup_logger(__name__)

# URL of the Django REST API endpoint with port
BASE_CONFIG_API_URL = "http://dj-dfm:8000"

def get_with_retries(url, retries=3, backoff_factor=0.3):
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504]  # Retry on these HTTP status codes
    )
    adapter = HTTPAdapter(max_retries=retry)
    # Mount the adapter once for both http and https
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session.get(url, timeout=10)

def read_chess_config():
    try:
        CHESS_CONFIG_API_URL=f"{BASE_CONFIG_API_URL}/api/config/"
        response = get_with_retries(CHESS_CONFIG_API_URL)
        response.raise_for_status()
        
        config_lower = response.json()
        config = {key.upper(): value for key, value in config_lower.items()}
        
        if config.get("BASE_URL_TWIC") is None:
            logger.error(f"BASE_URL_TWIC is missing in the configuration from {CHESS_CONFIG_API_URL}, see {config} {config_lower}")
            raise ValueError("BASE_URL_TWIC is missing in the configuration.")
        
        logger.info(f"Successfully retrieved chess configuration from {CHESS_CONFIG_API_URL}")
        return config
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch chess configuration from {CHESS_CONFIG_API_URL}. Error: {e}")
        raise Exception(f"Error fetching configuration: {e}")