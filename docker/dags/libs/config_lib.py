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

def read_config(api_name):
    try:
        CONFIG_API_URL=f"{BASE_CONFIG_API_URL}/api/{api_name}/"
        response = get_with_retries(CONFIG_API_URL)
        response.raise_for_status()
        
        config_lower = response.json()
        config = {key.upper(): value for key, value in config_lower.items()}
        
        logger.info(f"Successfully retrieved configuration from {CONFIG_API_URL}")
        return config
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch configuration from {CONFIG_API_URL}. Error: {e}")
        raise Exception(f"Error fetching configuration from {CONFIG_API_URL}: {e}")
    
def read_chess_config():
    return read_config("config")
    
def read_lichess_config():
    return read_config("liconfig")