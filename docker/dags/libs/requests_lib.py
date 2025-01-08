import requests
import os
from requests.exceptions import HTTPError
from tenacity import retry, stop_after_attempt, wait_exponential

from libs.logging_lib import setup_logger
logger = setup_logger(__name__)

@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=4, max=10))
def download_zip_file(url, file_path, file_name=None, ua=None):
    if ua is not None:
        try:
            logger.info(f"Trying to download with User Agent {ua}")
            with requests.get(url, headers={"User-Agent": ua}, stream=True) as response:
                # Check for HTTP errors (4xx, 5xx status codes)
                response.raise_for_status()  # This will raise HTTPError for bad responses
                with open(file_path, 'wb') as out_file:
                    for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB chunks
                        out_file.write(chunk)
                logger.info(f"File successfully downloaded to {file_path}")
        except HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
            raise HTTPError(f"HTTP error occurred: {http_err}") 
        except Exception as err:
            logger.error(f"Other error occurred: {err}")  # Handle other errors (e.g., connection issues)
            raise Exception(f"Other error occurred: {err}")
    else:
        try:
            logger.info(f"Trying to download file {url}")
            response = requests.get(url)
            if response.status_code == 200:
                if file_name is not None:
                    file_path = os.path.join(file_path, file_name)
                logger.info(f"Writing to {file_path}")
                with open(file_path, 'wb') as file:
                    file.write(response.content)
                logger.info('File downloaded successfully')
                return
            else:
                logger.error(f'Failed to download file. HTTP Error: {response.status_code}')
                logger.error(response.content)

        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading the file: {e}")
        raise
