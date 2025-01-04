import requests
from requests.exceptions import HTTPError

def download_zip_file(url, file_path, ua):
    try:
        with requests.get(url, headers={"User-Agent": ua}, stream=True) as response:
            # Check for HTTP errors (4xx, 5xx status codes)
            response.raise_for_status()  # This will raise HTTPError for bad responses
            with open(file_path, 'wb') as out_file:
                for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB chunks
                    out_file.write(chunk)
            print(f"File successfully downloaded to {file_path}")
    except HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        raise HTTPError(f"HTTP error occurred: {http_err}") 
    except Exception as err:
        print(f"Other error occurred: {err}")  # Handle other errors (e.g., connection issues)
        raise Exception(f"Other error occurred: {err}")
