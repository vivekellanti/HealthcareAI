import requests
import os
from src.utils.cms_data_utils import *
from requests.adapters import HTTPAdapter


#CMS data url
API_URL = "https://data.cms.gov/data-api/v1/dataset/690ddc6c-2767-4618-b277-420ffb2bf27c/data"
#Logging configuration
logging.basicConfig(level=logging.INFO)

#retries and timeout for requests
session = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

def fetch_inpatient_data():
    try:
        response = requests.get(API_URL)
        response.raise_for_status()

        if response.status_code == 200:
            data = response.json()
            print(f"Fetched {len(data)} records from CMS Hospital Cost API")
            return data
    except requests.exceptions.HTTPError as http_err:
       logging.error(f"HTTP error: {http_err}")
    except requests.exceptions.Timeout as timeout_err:
        logging.error(f"Timeout error: {timeout_err}")
    except Exception as e:
        logging.error(f"Other error occurred: {e}")

if __name__ == "__main__":
    raw_data = fetch_inpatient_data()
    if raw_data:
        preprocessed_data = preprocess_data(raw_data)
        if preprocessed_data:
            # Use environment variable for S3 bucket name
            BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'healthaibucket')
            store_in_s3(preprocessed_data, 'preprocessed_inpatient_data.json', bucket_name=BUCKET_NAME)

    
       