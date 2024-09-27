import json
import boto3
import pandas as pd
import requests
import time

# CMS data URL
API_URL = "https://data.cms.gov/data-api/v1/dataset/690ddc6c-2767-4618-b277-420ffb2bf27c/data"

# S3 setup
s3 = boto3.client('s3')
BUCKET_NAME = "healthaibucket"

def fetch_paged_data(size=30, total_pages=2):
    """Fetch paged data from the CMS API."""
    all_data = []

    for page in range(total_pages):
        offset = page * size
        url = f"{API_URL}?size={size}&offset={offset}"
        
        try:
            print(f"Fetching page {page + 1} with offset {offset}...")
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()  # Make sure to call this as a function
                all_data.extend(data)  # Append the current page data to the list
                print(f"Fetched {len(data)} records from page {page + 1}.")
            else:
                print(f"Failed to fetch page {page + 1}. Status Code: {response.status_code}")
                break

        except Exception as e:
            print(f"Error fetching page {page + 1}: {e}")
            break

        # Sleep to avoid overwhelming the server (optional)
        time.sleep(1)  # 1 second delay between requests (adjust as needed)

    return all_data

def preprocess_hospital_cost_data(data):
    """Preprocess and clean the Hospital Cost data."""
    try:
        df = pd.DataFrame(data)

        # Drop rows with missing (null) values
        df.dropna(inplace=True)

        # Normalize column names
        df.columns = df.columns.str.lower().str.replace(' ', '_')  # Replace spaces with underscores

        df.to_csv('hospital_cost_data.csv', index = False)
        print("Data saved to hospital_cost_data.csv")





        # Convert the DataFrame back to JSON
        preprocessed_data = df.to_json(orient='records')
        print(f"Preprocessed data contains {len(df)} records.")
        return preprocessed_data
    except Exception as e:
        print(f"Error during preprocessing: {e}")
        return None

def store_in_s3(data, file_name):
    """Store preprocessed data in S3."""
    try:
        s3.put_object(
            Body=data,
            Bucket=BUCKET_NAME,
            Key=file_name
        )
        print(f"Data successfully uploaded to S3 as {file_name}")
    except Exception as e:
        print(f"Error uploading data to S3: {e}")

def test_fetch_paged_data():
    """Test function for API requests."""
    raw_data = fetch_paged_data(size=50, total_pages=2)  # Fetch 2 pages of 50 records for testing
    
    if raw_data:
        # Preprocess the data
        preprocessed_data = preprocess_hospital_cost_data(raw_data)
        
        if preprocessed_data:
            # Save to S3 (you can comment this out for local testing if desired)
            store_in_s3(preprocessed_data, 'test_preprocessed_hospital_cost_data.json')

if __name__ == "__main__":
    test_fetch_paged_data()
