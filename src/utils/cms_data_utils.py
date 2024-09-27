import os
import gzip
import json
import logging
import pandas as pd
import boto3
from urllib3.util.retry import Retry
from io import BytesIO
import requests

# Logging configuration
logging.basicConfig(level=logging.INFO)

# S3 setup
s3 = boto3.client('s3')

def preprocess_data(data):
    """
    Preprocess raw data by dropping missing values and normalizing column names.
    
    Args:
    - data: List of raw data records
    
    Returns:
    - JSON string of preprocessed data
    """
    try:
        df = pd.DataFrame(data)
        df.dropna(inplace=True)
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        preprocessed_data = df.to_json(orient='records')
        logging.info(f"Preprocessed data contains {len(df)} records.")
        return preprocessed_data
    except Exception as e:
        logging.error(f"Error during preprocessing: {e}")
        return None

def compress_data(data):
    """Compress the preprocessed data using Gzip."""
    try:
        out = BytesIO()
        with gzip.GzipFile(fileobj=out, mode="w") as f:
            f.write(data.encode('utf-8'))
        return out.getvalue()
    except Exception as e:
        logging.error(f"Error during compression: {e}")
        return None

def store_in_s3(data, file_name, bucket_name, content_type='application/json'):
    """Upload preprocessed (and possibly compressed) data to S3."""
    try:
        s3.put_object(
            Body=data,
            Bucket=bucket_name,
            Key=file_name,
            ContentType=content_type
        )
        logging.info(f"Data successfully uploaded to S3 as {file_name}")
    except Exception as e:
        logging.error(f"Error uploading data to S3: {e}")
