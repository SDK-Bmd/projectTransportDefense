"""
Utility functions for data lake operations
"""
import boto3
from botocore.client import Config
import json
import pandas as pd
from io import BytesIO
import logging
from datetime import datetime

# Import configuration
import sys
import os

# Add the parent directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from configuration.config import DATA_LAKE

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_lake_utils')


def get_s3_client():
    """Create and return an S3 client connected to MinIO"""
    return boto3.client(
        's3',
        endpoint_url=DATA_LAKE["endpoint_url"],
        aws_access_key_id=DATA_LAKE["access_key"],
        aws_secret_access_key=DATA_LAKE["secret_key"],
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )


def check_file_exists(bucket, key):
    """Check if a file exists in the data lake"""
    s3 = get_s3_client()
    try:
        s3.head_object(Bucket=bucket, Key=key)
        logger.info(f"File exists: {key}")
        return True
    except Exception as e:
        logger.warning(f"File does not exist: {key} - {str(e)}")
        return False


def save_json_to_data_lake(bucket, key, data):
    """Save JSON data to the data lake"""
    s3 = get_s3_client()
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data),
            ContentType="application/json"
        )
        logger.info(f"JSON data saved to {key}")
        return True
    except Exception as e:
        logger.error(f"Error saving JSON to {key}: {str(e)}")
        return False


def save_parquet_to_data_lake(bucket, key, dataframe):
    """Save Pandas DataFrame as Parquet to the data lake"""
    s3 = get_s3_client()
    try:
        parquet_buffer = BytesIO()
        dataframe.to_parquet(parquet_buffer)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=parquet_buffer.getvalue(),
            ContentType="application/octet-stream"
        )
        logger.info(f"Parquet data saved to {key}")
        return True
    except Exception as e:
        logger.error(f"Error saving Parquet to {key}: {str(e)}")
        return False


def read_json_from_data_lake(bucket, key):
    """Read JSON data from the data lake"""
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        logger.info(f"JSON data read from {key}")
        return json.loads(content)
    except Exception as e:
        logger.error(f"Error reading JSON from {key}: {str(e)}")
        return None


def read_parquet_from_data_lake(bucket, key):
    """Read Parquet data from the data lake as Pandas DataFrame"""
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        parquet_data = BytesIO(response['Body'].read())
        df = pd.read_parquet(parquet_data)
        logger.info(f"Parquet data read from {key}")
        return df
    except Exception as e:
        logger.error(f"Error reading Parquet from {key}: {str(e)}")
        return pd.DataFrame()


def list_files_in_data_lake(bucket, prefix=""):
    """List files in the data lake with optional prefix"""
    s3 = get_s3_client()
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = [item['Key'] for item in response.get('Contents', [])]
        logger.info(f"Listed {len(files)} files with prefix {prefix}")
        return files
    except Exception as e:
        logger.error(f"Error listing files with prefix {prefix}: {str(e)}")
        return []


def delete_older_files(bucket, prefix, max_files_to_keep):
    """Keep only the specified number of most recent files with a given prefix"""
    files = list_files_in_data_lake(bucket, prefix)

    # Sort files by name (assuming timestamp is part of the filename)
    files.sort(reverse=True)

    # Delete older files
    if len(files) > max_files_to_keep:
        s3 = get_s3_client()
        for file_to_delete in files[max_files_to_keep:]:
            try:
                s3.delete_object(Bucket=bucket, Key=file_to_delete)
                logger.info(f"Deleted old file: {file_to_delete}")
            except Exception as e:
                logger.error(f"Error deleting {file_to_delete}: {str(e)}")

    return True