import boto3
from botocore.client import Config
import config
import pandas as pd
import os
import re
import requests

def get_s3_client():
    """Create and return an S3 client connected to MinIO"""
    return boto3.client(
        's3',
        endpoint_url=config.DATA_LAKE["endpoint_url"],
        aws_access_key_id=config.DATA_LAKE["access_key"],
        aws_secret_access_key=config.DATA_LAKE["secret_key"],
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def upload_to_s3(s3_client, data, bucket_name, s3_key, is_refined=False):
    """
    Upload data to S3 bucket in CSV format
    :param s3_client: Boto3 S3 client
    :param data: Data to upload
    :param bucket_name: S3 bucket name
    :param s3_key: S3 key/path
    """
    try:
        # Convert to DataFrame if needed
        if not isinstance(data, pd.DataFrame):
            data_df = pd.DataFrame(data)
        else:
            data_df = data
        
        # Update file extension if needed
        if s3_key.endswith('.json'):
            s3_key = s3_key.replace('.json', '.csv')
        
        # Convert to CSV string
        csv_buffer = data_df.to_csv(sep=';', index=False, encoding='utf-8')
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_buffer.encode('utf-8')
        )
        print(f"Successfully uploaded {s3_key}")
        return s3_key
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return None


def download_file(url, local_filename):
    """
    Download a file from a given URL
    
    :param url: URL to download from
    :param local_filename: Local path to save the file
    :return: Local path of the downloaded file
    """
    print(f"Téléchargement des données depuis {url}...")
    try:
        #directory exists ?
        os.makedirs(os.path.dirname(local_filename), exist_ok=True)
        
        #dl file
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"Téléchargement terminé: {local_filename}")
        return local_filename
    except Exception as e:
        print(f"Error downloading file: {e}")
        return None

def cleanup_raw_files(s3_client, bucket_name, raw_key):
    """
    Remove raw files from S3 after processing

    :param s3_client: Boto3 S3 client
    :param bucket_name: S3 bucket name
    :param raw_key: S3 key of the raw file to delete
    """
    try:
        s3_client.delete_object(
            Bucket=bucket_name,
            Key=raw_key
        )
        print(f"Successfully deleted raw file: {raw_key}")
    except Exception as e:
        print(f"Error deleting raw file {raw_key}: {e}")

def upload_with_cleanup(s3_client, data, bucket_name, raw_key, refined_key, is_refined=False):
    """
    Upload data to S3 and then clean up raw files

    :param s3_client: Boto3 S3 client
    :param data: Data to upload (DataFrame or dict)
    :param bucket_name: S3 bucket name
    :param raw_key: S3 key for raw data
    :param refined_key: S3 key for refined data
    :param is_refined: Flag to determine if additional processing is needed
    """
    try:
        # Update file extension if needed
        if refined_key.endswith('.json'):
            refined_key = refined_key.replace('.json', '.csv')
        
        # Upload to refined
        uploaded_key = upload_to_s3(s3_client, data, bucket_name, refined_key, is_refined)
        
        if uploaded_key:
            # Cleanup raw file if it's a S3 key (not a local file)
            if raw_key.startswith('raw/'):
                cleanup_raw_files(s3_client, bucket_name, raw_key)
        else:
            print(f"Skipping cleanup because upload failed for {refined_key}")
    except Exception as e:
        print(f"Error in upload with cleanup: {e}")




def filter_stations(df, station_list, col_name):
    """
    Filter DataFrame to include only specified stations
    
    :param df: DataFrame
    :param station_list: kept stations list
    :return: Filtered DataFrame
    """
    # # Create a regex pattern that matches any of the stations
    # station_pattern = '|'.join([
    #     f'.*{re.escape(station)}.*' for station in station_list
    # ])
    
    # # Try to find the column with station names
    # station_columns = [col for col in df.columns if df[col].dtype == 'object']
    
    # # Filter stations across potential station columns
    # filtered_df = df[df[station_columns].apply(
    #     lambda row: row.str.contains(station_pattern, case=False, na=False).any(), axis=1
    # )]

    # Création de l'expression régulière (on échappe les caractères spéciaux comme les accents si nécessaire)
    pattern = r'(' + '|'.join(station_list) + r')'

    # Filtrage : ignore la casse, respecte les accents
    df_filtered = df[df[col_name].str.contains(pattern, flags=re.IGNORECASE, regex=True, na=False)]

    return df_filtered