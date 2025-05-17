"""
Script d'extraction et d'upload des données GTFS d'ile de france mobilités
"""
import json
import os
import zipfile
import tempfile
import shutil
import requests
from datetime import datetime
import boto3
from botocore.client import Config

# Import local config from the same directory instead of parent
import config

# GTFS download URL
GTFS_URL = "https://data.iledefrance-mobilites.fr/api/datasets/1.0/offre-horaires-tc-gtfs-idfm/images/a925e164271e4bca93433756d6a340d1"

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

def download_gtfs_data(url, download_dir):
    """Download the GTFS zip file from the provided URL"""
    print(f"Downloading GTFS data from {url}...")
    
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise Exception(f"Failed to download GTFS data: {response.status_code}")
    
    zip_path = os.path.join(download_dir, "gtfs_data.zip")
    with open(zip_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    print(f"Download completed: {zip_path}")
    return zip_path

def extract_gtfs_files(zip_path, extract_dir):
    """Extract all GTFS files from the zip archive"""
    print(f"Extracting GTFS files to {extract_dir}...")
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Extract all files
        zip_ref.extractall(extract_dir)
        
        # List all extracted files
        all_files = zip_ref.namelist()
        
    # Get list of files (not directories) that were extracted
    extracted_files = []
    for root, dirs, files in os.walk(extract_dir):
        for file in files:
            # Get the relative path to maintain directory structure
            rel_path = os.path.relpath(os.path.join(root, file), extract_dir)
            extracted_files.append(rel_path)
    
    print(f"Extracted {len(extracted_files)} GTFS files")
    return extracted_files

def upload_to_minio(s3_client, local_dir, bucket_name, extracted_files, source_url):
    """Upload the extracted GTFS files to datalake and generate metadata"""
    print(f"Uploading GTFS files to datalake bucket '{bucket_name}'...")
    
    # Generate timestamp for the folder
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Get file extensions statistics
    file_extensions = {}
    for file_path in extracted_files:
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()  # Normalize extensions
        if ext in file_extensions:
            file_extensions[ext] += 1
        else:
            file_extensions[ext] = 1
    
    # Upload each file
    upload_count = 0
    for filepath in extracted_files:
        local_path = os.path.join(local_dir, filepath)
        
        # Check if the file exists and is not a directory
        if not os.path.isfile(local_path):
            print(f"Skipping {filepath} - not a file or doesn't exist")
            continue
            
        # Define S3 key with raw prefix
        s3_key = f"raw/gtfs/{timestamp}/{filepath}"
        
        try:
            with open(local_path, 'rb') as file_data:
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=file_data
                )
            
            # Also store the latest version
            latest_key = f"raw/gtfs/latest/{filepath}"
            with open(local_path, 'rb') as file_data:
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=latest_key,
                    Body=file_data
                )
            
            upload_count += 1
            print(f"Uploaded {filepath} to {s3_key}")
            
        except Exception as e:
            print(f"Error uploading {filepath}: {str(e)}")
    
    print(f"Uploaded {upload_count} files to datalake")
    
    # Create metadata file
    metadata = {
        "extraction_time": datetime.now().isoformat(),
        "source_url": source_url,
        "files_count": upload_count,
        "file_types": file_extensions
    }
    
    # Save metadata file to the same directory as the data files
    metadata_key = f"raw/gtfs/{timestamp}/metadata.json"
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2)
        )
        print(f"Metadata saved to {metadata_key}")
    except Exception as e:
        print(f"Error saving metadata: {str(e)}")
    
    return upload_count

def extract_gtfs_to_minio():
    """Main function to extract GTFS data and upload to datalake"""
    # Create temporary directory for downloads and extraction
    temp_dir = tempfile.mkdtemp()
    try:
        # Initialize MinIO client
        s3 = get_s3_client()
        bucket_name = config.DATA_LAKE["bucket_name"]
        
        # Download GTFS zip file
        zip_path = download_gtfs_data(GTFS_URL, temp_dir)
        
        # Extract GTFS files
        extract_dir = os.path.join(temp_dir, "extracted")
        os.makedirs(extract_dir, exist_ok=True)
        extracted_files = extract_gtfs_files(zip_path, extract_dir)
        
        # Upload to datalake with metadata generation
        upload_count = upload_to_minio(s3, extract_dir, bucket_name, extracted_files, GTFS_URL)
        
        print(f"GTFS data extraction completed. {upload_count} files uploaded to datalake.")
        
    except Exception as e:
        print(f"Error in GTFS extraction process: {str(e)}")
    finally:
        # Clean up temporary directory
        shutil.rmtree(temp_dir)
        print("Temporary files cleaned up")

if __name__ == "__main__":
    extract_gtfs_to_minio()