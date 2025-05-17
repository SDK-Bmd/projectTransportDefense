"""
Script d'extraction de prétraitement des données d'accessibilité & assenceurs de ile de france mobilité
"""
import pandas as pd
import config
from utils_extract import (
    get_s3_client, 
    download_file, 
    upload_with_cleanup,
    filter_stations
)

def extract_infra_data():
    # Initialise S3 client
    s3_client = get_s3_client()
    bucket_name = config.DATA_LAKE["bucket_name"]

    # Stations à converser
    stations = config.STATIONS_OF_INTEREST

    #datasets
    # Accessibility
    accessibility_dataset = config.DATASETS['accessibility']
    accessibility_url = accessibility_dataset['url']
    local_accessibility_file = accessibility_dataset['raw_path']
    refined_accessibility_path = accessibility_dataset['refined_path']

    # Elevator
    elevator_dataset = config.DATASETS['elevators']
    elevator_url = elevator_dataset['url']
    local_elevator_file = elevator_dataset['raw_path']
    refined_elevator_path = elevator_dataset['refined_path']

    # Download files
    try:
    
        accessibility_dl = download_file(accessibility_url, local_accessibility_file)
        elevator_dl = download_file(elevator_url, local_elevator_file)
    except Exception as e:
        print(f"Error dowloading data: {e}")

    # Preprocess Accessibility Data
    try:
        df_accessibility = pd.read_csv(accessibility_dl, encoding='utf-8', delimiter=';')
        accessibility_filtered = filter_stations(df_accessibility, stations, col_name='stop_name')
        
        # Upload refined data and cleanup raw file
        upload_with_cleanup(
            s3_client, 
            accessibility_filtered, 
            bucket_name, 
            local_accessibility_file,
            refined_accessibility_path,
            is_refined=True
        )
    except Exception as e:
        print(f"Error processing accessibility data: {e}")

    # Process Elevator data
    try:
        df_elevator = pd.read_csv(elevator_dl, encoding='utf-8', delimiter=';')
        elevator_filtered = filter_stations(df_elevator, stations, col_name='ZdcName')
        
        # Upload refined data and cleanup raw file
        upload_with_cleanup(
            s3_client, 
            elevator_filtered, 
            bucket_name, 
            local_elevator_file,
            refined_elevator_path,
            is_refined=True
        )
    except Exception as e:
        print(f"Error processing elevator status data: {e}")

if __name__ == "__main__":
    extract_infra_data()