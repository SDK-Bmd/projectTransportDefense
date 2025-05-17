"""
This script orchestrates all data extraction and processing tasks
"""
import os
import time
from datetime import datetime

def run_extraction():
    """Run station data extraction and processing"""
    os.system('python data_static_extraction\extract_frequentation_la_defense.py')
    os.system('python data_static_extraction\extract_GTFS.py')
    os.system('python data_static_extraction\extract_validation_data.py')
    os.system('python data_static_extraction\extract_infrastructure.py')
    os.system('python data_static_extraction\extract_referentiel.py')


def run_all_extractions():
    """Run all data extraction processes"""
    # print(f"\n--- Starting complete extraction at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---\n")
    start_time = time.time()
    run_extraction()
    end_time = time.time()
    print(f"\n--- Completed extraction at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---\n")
    print(f'Fxtraction took : {(end_time - start_time)/60} minutes \n')

if __name__ == "__main__":
    run_all_extractions()