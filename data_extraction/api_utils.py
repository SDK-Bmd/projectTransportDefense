"""
API utilities for resilient data extraction
"""
import requests
import time
import random
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    filename='api_requests.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('api_utils')

def get_with_retries(url, max_retries=3, backoff_factor=2, timeout=10,
                     headers=None, params=None):
    """
    Make HTTP GET requests with exponential backoff retries
    """
    for retry in range(max_retries):
        try:
            response = requests.get(
                url,
                timeout=timeout,
                headers=headers,
                params=params
            )

            # Log the request
            logger.info(f"GET {url} - Status: {response.status_code}")

            # For successful responses, return immediately
            if response.status_code < 400:
                return response

            # For server errors (5xx), retry
            elif response.status_code >= 500:
                logger.warning(f"Server error {response.status_code} for {url} - Attempt {retry+1}/{max_retries}")

            # For client errors (4xx), retry some but not all
            elif response.status_code == 429:  # Too many requests
                logger.warning(f"Rate limited (429) for {url} - Attempt {retry+1}/{max_retries}")
            elif response.status_code == 404:  # Not found
                logger.error(f"Resource not found (404) for {url}")
                return response  # Return the 404 response, no need to retry
            else:
                logger.error(f"Client error {response.status_code} for {url}")
                return response  # Return other client errors, no need to retry

        except Exception as e:
            logger.warning(f"Request error for {url}: {str(e)} - Attempt {retry+1}/{max_retries}")

        # Calculate wait time with exponential backoff and jitter
        if retry < max_retries - 1:  # Don't sleep after the last attempt
            wait_time = (backoff_factor ** retry) + random.uniform(0, 1)
            logger.info(f"Retrying in {wait_time:.2f}s")
            time.sleep(wait_time)

    logger.error(f"All {max_retries} attempts failed for {url}")
    return None

# API endpoint constants - centralize URL definitions here
API_ENDPOINTS = {
    # RATP API endpoints
    'ratp_metro_line1': 'https://api-ratp.pierre-grimaud.fr/v4/stations/metros/1',
    'ratp_rer_lineA': 'https://api-ratp.pierre-grimaud.fr/v4/stations/rers/A',
    'ratp_tram_line2': 'https://api-ratp.pierre-grimaud.fr/v4/stations/tramways/2',

    # RATP schedules (replace {type}, {line}, {station} in code)
    'ratp_schedules': 'https://api-ratp.pierre-grimaud.fr/v4/schedules/{type}/{line}/{station}/A+R',

    # RATP traffic (replace {type}, {line} in code)
    'ratp_traffic': 'https://api-ratp.pierre-grimaud.fr/v4/traffic/{type}/{line}',

    # RATP equipment
    'ratp_equipment': 'https://data.ratp.fr/api/records/1.0/search/',

    # RATP accessibility
    'ratp_accessibility': 'https://data.ratp.fr/api/records/1.0/search/',

    # Sytadin traffic (updated URL)
    'sytadin_traffic': 'https://www.sytadin.fr/gp/sytadin/data_traffic.jsp.data/traffic.jsp',

    # Overpass API
    'osm_overpass': 'http://overpass-api.de/api/interpreter'
}