"""
Configuration module for the La Défense mobility data lake
"""

# Data lake connection parameters
DATA_LAKE = {
    "bucket_name": "ladefense-mobility",
    "endpoint_url": "http://localhost:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin"
}

# Structure of data lake folders
FOLDER_STRUCTURE = {
    "landing": ["transport", "weather", "traffic", "stations"],
    "raw": ["transport", "weather", "traffic", "stations"],
    "refined": ["transport", "weather", "traffic", "stations", "aggregated"],
    "analytics": ["models", "predictions", "dashboards"]
}

# Coordinates for La Défense
LADEFENSE_COORDINATES = {
    "lat": 48.8917,
    "lon": 2.2385,
    "bbox": {  # bounding box for spatial queries
        "min_lat": 48.88,
        "min_lon": 2.22,
        "max_lat": 48.90,
        "max_lon": 2.25
    }
}

# Points of interest at La Défense
POINTS_OF_INTEREST = [
    {
        "name": "Grande Arche",
        "lat": 48.8924,
        "lon": 2.2359,
        "type": "landmark"
    },
    {
        "name": "Centre Commercial Les Quatre Temps",
        "lat": 48.8911,
        "lon": 2.2376,
        "type": "commercial"
    },
    {
        "name": "Gare La Défense Grande Arche",
        "lat": 48.8917,
        "lon": 2.2385,
        "type": "transport"
    }
]

# Extraction configuration
EXTRACTION_CONFIG = {
    "transport": {
        "frequency_minutes": 15,
        "max_files_to_keep": 100
    },
    "weather": {
        "frequency_minutes": 60,
        "max_files_to_keep": 48
    },
    "traffic": {
        "frequency_minutes": 30,
        "max_files_to_keep": 48
    },
    "stations": {
        "frequency_minutes": 1440,  # Once per day
        "max_files_to_keep": 30
    },
    "dataQuality": {
        "frequency_minutes": 360,  # Once per day
        "max_files_to_keep": 30
    }
}