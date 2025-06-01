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
        "frequency_minutes": 360,  # Six hours
        "max_files_to_keep": 30
    }
}

# API endpoints - moved from api_utils.py
API_ENDPOINTS = {
    # RATP API endpoints
    'ratp_metro_line1': 'https://api-ratp.pierre-grimaud.fr/v4/stations/metros/1',
    'ratp_rer_lineA': 'https://api-ratp.pierre-grimaud.fr/v4/stations/rers/A',
    'ratp_tram_line2': 'https://api-ratp.pierre-grimaud.fr/v4/stations/tramways/2',
    'ratp_rer_lineE': 'https://api-ratp.pierre-grimaud.fr/v4/stations/rers/E',
    'ratp_transilien_lineL': 'https://api-ratp.pierre-grimaud.fr/v4/stations/transilien/L',

# Bus lines (major ones serving La Défense)
    'ratp_bus_line73': 'https://api-ratp.pierre-grimaud.fr/v4/stations/buses/73',
    'ratp_bus_line144': 'https://api-ratp.pierre-grimaud.fr/v4/stations/buses/144',
    'ratp_bus_line158': 'https://api-ratp.pierre-grimaud.fr/v4/stations/buses/158',
    'ratp_bus_line163': 'https://api-ratp.pierre-grimaud.fr/v4/stations/buses/163',
    'ratp_bus_line174': 'https://api-ratp.pierre-grimaud.fr/v4/stations/buses/174',
    'ratp_bus_line178': 'https://api-ratp.pierre-grimaud.fr/v4/stations/buses/178',
    'ratp_bus_line258': 'https://api-ratp.pierre-grimaud.fr/v4/stations/buses/258',
    'ratp_bus_line262': 'https://api-ratp.pierre-grimaud.fr/v4/stations/buses/262',
    'ratp_bus_line272': 'https://api-ratp.pierre-grimaud.fr/v4/stations/buses/272',
    'ratp_bus_line275': 'https://api-ratp.pierre-grimaud.fr/v4/stations/buses/275',


    # RATP schedules (replace {type}, {line}, {station} in code)
    'ratp_schedules': 'https://api-ratp.pierre-grimaud.fr/v4/schedules/{type}/{line}/{station}/A+R',

    # RATP traffic (replace {type}, {line} in code)
    'ratp_traffic': 'https://api-ratp.pierre-grimaud.fr/v4/traffic/{type}/{line}',

    # RATP equipment
    'ratp_equipment': 'https://data.ratp.fr/api/records/1.0/search/',

    # RATP accessibility
    'ratp_accessibility': 'https://data.ratp.fr/api/records/1.0/search/',

    # Sytadin traffic
    'sytadin_traffic': 'https://www.sytadin.fr/gp/sytadin/data_traffic.jsp.data/traffic.jsp',

    # Overpass API
    'osm_overpass': 'http://overpass-api.de/api/interpreter',

    # IDFM API endpoints
    'idfm_base': 'https://prim.iledefrance-mobilites.fr/marketplace',
    'idfm_stop_points': 'https://prim.iledefrance-mobilites.fr/marketplace/stop-points',
    'idfm_stop_monitoring': 'https://prim.iledefrance-mobilites.fr/marketplace/stop-monitoring',
    'idfm_general_message': 'https://prim.iledefrance-mobilites.fr/marketplace/general-message',
}

# Add bus stops coordinates for La Défense area
BUS_STOPS_LADEFENSE = {
    "Grande Arche": {"lat": 48.8924, "lon": 2.2359},
    "CNIT": {"lat": 48.8916, "lon": 2.2375},
    "Esplanade de La Défense": {"lat": 48.8897, "lon": 2.2434},
    "Quatre Temps": {"lat": 48.8911, "lon": 2.2376},
    "Pont de Neuilly": {"lat": 48.8847, "lon": 2.2594},
    "Les Bergeries": {"lat": 48.8928, "lon": 2.2289},
}

# Update transport types
TRANSPORT_TYPES = {
    "metro": ["1"],
    "rers": ["A", "E"],
    "transilien": ["L"],
    "buses": ["73", "144", "158", "163", "174", "178", "258", "262", "272", "275"]
}