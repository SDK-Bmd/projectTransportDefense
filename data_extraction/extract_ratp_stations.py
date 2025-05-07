"""
Station data extraction script for La Défense using RATP API
"""
import json
from datetime import datetime
import boto3
from botocore.client import Config
from configuration import config
from api_utils import get_with_retries, API_ENDPOINTS

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

def extract_ratp_station_data():
    """Extract detailed information about RATP stations at La Défense"""
    # Configuration
    s3 = get_s3_client()
    bucket_name = config.DATA_LAKE["bucket_name"]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Data to collect
    station_data = {
        "extraction_time": datetime.now().isoformat(),
        "source": "RATP Open Data",
        "stations": []
    }

    # RATP Endpoints from API_ENDPOINTS
    metro_stations_url = API_ENDPOINTS['ratp_metro_line1']
    rer_stations_url = API_ENDPOINTS['ratp_rer_lineA']

    # Equipment status (elevators/escalators)
    equipment_url = API_ENDPOINTS['ratp_equipment']
    equipment_params = {
        "dataset": "etat-du-service-des-equipements-en-gare",
        "q": "la+defense",
        "rows": 20
    }

    # Accessibility information
    accessibility_url = API_ENDPOINTS['ratp_accessibility']
    accessibility_params = {
        "dataset": "accessibilite-des-gares-et-stations-metro-et-rer-ratp",
        "q": "la+defense",
        "rows": 10
    }

    try:
        # Get Metro Line 1 stations with retry logic
        metro_response = get_with_retries(metro_stations_url, max_retries=3)

        if metro_response and metro_response.status_code == 200:
            metro_data = metro_response.json()

            # Find La Défense station
            for station in metro_data.get("result", {}).get("stations", []):
                if "defense" in station.get("name", "").lower():
                    station_info = {
                        "name": station.get("name", ""),
                        "id": station.get("id", ""),
                        "type": "Metro",
                        "line": "1",
                        "slug": station.get("slug", ""),
                        "coordinates": {},  # Will be filled from accessibility data
                        "accessibility": {},
                        "equipment": {}
                    }
                    station_data["stations"].append(station_info)

            print(f"Found {len(station_data['stations'])} Metro stations at La Défense")
        else:
            print(f"Error retrieving RATP Metro stations: {metro_response.status_code if metro_response else 'No response'}")

        # Get RER A stations with retry logic
        rer_response = get_with_retries(rer_stations_url, max_retries=3)

        if rer_response and rer_response.status_code == 200:
            rer_data = rer_response.json()

            # Find La Défense station
            for station in rer_data.get("result", {}).get("stations", []):
                if "defense" in station.get("name", "").lower():
                    station_info = {
                        "name": station.get("name", ""),
                        "id": station.get("id", ""),
                        "type": "RER",
                        "line": "A",
                        "slug": station.get("slug", ""),
                        "coordinates": {},
                        "accessibility": {},
                        "equipment": {}
                    }
                    station_data["stations"].append(station_info)

            print(f"Found {len(station_data['stations']) - 1 if station_data['stations'] else 0} RER stations at La Défense")
        else:
            print(f"Error retrieving RATP RER stations: {rer_response.status_code if rer_response else 'No response'}")

        # Get equipment status with retry logic
        equipment_response = get_with_retries(equipment_url, params=equipment_params, max_retries=3)

        if equipment_response and equipment_response.status_code == 200:
            equipment_data = equipment_response.json()

            # Process equipment data
            equipment_items = []
            for record in equipment_data.get("records", []):
                fields = record.get("fields", {})

                item = {
                    "station": fields.get("gare", ""),
                    "type": fields.get("type", ""),
                    "direction": fields.get("direction", ""),
                    "status": fields.get("etat", ""),
                    "last_updated": fields.get("last_update", ""),
                    "description": fields.get("nom", "")
                }
                equipment_items.append(item)

            # Attach equipment data to stations
            for station in station_data["stations"]:
                station_equipment = [item for item in equipment_items
                                    if station["name"].lower() in item["station"].lower()]

                # Summarize equipment
                elevators = [item for item in station_equipment if "ascenseur" in item["type"].lower()]
                escalators = [item for item in station_equipment if "escalier" in item["type"].lower()]
                gates = [item for item in station_equipment if "ligne" in item["type"].lower() and "controle" in item["type"].lower()]

                station["equipment"] = {
                    "elevators": {
                        "count": len(elevators),
                        "operational": len([e for e in elevators if e["status"].lower() == "en service"]),
                        "details": elevators
                    },
                    "escalators": {
                        "count": len(escalators),
                        "operational": len([e for e in escalators if e["status"].lower() == "en service"]),
                        "details": escalators
                    },
                    "gates": {
                        "count": len(gates),
                        "operational": len([g for g in gates if g["status"].lower() == "en service"]),
                        "details": gates
                    }
                }

            print("Added equipment data to stations")
        else:
            print(f"Error retrieving RATP equipment data: {equipment_response.status_code if equipment_response else 'No response'}")

        # Get accessibility information with retry logic
        accessibility_response = get_with_retries(accessibility_url, params=accessibility_params, max_retries=3)

        if accessibility_response and accessibility_response.status_code == 200:
            accessibility_data = accessibility_response.json()

            for record in accessibility_data.get("records", []):
                fields = record.get("fields", {})
                station_name = fields.get("nom_gare", "")

                # Find the corresponding station
                for station in station_data["stations"]:
                    if station_name.lower() in station["name"].lower() or station["name"].lower() in station_name.lower():
                        station["coordinates"] = {
                            "lat": fields.get("coord_geo", [0, 0])[0] if fields.get("coord_geo") else 0,
                            "lon": fields.get("coord_geo", [0, 0])[1] if fields.get("coord_geo") else 0
                        }

                        station["accessibility"] = {
                            "wheelchair": fields.get("accessibilite_ufr", ""),
                            "visual_guidance": fields.get("guidance_visuel", ""),
                            "elevators": fields.get("nb_ascenseur", 0),
                            "escalators": fields.get("nb_escalier", 0),
                            "wheelchair_entrance": fields.get("entree_ufr", ""),
                            "platforms_accessible": fields.get("quai_ufr", "")
                        }
                        break

            print("Added accessibility data to stations")
        else:
            print(f"Error retrieving RATP accessibility data: {accessibility_response.status_code if accessibility_response else 'No response'}")

        # Save to data lake regardless of partial data collection
        s3_key = f"landing/stations/ratp_stations_{timestamp}.json"
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(station_data)
        )

        # Also save a latest version
        s3.put_object(
            Bucket=bucket_name,
            Key="landing/stations/ratp_stations_latest.json",
            Body=json.dumps(station_data)
        )

        print(f"RATP station data extracted and saved to {s3_key}")
        return True

    except Exception as e:
        print(f"Error extracting RATP station data: {str(e)}")
        return False

if __name__ == "__main__":
    extract_ratp_station_data()