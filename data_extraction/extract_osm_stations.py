"""
Enhanced OpenStreetMap station data extraction for La Défense
"""
import requests
import json
from datetime import datetime
import boto3
from botocore.client import Config
from configuration import config


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


def extract_osm_station_data():
    """Extract detailed station infrastructure data from OpenStreetMap"""
    # Configuration
    s3 = get_s3_client()
    bucket_name = config.DATA_LAKE["bucket_name"]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Coordinates for La Défense
    bbox = config.LADEFENSE_COORDINATES["bbox"]

    # Data to collect
    station_data = {
        "extraction_time": datetime.now().isoformat(),
        "source": "OpenStreetMap Enhanced",
        "stations": [],
        "entrances": [],
        "platforms": [],
        "amenities": []
    }

    # Advanced Overpass QL query to get detailed station information
    overpass_url = "http://overpass-api.de/api/interpreter"

    # This query gets:
    # 1. All public transport stations (metro, train, bus, tram)
    # 2. Station entrances and exits
    # 3. Platforms
    # 4. Amenities within stations (shops, info points, etc.)
    # 5. Accessibility information
    overpass_query = f"""
    [out:json];
    (
      // Get all stations
      node["public_transport"="station"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});
      way["public_transport"="station"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});
      relation["public_transport"="station"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});

      // Get railway stations
      node["railway"="station"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});
      way["railway"="station"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});
      relation["railway"="station"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});

      // Get subway stations (metro)
      node["railway"="subway_entrance"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});
      node["station"="subway"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});

      // Get tram stops
      node["railway"="tram_stop"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});

      // Get bus stations/stops
      node["highway"="bus_stop"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});
      node["public_transport"="stop_position"]["bus"="yes"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});
    );
    out body;

    // Get station entrances
    (
      node["railway"="subway_entrance"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});
      node["railway"="station_entrance"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});
      node["entrance"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});
    );
    out body;

    // Get platforms
    (
      node["railway"="platform"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});
      way["railway"="platform"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});
      node["public_transport"="platform"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});
      way["public_transport"="platform"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});
    );
    out body;

    // Get amenities within or near stations
    (
      node["amenity"]({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']});
    );
    out body;
    """

    try:
        # Execute the Overpass query
        response = requests.post(overpass_url, data={"data": overpass_query})

        if response.status_code == 200:
            data = response.json()

            # Process stations
            for element in data.get("elements", []):
                tags = element.get("tags", {})

                # Determine element type
                if "railway" in tags and tags["railway"] in ["station", "subway_entrance", "tram_stop"]:
                    element_type = "station"
                elif "public_transport" in tags and tags["public_transport"] == "station":
                    element_type = "station"
                elif "highway" in tags and tags["highway"] == "bus_stop":
                    element_type = "station"
                elif "railway" in tags and tags["railway"] in ["subway_entrance", "station_entrance"]:
                    element_type = "entrance"
                elif "entrance" in tags:
                    element_type = "entrance"
                elif "railway" in tags and tags["railway"] == "platform":
                    element_type = "platform"
                elif "public_transport" in tags and tags["public_transport"] == "platform":
                    element_type = "platform"
                elif "amenity" in tags:
                    element_type = "amenity"
                else:
                    element_type = "other"

                # Extract common properties
                properties = {
                    "id": element.get("id"),
                    "type": element.get("type"),
                    "tags": tags
                }

                # Add coordinates if available
                if "lat" in element and "lon" in element:
                    properties["coordinates"] = {
                        "lat": element.get("lat"),
                        "lon": element.get("lon")
                    }

                # Extract name if available
                if "name" in tags:
                    properties["name"] = tags["name"]
                elif "ref" in tags:
                    properties["name"] = tags["ref"]

                # Extract network and operator if available
                if "network" in tags:
                    properties["network"] = tags["network"]
                if "operator" in tags:
                    properties["operator"] = tags["operator"]

                # Extract lines/routes if available
                if "route_ref" in tags:
                    properties["lines"] = tags["route_ref"].split(";")
                elif "ref" in tags:
                    properties["lines"] = [tags["ref"]]

                # Extract accessibility information
                accessibility = {}
                if "wheelchair" in tags:
                    accessibility["wheelchair"] = tags["wheelchair"]
                if "tactile_paving" in tags:
                    accessibility["tactile_paving"] = tags["tactile_paving"]
                if "blind" in tags:
                    accessibility["blind"] = tags["blind"]
                if "elevator" in tags:
                    accessibility["elevator"] = tags["elevator"]
                if "handrail" in tags:
                    accessibility["handrail"] = tags["handrail"]

                if accessibility:
                    properties["accessibility"] = accessibility

                # Add to appropriate category
                if element_type == "station":
                    station_data["stations"].append(properties)
                elif element_type == "entrance":
                    station_data["entrances"].append(properties)
                elif element_type == "platform":
                    station_data["platforms"].append(properties)
                elif element_type == "amenity":
                    station_data["amenities"].append(properties)

            print(f"Extracted {len(station_data['stations'])} stations")
            print(f"Extracted {len(station_data['entrances'])} entrances")
            print(f"Extracted {len(station_data['platforms'])} platforms")
            print(f"Extracted {len(station_data['amenities'])} amenities")

            # Save to data lake
            s3_key = f"landing/stations/osm_enhanced_{timestamp}.json"
            s3.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=json.dumps(station_data)
            )

            # Also save a latest version
            s3.put_object(
                Bucket=bucket_name,
                Key="landing/stations/osm_enhanced_latest.json",
                Body=json.dumps(station_data)
            )

            print(f"Enhanced OSM station data extracted and saved to {s3_key}")
            return True
        else:
            print(f"Error querying Overpass API: {response.status_code} - {response.text}")
            return False

    except Exception as e:
        print(f"Error extracting OSM station data: {str(e)}")
        return False


if __name__ == "__main__":
    extract_osm_station_data()