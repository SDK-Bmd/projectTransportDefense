import base64

import requests
import pandas as pd
import json
from datetime import datetime
import os
import boto3
from dotenv import load_dotenv
from dataLake_creation import get_s3_client

# Charger les variables d'environnement (pour la clé API SNCF)
load_dotenv()


def extract_sncf_data():
    # Votre clé API SNCF (à obtenir sur https://www.digital.sncf.com/startup/api)
    sncf_api_key = 'aa4f57dd-38eb-45df-a46c-11cb793a2fce'

    auth_string = f"{sncf_api_key}:"  # Note the colon at the end (username:password format)
    encoded_key = base64.b64encode(auth_string.encode()).decode()

    if not sncf_api_key:
        print("Erreur: Clé API SNCF non trouvée. Créez un fichier .env avec SNCF_API_KEY=votre_clé")
        return

    # Configuration
    s3 = get_s3_client()
    bucket_name = 'ladefense-mobility'
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Code UIC de la gare de La Défense (à vérifier/ajuster)
    ladefense_station_id = "stop_area:SNCF:87758011"

    # URL de l'API pour les départs et arrivées
    departures_url = f"https://api.sncf.com/v1/coverage/sncf/stop_areas/{ladefense_station_id}/departures"
    arrivals_url = f"https://api.sncf.com/v1/coverage/sncf/stop_areas/{ladefense_station_id}/arrivals"

    headers = {
        "Authorization": f"Basic {encoded_key}"
    }

    try:
        # Récupération des départs
        departures_response = requests.get(departures_url, headers=headers)
        departures_data = departures_response.json()

        # Récupération des arrivées
        arrivals_response = requests.get(arrivals_url, headers=headers)
        arrivals_data = arrivals_response.json()

        # Combiner les données
        combined_data = {
            "extraction_time": datetime.now().isoformat(),
            "station_id": ladefense_station_id,
            "station_name": "La Défense",
            "departures": departures_data,
            "arrivals": arrivals_data
        }

        # Sauvegarde temporaire en local
        local_path = f"temp_sncf_ladefense_{timestamp}.json"
        with open(local_path, 'w') as f:
            json.dump(combined_data, f)

        # Upload vers le data lake
        s3_key = f"landing/transport/sncf_ladefense_{timestamp}.json"
        s3.upload_file(local_path, bucket_name, s3_key)

        # Suppression du fichier temporaire
        os.remove(local_path)

        print(f"Données SNCF extraites et sauvegardées pour La Défense")

    except Exception as e:
        print(f"Erreur lors de l'extraction des données SNCF: {str(e)}")


if __name__ == "__main__":
    extract_sncf_data()