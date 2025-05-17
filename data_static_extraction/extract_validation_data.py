"""
Script d'extraction des données historiques de validations du réseau ferré d'Île-de-France Mobilités
Télécharge les fichiers zip par année et les stocke dans le datalake
"""
import json
import os
import zipfile
import tempfile
import shutil
import requests
from datetime import datetime

import config

from utils_extract import (
    get_s3_client
)

def download_yearly_data(url, year, download_dir):
    """Télécharge les données d'une année spécifique"""
    print(f"Téléchargement des données {year} depuis {url}...")
    
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise Exception(f"Échec du téléchargement des données {year}: {response.status_code}")
    
    zip_path = os.path.join(download_dir, f"rail_validations_{year}.zip")
    with open(zip_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    print(f"Téléchargement terminé: {zip_path}")
    return zip_path

def extract_zip_files(zip_path, extract_dir):
    """Extrait tous les fichiers du zip"""
    print(f"Extraction des fichiers vers {extract_dir}...")
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Extraire tous les fichiers
        zip_ref.extractall(extract_dir)
        
        # Lister tous les fichiers extraits
        all_files = zip_ref.namelist()
        
    # Obtenir la liste des fichiers (pas les répertoires) qui ont été extraits
    extracted_files = []
    for root, dirs, files in os.walk(extract_dir):
        for file in files:
            # Obtenir le chemin relatif pour maintenir la structure des répertoires
            rel_path = os.path.relpath(os.path.join(root, file), extract_dir)
            extracted_files.append(rel_path)
    
    print(f"Extraction de {len(extracted_files)} fichiers terminée")
    return extracted_files

def upload_to_datalake(s3_client, local_dir, bucket_name, extracted_files, year, source_url):
    """Dl les fichiers extraits vers le datalake et génère les métadonnées"""
    print(f"Téléversement des fichiers {year} vers le bucket '{bucket_name}'...")
    
    # Générer l'horodatage pour le dossier
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Obtenir les statistiques des extensions de fichiers
    file_extensions = {}
    for file_path in extracted_files:
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()  # Normaliser les extensions
        if ext in file_extensions:
            file_extensions[ext] += 1
        else:
            file_extensions[ext] = 1
    
    # Téléverser chaque fichier
    upload_count = 0
    for filepath in extracted_files:
        local_path = os.path.join(local_dir, filepath)
        
        # Vérifier si le fichier existe et n'est pas un répertoire
        if not os.path.isfile(local_path):
            print(f"Ignorer {filepath} - pas un fichier ou n'existe pas")
            continue
            
        # Définir la clé S3 avec le préfixe raw/year
        s3_key = f"raw/rail_validations/{year}/{filepath}"
        
        try:
            with open(local_path, 'rb') as file_data:
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=file_data
                )
            
            upload_count += 1
            print(f"Téléversé {filepath} vers {s3_key}")
            
        except Exception as e:
            print(f"upload error of {filepath}: {str(e)}")
    
    print(f"{upload_count} files uploaded to datalake")
    
    # Créer le fichier de métadonnées
    metadata = {
        "extraction_time": datetime.now().isoformat(),
        "source_url": source_url,
        "year": year,
        "files_count": upload_count,
        "file_types": file_extensions
    }
    
    # Enregistrer le fichier de métadonnées dans le même répertoire que les fichiers de données
    metadata_key = f"raw/rail_validations/{year}/metadata.json"
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2)
        )
        print(f"Métadonnées enregistrées dans {metadata_key}")
    except Exception as e:
        print(f"Erreur lors de l'enregistrement des métadonnées: {str(e)}")
    
    return upload_count

def process_year(s3_client, bucket_name, year, url):
    """Traite les données d'une année spécifique"""
    # Créer un répertoire temporaire pour les téléchargements et l'extraction
    temp_dir = tempfile.mkdtemp()
    try:
        print(f"\n=== Traitement des données de {year} ===")
        
        # Télécharger le fichier zip
        zip_path = download_yearly_data(url, year, temp_dir)
        
        # Extraire les fichiers
        extract_dir = os.path.join(temp_dir, "extracted")
        os.makedirs(extract_dir, exist_ok=True)
        extracted_files = extract_zip_files(zip_path, extract_dir)
        
        # Téléverser vers datalake avec génération de métadonnées
        upload_count = upload_to_datalake(s3_client, extract_dir, bucket_name, extracted_files, year, url)
        
        print(f"Traitement des données {year} terminé. {upload_count} fichiers téléversés.")
        return upload_count
        
    except Exception as e:
        print(f"Erreur lors du traitement des données {year}: {str(e)}")
        return 0
    finally:
        # Nettoyer le répertoire temporaire
        shutil.rmtree(temp_dir)
        print(f"Fichiers temporaires de {year} nettoyés")

def extract_validations_to_minio(years=None):
    """
    Extraire les données de validations et les dl dans le datalake
    Args:
        years: Liste des années à traiter. Si None, traite toutes les années disponibles.
    """
    try:
        # Initialiser le client MinIO
        s3 = get_s3_client()
        bucket_name = config.DATA_LAKE["bucket_name"]
        
        years_to_process = years if years else config.RAIL_VALIDATION_URLS.keys()
        
        print(f"Extraction des données de validations pour : {', '.join(years_to_process)}")

        total_files = 0
        for year in years_to_process:
            if year in config.RAIL_VALIDATION_URLS:
                files_count = process_year(s3, bucket_name, year, config.RAIL_VALIDATION_URLS[year])
                total_files += files_count
            else:
                print(f"Aucune URL disponible pour l'année {year}")
        
        print(f"\nExtraction des données terminée. {total_files} fichiers insérés dans le datalake.")
        
    except Exception as e:
        print(f"Erreur dans le processus d'extraction: {str(e)}")

if __name__ == "__main__":
    # extract_validations_to_minio(["2019", "2020"])
    extract_validations_to_minio()