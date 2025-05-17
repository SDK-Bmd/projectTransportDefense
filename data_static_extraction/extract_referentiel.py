"""
Script d'extraction et de stockage des données de référence d'Île-de-France Mobilités
(Arrêts de référence, Zones d'arrêt, Zones de correspondance)
"""
import json
import os
import zipfile
import tempfile
import shutil
import requests
from datetime import datetime
from botocore.client import Config

import config
from utils_extract import get_s3_client

def download_reference_data(url, download_dir, ref_type):
    """Télécharge le fichier zip depuis l'URL fournie"""
    print(f"Téléchargement des données {ref_type} depuis {url}...")
    
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise Exception(f"Échec du téléchargement des données {ref_type}: {response.status_code}")
    
    zip_path = os.path.join(download_dir, f"REF_{ref_type}.zip")
    with open(zip_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    print(f"Téléchargement terminé: {zip_path}")
    return zip_path

def extract_reference_files(zip_path, extract_dir):
    """Extrait tous les fichiers du fichier zip"""
    print(f"Extraction des fichiers vers {extract_dir}...")
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Extraire tous les fichiers
        zip_ref.extractall(extract_dir)
        
        # Liste de tous les fichiers extraits
        all_files = zip_ref.namelist()
        
    # Obtenir la liste des fichiers (pas des répertoires) qui ont été extraits
    extracted_files = []
    for root, dirs, files in os.walk(extract_dir):
        for file in files:
            # Obtenir le chemin relatif pour maintenir la structure des répertoires
            rel_path = os.path.relpath(os.path.join(root, file), extract_dir)
            extracted_files.append(rel_path)
    
    print(f"Extraction de {len(extracted_files)} fichiers terminée")
    return extracted_files

def upload_to_datalake(s3_client, local_dir, bucket_name, extracted_files, source_url, ref_type):
    """Télécharge les fichiers extraits das le datalake et génère les métadonnées"""

    print(f"Téléchargement des fichiers REF_{ref_type} vers le bucket '{bucket_name}'...")
    
    # Générer un horodatage
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Statistiques sur les extensions de fichiers
    file_extensions = {}
    for file_path in extracted_files:
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()  # Normaliser les extensions
        if ext in file_extensions:
            file_extensions[ext] += 1
        else:
            file_extensions[ext] = 1
    
    # Télécharger les fichiers
    upload_count = 0
    for filepath in extracted_files:
        local_path = os.path.join(local_dir, filepath)
        
        # Vérifier si le fichier existe et n'est pas un répertoire
        if not os.path.isfile(local_path):
            print(f"Ignorer {filepath} - n'est pas un fichier ou n'existe pas")
            continue
            
        #Clef S3 avec le préfixe raw et referentiel
        raw_key = f"raw/referentiel/ref_{ref_type}/{timestamp}/{filepath}"
        ref_key = f"referentiel/ref_{ref_type}/{filepath}"
        
        try:
            # Télécharger dans le dossier raw avec horodatage
            with open(local_path, 'rb') as file_data:
                s3_client.put_object(Bucket=bucket_name, Key=raw_key, Body=file_data
                )
            
            # Télécharger dans le dossier referentiel (version la plus récente)
            with open(local_path, 'rb') as file_data:
                s3_client.put_object(Bucket=bucket_name, Key=ref_key, Body=file_data)
            
            upload_count += 1
            print(f"Téléchargé {filepath} vers {raw_key} et {ref_key}")
            
        except Exception as e:
            print(f"Err upload of {filepath}: {str(e)}")
    
    print(f"Téléchargement de {upload_count} fichiers vers datalake terminé")
    
    # Fichier de métadonnées
    metadata = {
        "extraction_time": datetime.now().isoformat(),
        "source_url": source_url,
        "files_count": upload_count,
        "file_types": file_extensions,
        "description": config.REF_DESCRIPTIONS[ref_type]
    }
    
    # Enregistrer le fichier de métadonnées dans les deux répertoires
    raw_metadata_key = f"raw/referentiel/ref_{ref_type}/{timestamp}/metadata.json"
    ref_metadata_key = f"referentiel/ref_{ref_type}/metadata.json"
    
    try:
        # Métadonnées dans le dossier raw
        s3_client.put_object(Bucket=bucket_name, Key=raw_metadata_key, Body=json.dumps(metadata, indent=2))
        # Métadonnées dans le dossier referentiel
        s3_client.put_object(Bucket=bucket_name, Key=ref_metadata_key, Body=json.dumps(metadata, indent=2))
        
        print(f"Métadonnées enregistrées dans {raw_metadata_key} et {ref_metadata_key}")
    except Exception as e:
        print(f"Erreur lors de l'enregistrement des métadonnées: {str(e)}")
    
    return upload_count

def extract_reference_to_datalake():
    """Fonction principale pour extraire les données de référence et les dl vers le datalake"""
    #Répertoire temporaire pour les téléchargements et l'extraction
    temp_dir = tempfile.mkdtemp()
    try:
        s3 = get_s3_client()
        bucket_name = config.DATA_LAKE["bucket_name"]
        
        # dl, extraire et upload pour chaque type de référence 
        for ref_type, url in config.REF_URLS.items():
            try:
                zip_path = download_reference_data(url, temp_dir, ref_type)
                
                extract_dir = os.path.join(temp_dir, f"extracted_{ref_type}")
                os.makedirs(extract_dir, exist_ok=True)
                extracted_files = extract_reference_files(zip_path, extract_dir)
                
                upload_count = upload_to_datalake(s3, extract_dir, bucket_name, extracted_files, url, ref_type)
                
                print(f"Extraction des données REF_{ref_type} terminée. {upload_count} fichiers téléchargés dans le datalake.")
                
            except Exception as e:
                print(f"Erreur lors du traitement de REF_{ref_type}: {str(e)}")
        
    except Exception as e:
        print(f"Erreur dans le processus d'extraction: {str(e)}")
    finally:
        # Nettoyer le répertoire temporaire
        shutil.rmtree(temp_dir)
        print("Fichiers temporaires nettoyés")

if __name__ == "__main__":
    extract_reference_to_datalake()