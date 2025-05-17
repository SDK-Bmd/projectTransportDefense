"""
Script d'extraction et de stockage des données de fréquentation du pole de la défense²
"""

import pandas as pd
import config
from utils_extract import (
    get_s3_client, 
    download_file, 
    upload_with_cleanup
)

def preprocess_frequentation_data(df):
    """
    Prétraite les données de fréquentation/jour du pole de la défense
    Args:
        df : DataFrame de fréquentation brut
        
    Returns:
        pd.DataFrame: DataFrame avec les données prét-traitées
    """
    #uniformiser les noms des colonnes 
    df = df.rename(str.lower, axis='columns')
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%d/%m/%Y')

    # colones add pour faciliter l'analyse
    df['date'] = pd.to_datetime(df['date'], format="%d/%m/%Y")
    df['jour'] = df['date'].dt.day
    df['mois'] = df['date'].dt.month
    df['annee'] = df['date'].dt.year

    df['total'] = pd.to_numeric(df['total'], errors='coerce')
    # suppression des valeurs manquantes éventuelles
    df_preprocess = df.dropna(subset=['date', 'type_jour', 'total'])
    
    return df_preprocess

def preprocess_hourly_data(df):
    """
    Prétraitement des données de fréquentation/heures
    Args:
        df: DataFrame brut fréquentation/heures
        
    Returns:
        pd.DataFrame: DataFrame prétraité
    """
    df_preprocess = df.rename(str.lower, axis='columns')
    # conversion des colones de mois et année en num
    df_preprocess['mois'] = pd.to_numeric(df_preprocess['mois'], errors='coerce')
    df_preprocess['annee'] = pd.to_numeric(df_preprocess['annee'], errors='coerce')

    # suppression des valeurs manquantes éventuelles
    df_preprocess = df_preprocess.dropna(subset=['mois', 'annee', 'type_jour'])
    
    return df_preprocess

def calculate_monthly_volumes(freq_df, hourly_df):
    """
    Calcul des volumes mensuels par type de jour et insertion dans le df des frquentation/heures
    Args:
        freq_df: DataFrame de fréquentation/jour
        hourly_df: DataFrame de fréquentation/heure
        
    Returns:
        pd.DataFrame: DataFrame
    """
    # Groupe par année, mois et type_jour pour obtenir les volumes mensuels
    monthly_volumes = freq_df.groupby(['annee', 'mois', 'type_jour'])['total'].sum().reset_index()
    monthly_volumes = monthly_volumes.rename(columns={'total': 'volume_mensuel'})

    # fusion
    merged_df = pd.merge(
        hourly_df, 
        monthly_volumes, 
        left_on=['annee', 'mois', 'type_jour'], 
        right_on=['annee', 'mois', 'type_jour'],
        how='left'
    )

    # del doublons colonnes
    # merged_df = merged_df.drop(columns=['annee', 'mois'])
    return merged_df

def extract_frequentation_data():
    """
    Extraction et traitement des données de fréquentation
    """
    # Initialiser le client S3
    s3_client = get_s3_client()
    bucket_name = config.DATA_LAKE["bucket_name"]
    
    freq_dfs = []
    hourly_dfs = []
    
    # Traitement des données/an
    for year in [2021, 2022, 2023]:
        freq_dataset = config.DATASETS[f'frequentation{year}']
        freq_url = freq_dataset['url']
        local_freq_path = freq_dataset['raw_path']
        refined_freq_path = freq_dataset['refined_path']
        
        hourly_dataset = config.DATASETS[f'frequentationhoraire{year}']
        hourly_url = hourly_dataset['url']
        local_hourly_path = hourly_dataset['raw_path']
        
        try:
            #fréquentation/jour
            freq_dl_path = download_file(freq_url, local_freq_path)
            freq_df = pd.read_csv(freq_dl_path, sep=';', encoding='utf-8')
            freq_df_cleaned = preprocess_frequentation_data(freq_df)
            #fréquentation/heure
            hourly_dl_path = download_file(hourly_url, local_hourly_path)
            hourly_df = pd.read_csv(hourly_dl_path, sep=';', encoding='utf-8')
            hourly_df_cleaned = preprocess_hourly_data(hourly_df)

            # Save
            freq_dfs.append(freq_df_cleaned)
            hourly_dfs.append(hourly_df_cleaned)
            
            # upload des données fréquentation/jour pré-traitées
            upload_with_cleanup(
                s3_client, 
                freq_df_cleaned, 
                bucket_name, 
                local_freq_path,
                refined_freq_path,
                is_refined=True
            )
            
        except Exception as e:
            print(f"Erreur lors du traitement des données de fréquentation de {year}: {e}")
    
    # Concatenation des données
    all_freq_df = pd.concat(freq_dfs, ignore_index=True)
    all_hourly_df = pd.concat(hourly_dfs, ignore_index=True)
    
    # Calcul et insertion des volumes mensuels
    calc_hourly_df = calculate_monthly_volumes(all_freq_df, all_hourly_df)
    
    # save dans le datalake
    upload_with_cleanup(
        s3_client, 
        calc_hourly_df, 
        bucket_name, 
        "raw/traffic/temp_frequentation_horaire_concat.csv",  # Chemin temporaire données brutes
        "refined/traffic/frequentation_horaire_all.csv", #données/heures ttes années confondues
        is_refined=True
    )
    
    print("Traitement des données de fréquentation terminé avec succès.")

if __name__ == "__main__":
    extract_frequentation_data()