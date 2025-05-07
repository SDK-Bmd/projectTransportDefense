# La Défense Mobility Platform

## À propos du projet

Ce projet vise à optimiser la mobilité urbaine à La Défense grâce à l'exploitation de données ouvertes et de techniques d'intelligence artificielle. Il permet de visualiser, analyser et prédire le trafic, les conditions météorologiques et les horaires de transport pour améliorer l'expérience des utilisateurs et réduire l'impact environnemental.

## Caractéristiques principales

- **Tableau de bord interactif** : Visualisation en temps réel des conditions de trafic, météo et transport
- **Prédictions de trafic** : Modèles d'apprentissage automatique pour prévoir les embouteillages
- **Recommandations d'itinéraires** : Suggestions intelligentes basées sur les conditions actuelles
- **Analyse environnementale** : Mesure de l'impact écologique des différentes options de transport
- **Intégration multi-source** : Consolidation des données provenant de diverses API (RATP, météo, trafic)

## Architecture du projet

Le projet suit une architecture modulaire organisée en plusieurs composants :

- **data_extraction** : Scripts pour collecter des données à partir de diverses sources
- **data_processing** : Scripts pour transformer et nettoyer les données collectées
- **models** : Modèles prédictifs pour l'analyse du trafic et des recommandations
- **dash_app** : Application Streamlit pour la visualisation et l'interaction utilisateur
- **automation** : Scripts pour l'exécution automatisée des tâches d'extraction et de traitement
- **config** : Fichiers de configuration pour le projet
- **utils** : Utilitaires partagés entre les différents modules

## Prérequis

- Python 3.8+
- Docker (pour MinIO)
- Clés API pour :
  - Visual Crossing Weather API
  - TomTom Traffic API
  - RATP API

## Installation

1. Clonez ce dépôt :
   ```bash
   git clone https://github.com/votre-organisation/ladefense-mobility.git
   cd ladefense-mobility
   ```
   
2. Créer votre environnement :
```bash
    python -m venv venv
    source venv/bin/activate  
    # Sur Windows : 
    venv\Scripts\activate
```

3. Installez les dépendances :
```bash
pip install -r requirements.txt 
```
4. Configurez les variables d'environnement :
```bash
pip install -r requirements.txt 
```

5. Lancez MinIO dataLake :
```bash
docker run -d -p 9000:9000 -p 9001:9001 -v ~/data-lake-ladefense:/data minio/minio server /data --console-address ":9001"
```

6. Init DataLake : 
```bash
    python automation/init_data_lake.py
```

7. Lancez l'extraction des données :
```bash
    python automation/run_extract.py
```

8. Lancez le dashboard :
```bash
    streamlit run dash_app/app.py
```