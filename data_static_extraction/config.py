DATA_LAKE = {
    "bucket_name": "ladefense-mobility",
    "endpoint_url": "http://localhost:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin"
}

GTFS_URL = "https://data.iledefrance-mobilites.fr/api/datasets/1.0/offre-horaires-tc-gtfs-idfm/images/a925e164271e4bca93433756d6a340d1"

RAIL_VALIDATION_URLS = {
    # "2015": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/histo-validations-reseau-ferre/files/85a9c79a1fc4ee27ac8f4199be336a4d",
    "2016": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/histo-validations-reseau-ferre/files/03dbbf888b1bb2386a2e5452861c5028",
    "2017": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/histo-validations-reseau-ferre/files/2ac7b336c2e7be2586f20bd8f298ab29",
    "2018": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/histo-validations-reseau-ferre/files/e1ef1b42c0e0ff7ea62ac76937ff0a60",
    "2019": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/histo-validations-reseau-ferre/files/6d7e7b859e6acac7bebad18bdb37bfc3",
    "2020": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/histo-validations-reseau-ferre/files/e6bcf4c994951fc086e31db6819a3448",
    "2021": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/histo-validations-reseau-ferre/files/e35b9ec0a183a8f2c7a8537dd43b124c",
    "2022": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/histo-validations-reseau-ferre/files/2a6afefb59a0ccd657ba46962c96d90b",
    "2023": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/histo-validations-reseau-ferre/files/c46588b16704dd56dd252f97b4de05f1"
}

DATASETS = {
    "accessibility": {
        "url": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/accessibilite-en-gare/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B",
        "raw_path": "raw/transport/accessibilite_gares.csv",
        "refined_path": "refined/transport/accessibilite_gares_filtered.csv"
    },
    "elevators": {
        "url": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/etat-des-ascenseurs/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B",
        "raw_path": "raw/transport/etat_ascenseurs.csv",
        "refined_path": "refined/transport/etat_ascenseurs_filtered.csv"
    },
    "frequentation2023": {
        "url": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/frequentation-du-pole-de-la-defense-experimentation/alternative_exports/total_jour_ladefense_ratp_20232_csv/",
        "raw_path": "raw/frequentation_la_defense.csv",
        "refined_path": "refined/traffic/frequentation_la_defense2023.csv"
    },
    "frequentationhoraire2023": {
        "url": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/frequentation-du-pole-de-la-defense-experimentation/alternative_exports/profil_horaire_ladefense_ratp_20232_csv/",
        "raw_path": "raw/traffic/frequentation_horaire_la_defense.csv",
        "refined_path": "refined/traffic/frequentation_horaire_la_defense2023.csv"
    },
    "frequentation2022": {
        "url": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/frequentation-du-pole-de-la-defense-experimentation/alternative_exports/total_jour_ladefense_ratp_2022_csv/",
        "raw_path": "raw/traffic/frequentation_la_defense.csv",
        "refined_path": "refined/traffic/frequentation_la_defense2022.csv"
    },
    "frequentationhoraire2022": {
        "url": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/frequentation-du-pole-de-la-defense-experimentation/alternative_exports/profil_horaire_ladefense_ratp_2022_csv/",
        "raw_path": "raw/traffic/frequentation_horaire_la_defense.csv",
        "refined_path": "refined/traffic/frequentation_horaire_la_defense2022.csv"
    },
    "frequentation2021": {
        "url": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/frequentation-du-pole-de-la-defense-experimentation/alternative_exports/total_jour_ladefense_ratp_20210_csv/",
        "raw_path": "raw/traffic/frequentation_la_defense.csv",
        "refined_path": "refined/traffic/frequentation_la_defense2021.csv"
    },
    "frequentationhoraire2021": {
        "url": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/frequentation-du-pole-de-la-defense-experimentation/alternative_exports/profil_horaire_ladefense_ratp_20211_csv/",
        "raw_path": "raw/traffic/frequentation_horaire_la_defense.csv",
        "refined_path": "refined/traffic/frequentation_horaire_la_defense2021.csv"
    }
}

# URLs de téléchargement des données de validation par année
RAIL_VALIDATION_URLS = {
    # "2015": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/histo-validations-reseau-ferre/files/85a9c79a1fc4ee27ac8f4199be336a4d",
    "2016": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/histo-validations-reseau-ferre/files/03dbbf888b1bb2386a2e5452861c5028",
    "2017": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/histo-validations-reseau-ferre/files/2ac7b336c2e7be2586f20bd8f298ab29",
    "2018": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/histo-validations-reseau-ferre/files/e1ef1b42c0e0ff7ea62ac76937ff0a60",
    "2019": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/histo-validations-reseau-ferre/files/6d7e7b859e6acac7bebad18bdb37bfc3",
    "2020": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/histo-validations-reseau-ferre/files/e6bcf4c994951fc086e31db6819a3448",
    "2021": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/histo-validations-reseau-ferre/files/e35b9ec0a183a8f2c7a8537dd43b124c",
    "2022": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/histo-validations-reseau-ferre/files/2a6afefb59a0ccd657ba46962c96d90b",
    "2023": "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/histo-validations-reseau-ferre/files/c46588b16704dd56dd252f97b4de05f1"
}

# URLs des données de référence
REF_URLS = {
    "ZdA": "https://eu.ftp.opendatasoft.com/stif/Reflex/REF_ZdA.zip",
    "ArR": "https://eu.ftp.opendatasoft.com/stif/Reflex/REF_ArR.zip", 
    "ZdC": "https://eu.ftp.opendatasoft.com/stif/Reflex/REF_ZdC.zip"
}

# Descriptions des données pour les métadonnées
REF_DESCRIPTIONS = {
    "ZdA": "Zones d'arrêt (ZdA) auxquelles sont reliés les arrêts de référence",
    "ArR": "Arrêts de référence (ArR)",
    "ZdC": "Zones de correspondance (ZdC) auxquelles sont reliés les arrêts"
}



# STATIONS_OF_INTEREST = [
#     'Bécon les Bruyères', 'Charlebourg', 'Courbevoie', 
#     'Esplanade de la Défense', 'Faubourg de l\'Arche', 
#     'Gare de Becon les Bruyères', 'Gare de Courbevoie', 
#     'Gare de la Garenne Colombes', 'Gare de Puteaux',
#     'La Défense', 'La Défense (Grande Arche)', 'Les Fauvelles', 
#     'Nanterre', 'Nanterre - Préfecture', 'Nanterre - Université', 
#     'Nanterre - Ville', 'Nanterre Préfecture', 'Nanterre Université', 
#     'Nanterre-La-Folie', 'Nanterre-Ville', 'Puteaux'
# ]
STATIONS_OF_INTEREST = [
    'Nanterre',
    'Defense', 
    'Défense', 
    'Puteaux', 
    'Courbevoie', 
    'Becon', 
    'Bécon les bruyères', 
    'Charlebourg', 
    'Les fauvelles', 
    'La Garenne', 
    'Faubourg de l\'arche'
]