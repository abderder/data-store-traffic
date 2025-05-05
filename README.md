	
🛍️ DataMag360 — Une solution data complète pour le suivi des magasins
> Toute l'activité d’un magasin, captée, traitée et visualisée automatiquement.

DataMag360 est un projet de data engineering bout-en-bout simulant et analysant l’activité de magasins physiques. Il automatise la génération, la collecte, le stockage, le traitement et la visualisation des données de fréquentation et de transactions. Le tout est orchestré dans un pipeline moderne, cloud et scalable.

🎯 Objectifs
- Simuler des données réalistes de visiteurs et de ventes
- Exposer ces données via une API REST
- Automatiser la collecte horaire avec Airflow
- Centraliser les fichiers dans Azure Blob Storage
- Charger les données dans Azure SQL (raw)
- Traiter et transformer les données via Databricks (analytics)
- Visualiser les KPIs dans une application Streamlit interactive

🛠️ Stack Technique
| Domaine       | Outils / Technologies                                      |
|---------------|------------------------------------------------------------|
| Génération    | Python, hashlib, random                                    |
| API           | FastAPI, Render (déploiement)                             |
| Orchestration | Apache Airflow                                            |
| Stockage      | Azure Blob Storage (Parquet)                              |
| Ingestion     | Azure Data Factory (schéma RAW - horaire)                 |
| Traitement    | Azure Databricks → Azure SQL (analytics - quotidien)     |
| Visualisation | Streamlit, Plotly, Azure SQL                              |
| CI/CD         | GitHub Actions (lint + tests + déploiement auto)         |

📂 Vue d’ensemble du pipeline



🧪 Fonctionnalités Clés
- Données simulées horaires, réalistes et contrôlées
- API REST ouverte sans authentification
- DAG Airflow horaire automatisé
- Partitionnement des données par date/hour
- Pipelines ADF pour ingestion (horaire) et transformation (quotidien)
- Base SQL Azure avec 2 schémas : raw, analytics
- Visualisation des indicateurs clés avec Streamlit
- Déploiements automatisés via CI/CD

🧰 Pipeline en 7 étapes

1. Génération des données
   - Fichiers sensor.py / realistic_sensor.py
   - Données synthétiques mais cohérentes

2. API REST FastAPI
   - Endpoints : /visiteurs et /transactions
   - Données disponibles par capteur, heure et jour

3. DAG Airflow (horaire)
   - Appelle l’API FastAPI
   - Enregistre les résultats en .parquet dans Azure Blob Storage

4. Pipeline ADF – Ingestion (horaire)
   - Ingestion automatique des fichiers parquet vers Azure SQL (raw)

5. Pipeline ADF – Transformation (quotidien)
   - Orchestration de notebooks Databricks
   - Nettoyage et transformation vers schéma analytics

6. Base Azure SQL
   - 2 schémas : raw (données brutes) et analytics (nettoyées et exploitables)

7. Dashboard Streamlit
   - Filtres interactifs, graphiques dynamiques, indicateurs clés

⚙️ Intégration Continue (CI) / Déploiement Continu (CD)

CI – GitHub Actions
- Lint automatique avec Black
- Tests unitaires exécutés à chaque push / PR sur dev & main

CD – Déploiement automatique (via intégration GitHub)
- L’API FastAPI est automatiquement redéployée par **Render** à chaque push
- L’app Streamlit est automatiquement mise à jour via **Streamlit Cloud**

🔓 Accès
- API : https://datamag360.onrender.com
- App Streamlit : datamag360.streamlit.app

