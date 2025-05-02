import streamlit as st
import pandas as pd
import pyodbc
import os
from dotenv import load_dotenv

# Chargement des variables d'environnement
load_dotenv()


# Connexion à Azure SQL
def connect_to_azure_sql():
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f'SERVER={os.getenv("AZURE_SQL_SERVER")};'
        f'DATABASE={os.getenv("AZURE_SQL_DB")};'
        f'UID={os.getenv("AZURE_SQL_USER")};'
        f'PWD={os.getenv("AZURE_SQL_PASSWORD")}'
    )
    return conn


sas_token = os.getenv("BLOB_SAS_TOKEN")
base_url = os.getenv("Blob_SAS_URL")


# Chargement des fichiers CSV (préalablement placés dans le conteneur et accessibles localement ou via un chemin)
@st.cache_data
def load_csv_data():
    stores_df = pd.read_csv(f"{base_url}/magasins.csv?{sas_token}")
    sensors_df = pd.read_csv(f"{base_url}/capteurs.csv?{sas_token}")
    return sensors_df, stores_df


def get_visiteurs_data():
    conn = connect_to_azure_sql()
    query = "SELECT * FROM analytics.visiteurs"
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def get_transactions_data():
    conn = connect_to_azure_sql()
    query = "SELECT * FROM analytics.transactions"
    df = pd.read_sql(query, conn)
    conn.close()
    return df



# Interface Streamlit
def main():
    st.title("📊 Dashboard Analytics des Magasins")

    # Chargement données
    visiteurs_df = get_visiteurs_data()
    transactions_df = get_transactions_data()
    sensors_df, stores_df = load_csv_data()
    stores_df = stores_df[["store_id", "city_store"]]
    sensors_df = sensors_df[["store_id", "sensor_id","door_name"]]

    # Fusion avec infos des magasins
    visiteurs_df = visiteurs_df.merge(stores_df, on="store_id", how="left")
    transactions_df = transactions_df.merge(stores_df, on="store_id", how="left")

    # Barre latérale - sélection du magasin
    magasin = st.sidebar.selectbox(
        "Sélectionnez un magasin :", stores_df["city_store"].unique()
    )
    capteurs_disponibles = visiteurs_df[visiteurs_df["city_store"] == magasin]['sensor_id'].unique()
    capteurs_selectionnés = st.sidebar.multiselect(
        "Choisissez un ou plusieurs capteurs :", 
        options=capteurs_disponibles, 
        default=capteurs_disponibles
    )

    # Filtrage
    visiteurs_filtrés = visiteurs_df[
    (visiteurs_df["city_store"] == magasin) &
    (visiteurs_df["sensor_id"].isin(capteurs_selectionnés))
    ]
    transactions_filtrées = transactions_df[transactions_df["city_store"] == magasin]


    # Graphiques
    st.subheader("📈 Visiteurs par heure")
    st.line_chart(visiteurs_filtrés.groupby("heure")["nb_visiteurs"].sum())

    st.subheader("📊 Chiffre d'affaires par heure")
    st.bar_chart(transactions_filtrées.groupby("heure")["chiffre_affaires"].sum())


if __name__ == "__main__":
    main()
