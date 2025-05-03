import streamlit as st
import plotly.express as px
import pandas as pd
from datetime import datetime
import pyodbc
import os
from dotenv import load_dotenv

# Chargement des variables d'environnement
load_dotenv()


# Connexion à Azure SQL
def connect_to_azure_sql():
    try:
        server = os.getenv("AZURE_SQL_SERVER")
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={os.getenv('AZURE_SQL_DB')};"
            f"UID={os.getenv('AZURE_SQL_USER')};"
            f"PWD={os.getenv('AZURE_SQL_PASSWORD')}"
        )
        return conn
    except Exception as e:
        st.error(f"Échec de connexion : {e}")
        return None


sas_token = os.getenv("BLOB_SAS_TOKEN")
base_url = os.getenv("Blob_SAS_URL")


# Chargement des fichiers CSV (préalablement placés dans le conteneur et accessibles localement ou via un chemin)
@st.cache_data
def load_csv_data():
    stores_df = pd.read_csv(f"{base_url}/magasins.csv?{sas_token}")
    sensors_df = pd.read_csv(f"{base_url}/capteurs.csv?{sas_token}")
    return sensors_df, stores_df


def get_visiteurs_data(conn):
    df = pd.read_sql(
        "SELECT * FROM analytics.visiteurs where date like '2024-05-%'", conn
    )
    return df


def get_transactions_data(conn):
    df = pd.read_sql(
        "SELECT * FROM analytics.transactions where date like '2024-05-%'", conn
    )
    return df


@st.cache_data
def load_cached_data():
    conn = connect_to_azure_sql()
    if conn is None:
        st.stop()
    visiteurs_df = get_visiteurs_data(conn)
    transactions_df = get_transactions_data(conn)
    conn.close()
    return visiteurs_df, transactions_df


# Interface Streamlit
def main():
    st.title("📊 Dashboard Analytics des Magasins")

    # Chargement données
    visiteurs_df, transactions_df = load_cached_data()
    sensors_df, stores_df = load_csv_data()
    stores_df = stores_df[["store_id", "city_store"]]
    sensors_df = sensors_df[["store_id", "sensor_id", "door_name"]]

    # Conversion des dates
    visiteurs_df["date"] = pd.to_datetime(visiteurs_df["date"])
    transactions_df["date"] = pd.to_datetime(transactions_df["date"])

    # Fusion avec infos des magasins
    visiteurs_df = visiteurs_df.merge(stores_df, on="store_id", how="left")
    transactions_df = transactions_df.merge(stores_df, on="store_id", how="left")

    # Barre latérale - sélection du magasin
    magasin = st.sidebar.selectbox(
        "🏬 Sélectionnez un magasin :", stores_df["city_store"].unique()
    )
    capteurs_disponibles = visiteurs_df[visiteurs_df["city_store"] == magasin][
        "sensor_id"
    ].unique()
    capteurs_selectionnés = st.sidebar.multiselect(
        "🎯 Choisissez un ou plusieurs capteurs :",
        options=capteurs_disponibles,
        default=capteurs_disponibles,
    )

    # Sélection plage de dates
    date_min = visiteurs_df["date"].min()
    date_max = visiteurs_df["date"].max()
    date_range = st.sidebar.date_input(
        "📅 Filtrer par date :",
        value=(date_min, date_max),
        min_value=date_min,
        max_value=date_max,
    )

    # Filtrage
    visiteurs_filtrés = visiteurs_df[
        (visiteurs_df["city_store"] == magasin)
        & (visiteurs_df["sensor_id"].isin(capteurs_selectionnés))
        & (visiteurs_df["date"] >= pd.to_datetime(date_range[0]))
        & (visiteurs_df["date"] <= pd.to_datetime(date_range[1]))
    ]

    transactions_filtrées = transactions_df[
        (transactions_df["city_store"] == magasin)
        & (transactions_df["date"] >= pd.to_datetime(date_range[0]))
        & (transactions_df["date"] <= pd.to_datetime(date_range[1]))
    ]

    # Création datetime pour Plotly
    visiteurs_filtrés["datetime"] = pd.to_datetime(
        visiteurs_filtrés["date"].astype(str)
        + " "
        + visiteurs_filtrés["heure"].astype(str)
        + ":00"
    )
    visiteurs_filtrés = visiteurs_filtrés.sort_values("datetime")

    # 📈 Plotly - Évolution des visiteurs par capteur
    st.subheader("📈 Évolution des visiteurs par capteur")
    fig = px.line(
        visiteurs_filtrés,
        x="datetime",
        y="nb_visiteurs",
        color="sensor_id",
        markers=True,
        title="Visiteurs par capteur (date + heure)",
        labels={"datetime": "Date & Heure", "nb_visiteurs": "Nombre de visiteurs"},
    )
    st.plotly_chart(fig, use_container_width=True)

    # 📊 Plotly - Visiteurs agrégés par heure
    st.subheader("🕒 Visiteurs par heure (total)")
    visiteurs_par_heure = (
        visiteurs_filtrés.groupby("heure")["nb_visiteurs"].sum().reset_index()
    )
    fig2 = px.bar(
        visiteurs_par_heure,
        x="heure",
        y="nb_visiteurs",
        labels={"heure": "Heure", "nb_visiteurs": "Nombre de visiteurs"},
        title="Visiteurs agrégés par heure",
    )
    st.plotly_chart(fig2, use_container_width=True)

    # 📈 Plotly - Chiffre d'affaires par heure
    st.subheader("💰 Chiffre d'affaires par heure")
    ca_par_heure = (
        transactions_filtrées.groupby("heure")["chiffre_affaires"].sum().reset_index()
    )
    fig3 = px.bar(
        ca_par_heure,
        x="heure",
        y="chiffre_affaires",
        labels={"heure": "Heure", "chiffre_affaires": "Chiffre d'affaires (€)"},
        title="Chiffre d'affaires par heure",
    )
    st.plotly_chart(fig3, use_container_width=True)


if __name__ == "__main__":
    main()
