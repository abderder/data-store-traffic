import streamlit as st
import plotly.express as px
import pandas as pd
from datetime import datetime
import pyodbc
import os
from dotenv import load_dotenv
import duckdb

# Connexion à DuckDB locale
@st.cache_resource
def connect_to_duckdb():
    try:
        conn = duckdb.connect("duckdb/local_db.duckdb")
        return conn
    except Exception as e:
        st.error(f"Échec de connexion à DuckDB : {e}")
        return None


# Chargement des fichiers CSV en local
@st.cache_data
def load_csv_data():
    stores_df = pd.read_csv("data/magasins.csv")
    sensors_df = pd.read_csv("data/capteurs.csv")
    return sensors_df, stores_df


def get_visiteurs_data(conn):
    df = conn.execute("SELECT * FROM visiteurs").fetchdf()
    return df

def get_transactions_data(conn):
    df = conn.execute("SELECT * FROM transactions").fetchdf()
    return df


@st.cache_data
def load_cached_data():
    conn = connect_to_duckdb()
    if conn is None:
        st.stop()
    visiteurs_df = get_visiteurs_data(conn)
    transactions_df = get_transactions_data(conn)
    return visiteurs_df, transactions_df

def main():
    st.title("📊 Dashboard Analytics des Magasins")

    # Chargement des données
    visiteurs_df, transactions_df = load_cached_data()
    sensors_df, stores_df = load_csv_data()

    stores_df = stores_df[["store_id", "city_store"]]
    sensors_df = sensors_df[["store_id", "sensor_id"]]

    visiteurs_df["date"] = pd.to_datetime(visiteurs_df["date"])
    transactions_df["date"] = pd.to_datetime(transactions_df["date"])

    visiteurs_df = visiteurs_df.merge(stores_df, on="store_id", how="left")
    transactions_df = transactions_df.merge(stores_df, on="store_id", how="left")

    visiteurs_df["heure_str"] = visiteurs_df["heure"].apply(
        lambda h: f"{int(h):02d}:00"
    )
    transactions_df["heure_str"] = transactions_df["heure"].apply(
        lambda h: f"{int(h):02d}:00"
    )

    # 🎛️ Filtres
    magasin = st.sidebar.selectbox("🏬 Magasin :", stores_df["city_store"].unique())
    capteurs = visiteurs_df[visiteurs_df["city_store"] == magasin]["sensor_id"].unique()
    capteurs_sel = st.sidebar.multiselect("🎯 Capteurs :", capteurs, default=capteurs)

    date_min = visiteurs_df["date"].min().date()
    date_max = visiteurs_df["date"].max().date()
    date_range = st.sidebar.date_input(
        "📅 Dates :", (date_min, date_max), min_value=date_min, max_value=date_max
    )

    heure_range = st.sidebar.slider("⏰ Heures :", 9, 19, (9, 19))
    # Sécurité : s'assurer que 2 dates sont choisies
    if isinstance(date_range, tuple) and len(date_range) == 2:
        date_debut = date_range[0]
        date_fin = date_range[1]
    else:
        st.warning("📅 Veuillez sélectionner une plage de dates (début et fin).")
        st.stop()

    # VISITEURS
    visiteurs_filtrés = visiteurs_df[
        (visiteurs_df["city_store"] == magasin)
        & (visiteurs_df["sensor_id"].isin(capteurs_sel))
        & (visiteurs_df["date"].dt.date >= date_range[0])
        & (visiteurs_df["date"].dt.date <= date_range[1])
        & (visiteurs_df["heure"].between(heure_range[0], heure_range[1]))
    ]
    # TRANSACTIONS
    transactions_filtrées = transactions_df[
        (transactions_df["city_store"] == magasin)
        & (transactions_df["date"].dt.date >= date_range[0])
        & (transactions_df["date"].dt.date <= date_range[1])
        & (transactions_df["heure"].between(heure_range[0], heure_range[1]))
    ]
    if visiteurs_filtrés.empty or transactions_filtrées.empty:
        st.warning(
            "Aucune donnée pour cette période. Essayez une autre plage de dates."
        )
        st.stop()

    st.subheader("🎟️ Données des visiteurs")
    col1, col2 = st.columns(2)
    col1.metric("Total visiteurs", int(visiteurs_filtrés["nb_visiteurs"].sum()))
    col2.metric("Capteurs actifs", visiteurs_filtrés["sensor_id"].nunique())

    # Agrégation + datetime propre pour le graphe
    visiteurs_plot = visiteurs_filtrés.groupby(
        ["sensor_id", "date", "heure"], as_index=False
    )["nb_visiteurs"].sum()
    visiteurs_plot["heure_str"] = visiteurs_plot["heure"].apply(
        lambda h: f"{int(h):02d}:00"
    )
    visiteurs_plot["datetime"] = pd.to_datetime(
        visiteurs_plot["date"].astype(str) + " " + visiteurs_plot["heure_str"]
    )
    visiteurs_plot = visiteurs_plot.sort_values("datetime")
    # Limiter à 3 derniers jours pour ce graphe uniquement
    nb_jours_max = 3
    dates_disponibles = sorted(visiteurs_plot["date"].unique())
    dates_limite = dates_disponibles[:nb_jours_max]

    visiteurs_plot_limité = visiteurs_plot[visiteurs_plot["date"].isin(dates_limite)]
    # Formatage des dates pour le titre
    date_debut = pd.to_datetime(dates_limite[0]).strftime("%d %b %Y")
    date_fin = pd.to_datetime(dates_limite[-1]).strftime("%d %b %Y")
    titre_graphe_v1 = f"Visiteurs par capteur (du {date_debut} au {date_fin})"

    fig_v1 = px.line(
        visiteurs_plot_limité,
        x="datetime",
        y="nb_visiteurs",
        color="sensor_id",
        title=titre_graphe_v1,
        markers=True,
    )
    st.plotly_chart(fig_v1, use_container_width=True)

    st.subheader("💸 Données des Transactions")
    col3, col4 = st.columns(2)
    col3.metric("Transactions", int(transactions_filtrées["nb_transactions"].sum()))
    col4.metric("CA (€)", f"{transactions_filtrées['chiffre_affaires'].sum():,.0f}")

    transactions_filtrées["datetime"] = pd.to_datetime(
        transactions_filtrées["date"].astype(str)
        + " "
        + transactions_filtrées["heure_str"]
    )
    # Agrégation + datetime propre pour le graphe CA
    transactions_plot = transactions_filtrées.groupby(
        ["date", "heure"], as_index=False
    ).agg({"chiffre_affaires": "sum"})
    transactions_plot["heure_str"] = transactions_plot["heure"].apply(
        lambda h: f"{int(h):02d}:00"
    )
    transactions_plot["datetime"] = pd.to_datetime(
        transactions_plot["date"].astype(str) + " " + transactions_plot["heure_str"]
    )
    transactions_plot_limité = transactions_plot[
        transactions_plot["date"].isin(dates_limite)
    ]
    titre_graphe_cf = (
        f"Chiffre d'affaires par heure agrégé (du {date_debut} au {date_fin})"
    )

    fig_ca = px.line(
        transactions_plot_limité,
        x="datetime",
        y="chiffre_affaires",
        title=titre_graphe_cf,
        markers=True,
        labels={"datetime": "Date", "chiffre_affaires": "Chiffre d'affaires (€)"},
    )
    fig_ca.update_layout(xaxis=dict(tickformat="%a %d %b"))
    fig_ca.update_traces(line_color="#00A676")
    st.plotly_chart(fig_ca, use_container_width=True)

    # COMPARAISON AGRÉGÉE (avec tous les capteurs du magasin)
    st.subheader("📉 Visiteurs vs Transactions (agrégés - tous capteurs)")

    visiteurs_magasin_agg = (
        visiteurs_df[
            (visiteurs_df["city_store"] == magasin)
            & (visiteurs_df["date"].dt.date >= date_range[0])
            & (visiteurs_df["date"].dt.date <= date_range[1])
            & (visiteurs_df["heure"].between(heure_range[0], heure_range[1]))
        ]
        .groupby(["date", "heure"], as_index=False)["nb_visiteurs"]
        .sum()
    )

    transactions_agg = transactions_filtrées.groupby(
        ["date", "heure"], as_index=False
    ).agg({"nb_transactions": "sum", "chiffre_affaires": "sum"})

    df_merge = pd.merge(
        visiteurs_magasin_agg, transactions_agg, on=["date", "heure"], how="inner"
    )
    df_merge["heure_str"] = df_merge["heure"].apply(lambda h: f"{int(h):02d}:00")
    df_merge["taux_conversion"] = df_merge["nb_transactions"] / df_merge["nb_visiteurs"]
    df_merge["ca_par_visiteur"] = (
        df_merge["chiffre_affaires"] / df_merge["nb_visiteurs"]
    )

    col5, col6 = st.columns(2)
    col5.metric(
        "Taux de conversion global", f"{df_merge['taux_conversion'].mean() * 100:.1f} %"
    )
    col6.metric("CA / Visiteur", f"{df_merge['ca_par_visiteur'].mean():.2f} €")

    # Taux de conversion global par jour (pondéré)
    df_conversion_jour = (
        df_merge.groupby(df_merge["date"].dt.date)
        .agg({"nb_visiteurs": "sum", "nb_transactions": "sum"})
        .reset_index()
    )
    df_conversion_jour["taux_conversion"] = (
        df_conversion_jour["nb_transactions"] / df_conversion_jour["nb_visiteurs"]
    )
    df_conversion_jour["date_str"] = df_conversion_jour["date"].apply(
        lambda d: d.strftime("%d %b %Y")
    )

    fig_tc_jour = px.bar(
        df_conversion_jour,
        x="date_str",
        y="taux_conversion",
        text="nb_visiteurs",
        title="Taux de conversion global par jour",
        labels={"date_str": "Date", "taux_conversion": "Taux de conversion"},
        color_discrete_sequence=["#3D90D7"]  # Variante plus vibrante
    )
    st.plotly_chart(fig_tc_jour, use_container_width=True)

    # Taux de conversion global par heure (pondéré)
    df_conversion = (
        df_merge.groupby("heure_str")
        .agg({"nb_visiteurs": "sum", "nb_transactions": "sum"})
        .reset_index()
    )
    df_conversion["taux_conversion"] = (
        df_conversion["nb_transactions"] / df_conversion["nb_visiteurs"]
    )

    fig_tc = px.bar(
        df_conversion,
        x="heure_str",
        y="taux_conversion",
        text="nb_visiteurs",  # facultatif : affiche le volume de visiteurs sur chaque barre
        title="Taux de conversion global par heure",
        labels={"heure_str": "Heure", "taux_conversion": "Taux de conversion"},
        color_discrete_sequence=["#3B8ED0"]  # Conversion
    )
    st.plotly_chart(fig_tc, use_container_width=True)


if __name__ == "__main__":
    main()
