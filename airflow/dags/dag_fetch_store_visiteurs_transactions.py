from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pytz
import pandas as pd
import urllib.parse
import shutil
import os
import requests
from azure.storage.blob import BlobServiceClient

default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="fetch_and_store_visiteurs_transactions",
    start_date=datetime(2025, 5, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["api", "azure", "parquet"],
)

local_dir = "/tmp/store_data"
os.makedirs(local_dir, exist_ok=True)


def download_csv_from_blob():
    conn = BaseHook.get_connection("azure_blob_conn_id1")
    extras = conn.extra_dejson
    sas_url = extras["sas_url"]
    sas_token = extras["sas_token"]

    magasins_url = f"{sas_url}/magasins.csv?{sas_token}"
    capteurs_url = f"{sas_url}/capteurs.csv?{sas_token}"

    magasins = pd.read_csv(magasins_url)
    capteurs = pd.read_csv(capteurs_url)

    magasins.to_csv(f"{local_dir}/magasins.csv", index=False)
    capteurs.to_csv(f"{local_dir}/capteurs.csv", index=False)


def fetch_data_from_api(ti):
    os.makedirs(local_dir, exist_ok=True)
    magasins = pd.read_csv(f"{local_dir}/magasins.csv")
    capteurs = pd.read_csv(f"{local_dir}/capteurs.csv")

    paris_time = datetime.now(pytz.timezone("Europe/Paris")) - timedelta(hours=1)
    year, month, day, hour = (
        paris_time.year,
        paris_time.month,
        paris_time.day,
        paris_time.hour,
    )
    # year, month, day, hour = 2025, 5, 2, 12

    all_visiteurs = []
    all_transactions = []

    for _, row in magasins.iterrows():
        city_store = row["city_store"]
        store_id = row["store_id"]

        # capteurs pour ce magasin
        capteurs_magasin = capteurs[capteurs["store_id"] == store_id]

        for _, capteur in capteurs_magasin.iterrows():
            sensor_id = capteur["sensor_id"]
            url = f"https://data-store-traffic.onrender.com/visiteurs?city_store={city_store}&sensor_id={sensor_id}&year={year}&month={month}&day={day}&hour={hour}"
            r = requests.get(url)
            print(f"requests : {url}")
            if r.status_code == 200:
                data = r.json()
                print(f"data : {data}")
                all_visiteurs.append(
                    {
                        "store_id": store_id,
                        "sensor_id": sensor_id,
                        "date": f"{year}-{month:02d}-{day:02d}",
                        "heure": f"{hour:02d}:00:00",
                        "nb_visiteurs": data["visiteurs"],
                    }
                )

        # transactions
        url = f"https://data-store-traffic.onrender.com/transactions?city_store={city_store}&year={year}&month={month}&day={day}&hour={hour}"
        r = requests.get(url)
        print(f"requests : {url}")
        if r.status_code == 200:
            data = r.json()
            print(f"data : {data}")
            all_transactions.append(
                {
                    "store_id": store_id,
                    "date": f"{year}-{month:02d}-{day:02d}",
                    "heure": f"{hour:02d}:00:00",
                    "transactions": data["transactions"],
                    "chiffre_affaires": data["chiffre_affaires"],
                }
            )

    # Sauvegarde les deux datasets localement
    df_visiteurs = pd.DataFrame(all_visiteurs)
    df_transactions = pd.DataFrame(all_transactions)

    # Définir le schéma attendu
    visiteurs_columns = ["store_id", "sensor_id", "date", "hour", "nb_visiteurs"]
    transactions_columns = [
        "store_id",
        "date",
        "hour",
        "transactions",
        "chiffre_affaires",
    ]

    # Visiteurs
    if df_visiteurs.empty:
        df_visiteurs = pd.DataFrame(columns=visiteurs_columns)
    df_visiteurs.to_parquet(f"{local_dir}/visiteurs.parquet", index=False)

    # Transactions
    if df_transactions.empty:
        df_transactions = pd.DataFrame(columns=transactions_columns)
    df_transactions.to_parquet(f"{local_dir}/transactions.parquet", index=False)


def upload_to_blob():
    conn = BaseHook.get_connection("azure_blob_conn_id2")
    extras = conn.extra_dejson
    sas_url = extras["sas_url"]
    sas_token = extras["sas_token"]

    paris_time = datetime.now(pytz.timezone("Europe/Paris")) - timedelta(hours=1)
    date_str = paris_time.strftime("%Y-%m-%d")
    hour_str = paris_time.strftime("%H")

    # date_str = "2025-05-02"
    # hour_str = 12

    # Découper sas_url correctement
    parsed_url = urllib.parse.urlparse(sas_url)
    account_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    container_name = parsed_url.path.strip("/")

    blob_service_client = BlobServiceClient(
        account_url=account_url, credential=sas_token
    )
    container_client = blob_service_client.get_container_client(container_name)
    # Upload visiteurs
    with open(f"{local_dir}/visiteurs.parquet", "rb") as f:
        path = f"visiteurs/date={date_str}/hour={hour_str}/data.parquet"
        container_client.upload_blob(path, f, overwrite=True)

    # Upload transactions
    with open(f"{local_dir}/transactions.parquet", "rb") as f:
        path = f"transactions/date={date_str}/hour={hour_str}/data.parquet"
        container_client.upload_blob(path, f, overwrite=True)


def clean_up_local_files():
    if os.path.exists(local_dir):
        shutil.rmtree(local_dir)


# Définition des tâches Airflow
t1 = PythonOperator(
    task_id="download_csv_from_blob",
    python_callable=download_csv_from_blob,
    dag=dag,
)

t2 = PythonOperator(
    task_id="fetch_data_from_api",
    python_callable=fetch_data_from_api,
    dag=dag,
)

t3 = PythonOperator(
    task_id="upload_to_blob",
    python_callable=upload_to_blob,
    dag=dag,
)
t4 = PythonOperator(
    task_id="clean_up_local_files",
    python_callable=clean_up_local_files,
    dag=dag,
)


t1 >> t2 >> t3 >> t4  # Ordre d'exécution
