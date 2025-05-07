import pandas as pd
from src.realistic_sensor import RealisticSensorPerHour
from datetime import date, time, datetime, timedelta
import random

capteurs = pd.read_csv("./data/capteurs.csv")
magasins = pd.read_csv("./data/magasins.csv")

data = []
end_date = date(2025, 4, 27)
start_date = date(2023, 4, 27)

while start_date <= end_date:
    for id_magasin in magasins["store_id"]:
        coef_magasin = magasins[magasins["store_id"] == id_magasin]["coef"].iloc[0]
        for id_capteur in capteurs[capteurs["store_id"] == id_magasin]["sensor_id"]:
            coef_capteur = capteurs[
                (capteurs["sensor_id"] == id_capteur)
                & (capteurs["store_id"] == id_magasin)
            ]["coef"].iloc[0]
            heure = time(9, 0)
            heure_datetime = datetime.combine(start_date, heure)
            fin_journee = datetime.combine(start_date, time(23, 59))  # Fin à 23h59
            while heure_datetime <= fin_journee:
                visiteurs = RealisticSensorPerHour(
                    id_magasin,
                    id_capteur,
                    heure_datetime.date(),
                    heure_datetime.time(),
                    coef_magasin,
                    coef_capteur,
                )
                data.append(
                    {
                        "store_id": id_magasin,
                        "sensor_id": id_capteur,
                        "date": heure_datetime.date(),
                        "heure": heure_datetime.time(),
                        "nb_visiteurs": visiteurs,
                    }
                )
                heure_datetime += timedelta(hours=1)

    start_date += timedelta(days=1)
df_visiteurs = pd.DataFrame(data)
df_visiteurs["store_id"] = df_visiteurs["store_id"].astype(str)
df_visiteurs["sensor_id"] = df_visiteurs["sensor_id"].astype(str)
# Fixer la seed
random.seed(42)

# 5% des lignes pour chaque anomalie
sample_size = int(0.01 * len(df_visiteurs))

# Anomalies store_id (None ou store_999)
for idx in random.sample(range(len(df_visiteurs)), sample_size):
    df_visiteurs.at[idx, "store_id"] = random.choice([None, 999])

# Anomalies sensor_id (None ou 999)
for idx in random.sample(range(len(df_visiteurs)), sample_size):
    df_visiteurs.at[idx, "sensor_id"] = random.choice([None, 999])

# Anomalies date (format différent)
for idx in random.sample(range(len(df_visiteurs)), sample_size):
    original_date = df_visiteurs.at[idx, "date"]
    df_visiteurs.at[idx, "date"] = random.choice(
        [original_date.strftime("%d/%m/%Y"), original_date.strftime("%Y/%m/%d")]
    )

# Anomalies heure (format différent)
for idx in random.sample(range(len(df_visiteurs)), sample_size):
    original_heure = df_visiteurs.at[idx, "heure"]
    df_visiteurs.at[idx, "heure"] = random.choice(
        [original_heure.strftime("%Hh%M"), original_heure.strftime("%H%M")]
    )

# Anomalies nb_visiteurs (valeurs aberrantes)
for idx in random.sample(range(len(df_visiteurs)), sample_size):
    df_visiteurs.at[idx, "nb_visiteurs"] = random.choice([-999, None])


df_visiteurs.to_csv("./data/visiteurs_20230427_20250427.csv", index=False)
