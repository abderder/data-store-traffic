import pandas as pd
import random
import hashlib
from datetime import datetime, timedelta, date, time

# Charger magasins et capteurs
magasins = pd.read_csv("./data/magasins.csv")
capteurs = pd.read_csv("./data/capteurs.csv")

# Coefficients
coef_date = {0: 0.4, 1: 0.5, 2: 0.6, 3: 0.5, 4: 0.6}  # Lundi à Vendredi
coef_heure = {
    9: 0.3,
    10: 0.5,
    11: 0.6,
    12: 0.7,
    13: 0.9,
    14: 0.6,
    15: 0.6,
    16: 0.6,
    17: 0.9,
    18: 0.9,
    19: 0.7,
}

# Liste pour stocker les données
data = []

# Période à générer
start_date = date(2023, 4, 27)
end_date = date(2025, 4, 27)

current_date = start_date
while current_date <= end_date:
    # Skip week-ends
    if current_date.weekday() >= 5:
        current_date += timedelta(days=1)
        continue

    for _, magasin in magasins.iterrows():
        store_id = magasin["store_id"]
        coef_store = magasin["coef"]

        # Récupérer les capteurs du magasin
        sensors_data = capteurs[capteurs["store_id"] == store_id][
            ["sensor_id", "coef"]
        ].values.tolist()
        for hour in range(0, 24):  # 0h à 23h
            # Générer une seed unique pour reproductibilité
            texte = f"{current_date.day}/{current_date.month}/{current_date.year}-{hour}-{store_id}"
            hash_obj = hashlib.sha256(texte.encode())
            seed = int(hash_obj.hexdigest(), 16)
            random.seed(seed)

            if 9 <= hour <= 19:
                # Heures d'ouverture → calcul normal
                visiteurs = random.randint(90, 120)
                date_coeff = coef_date.get(current_date.weekday(), 1.0)
                heure_coeff = coef_heure.get(hour, 1.0)
                base_visiteurs = visiteurs * coef_store * date_coeff * heure_coeff
                visiteurs_totaux = int(
                    base_visiteurs * sum(coef for _, coef in sensors_data)
                )

                taux_conversion = random.uniform(0.15, 0.30)
                transactions = int(visiteurs_totaux * taux_conversion)
                montants_transactions = [
                    random.uniform(10, 120) for _ in range(transactions)
                ]
                chiffre_affaires = sum(montants_transactions)
            else:
                # Hors heures d'ouverture → zéro transactions
                transactions = 0
                chiffre_affaires = 0

            data.append(
                {
                    "store_id": store_id,
                    "date": current_date,
                    "heure": time(hour, 0),
                    "transactions": transactions,
                    "chiffre_affaires": round(chiffre_affaires, 2),
                }
            )

    current_date += timedelta(days=1)

# Créer le DataFrame
df_transactions = pd.DataFrame(data)
df_transactions["store_id"] = df_transactions["store_id"].astype(str)

# Fixer la seed
random.seed(42)

# Taille de l'échantillon pour corrompre (ex : 5% des lignes)
sample_size = int(0.01 * len(df_transactions))

# Corruption store_id
for idx in random.sample(range(len(df_transactions)), sample_size):
    df_transactions.at[idx, "store_id"] = random.choice([None, "898", "941"])

# Corruption date (formats différents)
for idx in random.sample(range(len(df_transactions)), sample_size):
    original_date = df_transactions.at[idx, "date"]
    df_transactions.at[idx, "date"] = random.choice(
        [
            pd.to_datetime(original_date).strftime("%d/%m/%Y"),
            pd.to_datetime(original_date).strftime("%Y/%m/%d"),
        ]
    )

# Corruption heure (formats différents)
for idx in random.sample(range(len(df_transactions)), sample_size):
    original_hour = df_transactions.at[idx, "heure"]
    df_transactions.at[idx, "heure"] = random.choice(
        [
            pd.to_datetime(original_hour, format="%H:%M:%S").strftime("%Hh%M"),
            pd.to_datetime(original_hour, format="%H:%M:%S").strftime("%H%M"),
        ]
    )

# Corruption transactions (valeurs aberrantes)
for idx in random.sample(range(len(df_transactions)), sample_size):
    df_transactions.at[idx, "transactions"] = random.choice([-5, 9999, None])

# Corruption chiffre_affaires (valeurs aberrantes)
for idx in random.sample(range(len(df_transactions)), sample_size):
    df_transactions.at[idx, "chiffre_affaires"] = random.choice(
        [-100.0, None, 999999.99]
    )
print(df_transactions["store_id"].unique())

# Exporter en CSV
df_transactions.to_csv("./data/transactions_20230427_20250427.csv", index=False)
