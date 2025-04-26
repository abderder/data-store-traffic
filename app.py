import sys
import os
from datetime import date
import requests


year, month, day = sys.argv[1].split("-")
url = "https://data-store-traffic.onrender.com/visiteurs"

day = 1
while day < 30:
    params = {"city_store": "PARIS", "year": year, "month": month, "day": day}
    response = requests.get(url, params=params)
    # Vérifier le statut de la réponse
    if response.status_code == 200:
        data = response.json()  # Si la réponse est en JSON
        visiteurs = data.get("visiteurs")
        print(visiteurs)
    else:
        print(f"Erreur {response.status_code}")
    day += 1


# if len(sys.argv) > 2:
#     year, month, day = [int(v) for v in sys.argv[1].split("-")]
#     hour, minute = [int(v) for v in sys.argv[2].split("-")]
