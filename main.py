from fastapi import FastAPI, HTTPException
import time
from fastapi.responses import JSONResponse
import pandas as pd
from datetime import date, time, timedelta, datetime
from src.realistic_sensor import RealisticSensorPerDay, RealisticSensorPerHour
from src.realistic_sensor import RealisticStoreSensorPerDay, RealisticStoreSensorPerHour
from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
import os
import hashlib
import random

# Charger les variables d'environnement
load_dotenv()

# Récupérer les infos du .env
sas_token = os.getenv("BLOB_SAS_TOKEN")
base_url = os.getenv("Blob_SAS_URL")

magasins = pd.read_csv(f"{base_url}/magasins.csv?{sas_token}")
capteurs = pd.read_csv(f"{base_url}/capteurs.csv?{sas_token}")


app = FastAPI()


@app.get("/visiteurs")
def visit(
    city_store: str = None,
    year: int | None = None,
    month: int | None = None,
    day: int | None = None,
    hour: int | None = None,
    sensor_id: int | None = None,
) -> JSONResponse:
    # Store name vérification
    if city_store is None or city_store.strip() == "":
        raise HTTPException(status_code=404, detail="Ville du magasin obligatoire")

    if city_store.lower() not in magasins["city_store"].str.lower().values:
        raise HTTPException(status_code=404, detail="Store non disponible")

    # Sensors verification
    storeid = magasins.loc[
        magasins["city_store"].str.contains(city_store, case=False), "store_id"
    ].iloc[0]
    coef_store = magasins.loc[
        magasins["city_store"].str.contains(city_store, case=False), "coef"
    ].iloc[0]
    sensors_data = list(
        capteurs.loc[capteurs["store_id"] == storeid, ["sensor_id", "coef"]].itertuples(
            index=False, name=None
        )
    )
    sensors_ids = [sensor_id for sensor_id, coeff in sensors_data]
    if sensor_id is not None:
        if sensor_id not in sensors_ids:
            raise HTTPException(
                status_code=404,
                detail=f"Capteur non disponible, le nombre de de capteurs est {max(sensors_ids)}",
            )

    # Date vérification
    # Vérifier si certains sont remplis et d'autres pas
    date_params = [year, month, day]
    filled = [p is not None for p in date_params]  # liste de booléens

    if any(filled) and not all(
        filled
    ):  # S’il y en a au moins un qui est rempli mais pas tous
        raise HTTPException(
            status_code=404,
            detail="Si tu donnes une date,(year, month, day) doivent être tous remplis.",
        )
    # Si heure est donné sans la date complète
    if hour is not None and not all(filled):
        raise HTTPException(
            status_code=404,
            detail="Si tu donnes une heure, la date (year, month, day) doit être complète.",
        )
    today = date.today()
    d = None
    if all(filled):
        try:
            d = date(year, month, day)
        except ValueError:
            raise HTTPException(status_code=404, detail="La date est invalide !")
        # Vérifie si la date est dans le futur
        if d > today or d < date.fromisoformat("2021-01-01"):
            return JSONResponse(
                status_code=200,
                content={"message": "Data non disponible pour cette date"},
            )

    # Heure vérification
    now = datetime.now()
    current_hour = now.hour
    one_hour_ago = (current_hour - 1) % 24
    if hour is not None:
        try:
            t = time(hour, 0)
        except ValueError:
            raise HTTPException(
                status_code=404,
                detail="L'heure est invalide ! L'heure doit être compris entre 0 et 23",
            )
        now = datetime.now()
        current_hour = now.hour
        if d == today:
            if t.hour > one_hour_ago:
                return JSONResponse(
                    status_code=404,
                    content={"message": "Data non disponible pour cette date"},
                )
    if (all(param is None for param in [year, month, day, hour, sensor_id])) or (
        d == today
    ):
        # Exécuter la fonction spéciale si juste city_store est donné
        visiteurs = RealisticStoreSensorPerDay(
            storeid, sensors_data, today, coef_store, one_hour_ago
        )
        if visiteurs == -1:
            return JSONResponse(
                status_code=200,
                content={
                    "message": f"Magasin fermé!",
                    "is_closed": True,
                    "visiteurs": visiteurs,
                },
            )
        return JSONResponse(
            status_code=200,
            content={
                "message": f"Nombre de visiteurs le {today} est : {visiteurs}",
                "is_closed": False,
                "visiteurs": visiteurs,
            },
        )
    if all(param is None for param in [hour, sensor_id]):
        visiteurs = RealisticStoreSensorPerDay(storeid, sensors_data, d, coef_store)
        if visiteurs == -1:
            return JSONResponse(
                status_code=200,
                content={
                    "message": f"Magasin fermé!",
                    "is_closed": True,
                    "visiteurs": visiteurs,
                },
            )
        return JSONResponse(
            status_code=200,
            content={
                "message": f"Nombre de visiteurs le {d} est : {visiteurs}",
                "is_closed": False,
                "visiteurs": visiteurs,
            },
        )
    if all(param is None for param in [year, month, day, hour]) or (d == today):
        coef_sensor = next(
            (coeff for sensor_id2, coeff in sensors_data if sensor_id2 == sensor_id),
            None,
        )
        visiteurs = RealisticSensorPerDay(
            storeid, sensor_id, today, coef_store, coef_sensor, one_hour_ago
        )
        if visiteurs == -1:
            return JSONResponse(
                status_code=200,
                content={
                    "message": f"Magasin fermé!",
                    "is_closed": True,
                    "visiteurs": visiteurs,
                },
            )
        return JSONResponse(
            status_code=200,
            content={
                "message": f"Nombre de visiteurs le {today} pour capteur {sensor_id} est : {visiteurs}",
                "is_closed": False,
                "visiteurs": visiteurs,
            },
        )
    if hour is None:
        coef_sensor = next(
            (coeff for sensor_id2, coeff in sensors_data if sensor_id2 == sensor_id),
            None,
        )
        visiteurs = RealisticSensorPerDay(
            storeid, sensor_id, d, coef_store, coef_sensor
        )
        if visiteurs == -1:
            return JSONResponse(
                status_code=200,
                content={
                    "message": f"Magasin fermé!",
                    "is_closed": True,
                    "visiteurs": visiteurs,
                },
            )
        return JSONResponse(
            status_code=200,
            content={
                "message": f"Nombre de visiteurs le {today} pour capteur {sensor_id} est : {visiteurs}",
                "is_closed": False,
                "visiteurs": visiteurs,
            },
        )
    if sensor_id is None:
        visiteurs = RealisticStoreSensorPerHour(storeid, sensors_data, d, t, coef_store)
        if visiteurs == -1:
            return JSONResponse(
                status_code=200,
                content={
                    "message": f"Magasin fermé!",
                    "is_closed": True,
                    "visiteurs": visiteurs,
                },
            )
        return JSONResponse(
            status_code=200,
            content={
                "message": f"Nombre de visiteurs le {d} à  {t} est : {visiteurs}",
                "is_closed": False,
                "visiteurs": visiteurs,
            },
        )

    coef_sensor = next(
        (coeff for sensor_id2, coeff in sensors_data if sensor_id2 == sensor_id),
        None,
    )
    visiteurs = RealisticSensorPerHour(
        storeid, sensor_id, d, t, coef_store, coef_sensor
    )
    if visiteurs == -1:
        return JSONResponse(
            status_code=200,
            content={
                "message": f"Magasin fermé!",
                "is_closed": True,
                "visiteurs": visiteurs,
            },
        )
    return JSONResponse(
        status_code=200,
        content={
            "message": f"Nombre de visiteurs le {d} à {t} pour capteur {sensor_id} est : {visiteurs}",
            "is_closed": False,
            "visiteurs": visiteurs,
        },
    )


@app.get("/transactions")
def simuler_transactions(
    city_store: str,
    year: int,
    month: int,
    day: int,
    hour: int

) -> JSONResponse:
    if city_store.lower() not in magasins["city_store"].str.lower().values:
        raise HTTPException(status_code=404, detail="Store non disponible")
    try:
        date_transaction = datetime(year, month, day, hour)
    except ValueError:
        raise HTTPException(status_code=404, detail="Date invalide")
    
    now = datetime.now()
    if date_transaction > now or date_transaction.date() < date.fromisoformat("2021-01-01") :
        return JSONResponse(content={"result": "pas de data"})
    
    if date_transaction.weekday() >= 5:  # 5 = samedi, 6 = dimanche
        return JSONResponse(status_code=404,content={"result": -1})
    
    if hour > 19 or hour < 9:
        return JSONResponse(content={"result": 0})


     #Génération d'une seed unique
    texte = f"{day}/{month}/{year}-{hour}-{city_store}"
    hash_obj = hashlib.sha256(texte.encode())
    seed = int(hash_obj.hexdigest(), 16)
    random.seed(seed)

    
    
    storeid = magasins.loc[
        magasins["city_store"].str.contains(city_store, case=False), "store_id"
    ].iloc[0]
    coef_store = magasins.loc[
        magasins["city_store"].str.contains(city_store, case=False), "coef"
    ].iloc[0]
    sensors_data = list(
        capteurs.loc[capteurs["store_id"] == storeid, ["sensor_id", "coef"]].itertuples(
            index=False, name=None
        )
    )
    coef_date = {
        0: 0.4,  # Lundi
        1: 0.5,  # Mardi
        2: 0.6,  # Mercredi
        3: 0.5,  # Jeudi
        4: 0.6,  # Vendredi
    }

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
    visiteurs = random.randint(90, 120)
    date_coeff = coef_date.get(date_transaction.weekday(), 1.0)
    heure_coef = coef_heure.get(hour, 1.0)
    base_visiteurs = visiteurs * coef_store * date_coeff * heure_coef
    visiteurs_totaux = int(base_visiteurs * sum(coef for _, coef in sensors_data))

    taux_conversion = random.uniform(0.15, 0.30)
    transactions = int(visiteurs_totaux * taux_conversion)
    montants_transactions = [random.uniform(10, 120) for _ in range(transactions)]
    chiffre_affaires = sum(montants_transactions)
    return JSONResponse(content={
        "transactions": transactions,
        "chiffre_affaires": round(chiffre_affaires, 2)
    })
    


