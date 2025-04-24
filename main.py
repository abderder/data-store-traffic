from fastapi import FastAPI, HTTPException
import time
from fastapi.responses import JSONResponse
import pandas as pd
from datetime import date, time, timedelta, datetime
from src.realistic_sensor import RealisticSensorPerDay, RealisticSensorPerHour
from src.realistic_sensor import RealisticStoreSensorPerDay, RealisticStoreSensorPerHour

magasins = pd.read_csv("./data/magasins.csv")
capteurs = pd.read_csv("./data/capteurs.csv")


app = FastAPI()


@app.get("/visiteurs")
def visit(
    store_name: str = None,
    year: int | None = None,
    month: int | None = None,
    day: int | None = None,
    hour: int | None = None,
    sensor_id: int | None = None,
) -> JSONResponse:
    # Store name vérification
    if store_name is None or store_name.strip() == "":
        raise HTTPException(status_code=404, detail="Nom du magasin obligatoire")

    if store_name.lower() not in magasins["store_name"].str.lower().values:
        raise HTTPException(status_code=404, detail="Store non disponible")

    # Sensors verification
    storeid = magasins.loc[
        magasins["store_name"].str.contains(store_name, case=False), "store_id"
    ].iloc[0]
    coef_store = magasins.loc[
        magasins["store_name"].str.contains(store_name, case=False), "coef"
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
        if d > today:
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
        # Exécuter la fonction spéciale si juste store_name est donné
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
    if all(param is None for param in [year, month, day, hour]):
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

    return JSONResponse(status_code=200, content=f"Hello {store_name}, {storeid}")
