from fastapi import FastAPI, HTTPException
import time
from fastapi.responses import JSONResponse
import pandas as pd
from datetime import date, time, timedelta, datetime

magasins = pd.read_csv('./data/magasins.csv')
capteurs = pd.read_csv('./data/capteurs.csv')


app = FastAPI()

@app.get("/visit")




def visit(
    store_name: str = None,
    year: int | None = None,
    month: int | None = None,
    day: int | None = None,
    hour: int | None = None,
    sensor_id: int | None = None
) -> JSONResponse:
    # Store name vérification
    if store_name is None or store_name.strip() == "":
        raise HTTPException(status_code=400, detail="Nom du magasin obligatoire")
    if store_name.lower() not in magasins['store_name'].str.lower().values:
        raise HTTPException(status_code=404, detail="Store non disponible")
    
    # Date vérification
    # Vérifier si certains sont remplis et d'autres pas
    date_params = [year, month, day]
    filled = [p is not None for p in date_params] # liste de booléens

    if any(filled) and not all(filled): # S’il y en a au moins un qui est rempli mais pas tous
        raise HTTPException(
            status_code=400, detail="Si tu donnes une date,(year, month, day) doivent être tous remplis."
        )
    # Si heure est donné sans la date complète
    if time is not None and not all(filled):
        raise HTTPException(
            status_code=400,
            detail="Si tu donnes une heure, la date (year, month, day) doit être complète."
        )
    
    if all(filled):
        try:
            d = date(year, month, day)
        except ValueError:
            raise HTTPException(status_code=400, detail="La date est invalide !")
         # Vérifie si la date est dans le futur
        today = date.today()
        if d > today:
            return JSONResponse(status_code=200, content={"message": "Data non disponible pour cette date"})
    
    # Heure vérification
    if hour is not None:
        try:
            t = time(hour, 0)
        except ValueError:
            raise HTTPException(status_code=400, detail="L'heure est invalide ! L'heure doit être compris entre 0 et 23")
        now = datetime.now()
        current_hour = now.hour
        if d == today:
            now = datetime.now()
            current_hour = now.hour
            one_hour_ago = (current_hour - 1) % 24
            if t.hour > one_hour_ago:
                return JSONResponse(status_code=200, content={"message": "Data non disponible pour cette date"})

    
    return JSONResponse(status_code=200, content=f"Hello {store_name}")
