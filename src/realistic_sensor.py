from src.sensor import Sensor
from datetime import date, time
import hashlib
import random



def RealisticSensorPerHour (store_id: int, sensor_id: int, date_obj: date, heure_obj: time,
                      coef_store: float, coef_sensor: float) -> int:
    
    
    coef_date = {
    0: 0.8,  # Lundi
    1: 0.9,  # Mardi
    2: 1.0,  # Mercredi
    3: 1.1,  # Jeudi
    4: 1.2,  # Vendredi
    }

    coef_heure = {
    9: 0.8,
    10: 0.9,
    11: 1.0,
    12: 1.3,
    13: 1.1,
    14: 1.0,
    15: 1.1,
    16: 1.2,
    17: 1.4,
    18: 1.5,
    19: 1.1
    }
    
    if date_obj.weekday() in (5,6) :
        return -1
    
    visiteurs_bruts = Sensor(
                store_id, sensor_id, date_obj, heure_obj
            ).visiteur_jour_heure()
    if visiteurs_bruts is None:
        return None

    coef_total = 1.0
    coef_total *= coef_store
    coef_total *= coef_sensor
    coef_total *= coef_date.get(date_obj.weekday(), 1.0)
    coef_total *= coef_heure.get(heure_obj.hour, 1.0)

    
    # Création d'une seed déterministe
    texte_seed = f"{store_id}-{sensor_id}-{date_obj.isoformat()}-{heure_obj.strftime('%H:%M')}"
    seed = int(hashlib.sha256(texte_seed.encode()).hexdigest(), 16)
    random.seed(seed)

    # Petite variation contrôlée (ex : +/-5%)
    fluctuation = random.uniform(0.95, 1.05)
    coef_total *= fluctuation

    visiteurs_ajustes = int(visiteurs_bruts * coef_total)
    return visiteurs_ajustes

def RealisticSensorPerDay(store_id: int, sensor_id: int, date_obj: date, coef_store: float, coef_sensor: float) -> int:
    if date_obj.weekday() in (5,6) :
        return -1
    nb_visiteurs_total = 0
    for heure in range(9,20):
        visiteurs = RealisticSensorPerHour(
        store_id,
        sensor_id,
        date_obj,
        time(heure, 0), 
        coef_store,
        coef_sensor
        )
        if visiteurs is not None:
            nb_visiteurs_total += visiteurs
    return nb_visiteurs_total


