import pandas as pd
from datetime import date, time
from src.realistic_sensor import RealisticSensorPerHour, RealisticSensorPerDay



# Définir une date et une heure de test
test_date = date(2025, 4, 21)  # Mercredi
test_time = time(15, 0)        # Midi

visiteurs = RealisticSensorPerDay(
    store_id=1,
    sensor_id=2,
    date_obj=test_date,
    coef_store=1.2,
    coef_sensor=1.2
    )

print(f"Visiteurs ajustés pour capteur {1} au magasin {1} : {visiteurs}")
