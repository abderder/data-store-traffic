import pandas as pd
from datetime import date, time
from src.realistic_sensor import RealisticSensorPerHour, RealisticSensorPerDay
from src.realistic_sensor import RealisticStoreSensorPerHour, RealisticStoreSensorPerDay


# Définir une date et une heure de test
test_date = date(2025, 4, 21)  # Mercredi
test_time = time(15, 0)  # Midi

store_id = 1  # Paris
sensor_data = [
    (101, 1.0),
    (102, 0.8),
    (103, 1.2)
]
coef_store = 0.9

visiteurs_total = RealisticStoreSensorPerDay(store_id, sensor_data, date(2025, 4, 23), coef_store)

print(f"Visiteurs totaux : {visiteurs_total}")