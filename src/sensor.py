import random
import hashlib
from datetime import date, time


class Sensor:
    def __init__(self, store_id: int, sensor_id: int, date: date, heure: time):
        self.sensor_id = sensor_id
        self.date = date
        self.heure = heure
        self.store_id = store_id

    def visiteur_jour_heure(self) -> int:
        # Création d'une chaîne unique avec la date, l'heure, l'ID capteur et magasin
        texte = f"{self.date.isoformat()}-{self.heure.strftime('%H:%M')}"
        texte = texte + f" {self.sensor_id} - {self.store_id}"
        # Hash de la chaîne pour générer une seed unique
        hash_obj = hashlib.sha256(texte.encode())
        seed = int(hash_obj.hexdigest(), 16)
        # Initialisation du générateur aléatoire avec la seed
        random.seed(seed)
        return random.randint(1, 100)  # Nombre aléatoire déterministe entre 1 et 100


if __name__ == "__main__":
    d = date.fromisoformat("2023-04-21")
    t = time(hour=15, minute=30)

    sensor = Sensor(store_id=1, sensor_id=2, date=d, heure=t)
    print(sensor.visiteur_jour_heure())
