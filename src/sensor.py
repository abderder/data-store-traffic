import random
import hashlib
from datetime import date, time, datetime


class Sensor:
    def __init__(self, store_id: int, sensor_id: int, date_obj: date, heure_obj: time):
        self.sensor_id = sensor_id
        self.date_obj = date_obj
        self.heure_obj = heure_obj
        self.store_id = store_id

    def visiteur_jour_heure_obj(self) -> int:
        # Création d'une chaîne unique avec la date_obj, l'heure_obj, l'ID capteur et magasin
        texte = f"{self.date_obj.isoformat()}-{self.heure_obj.strftime('%H:%M')}"
        texte = texte + f" {self.sensor_id} - {self.store_id}"
        # Hash de la chaîne pour générer une seed unique
        hash_obj = hashlib.sha256(texte.encode())
        seed = int(hash_obj.hexdigest(), 16)
        # Initialisation du générateur aléatoire avec la seed
        random.seed(seed)
        # Vérifie si on est entre 19h et 9h du matin
        heure = self.heure_obj.hour
        if heure >= 19 or heure < 9:
            # Génère un nombre entre 0 et 1
            chance = random.random()
            if chance > 0.001:
                return 0
            else:
                return random.randint(1, 10)
        else:
            chance = random.random()
            if chance > 0.04:
                return random.randint(40, 70)
            else:
                chance2 = random.random()
                if chance2 < 0.50:
                    return None
                elif chance2 < 0.70:
                    return random.randint(300, 400)
                else:
                    return random.randint(1, 5)



if __name__ == "__main__":
    d = date.fromisoformat("2023-04-15")
    t = time(hour=15, minute=30)

    sensor = Sensor(store_id=1, sensor_id=2, date_obj=d, heure_obj=t)
    print(sensor.visiteur_jour_heure_obj())
