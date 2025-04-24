import hashlib
import random
from datetime import date, time, timedelta

# import sys


class Sensor:
    """Classe représentant un capteur d'un magasin simulant le nombre de visiteurs."""

    def __init__(self, store_id: int, sensor_id: int, date_obj: date, heure_obj: time):
        self.sensor_id = sensor_id
        self.date_obj = date_obj
        self.heure_obj = heure_obj
        self.store_id = store_id

    def visiteur_jour_heure(self) -> int:
        """
        Génère un nombre de visiteurs simulé basé sur l'heure, la date, le magasin et le capteur.

        Returns:
            int | None: Nombre de visiteurs, ou None pour simuler une donnée manquante.
        """
        # Création d'une chaîne unique avec la date_obj, l'heure_obj, l'ID capteur et magasin
        texte = f"{self.date_obj.isoformat()}-{self.heure_obj.strftime('%H:%M')}"
        texte = texte + f" {self.sensor_id} - {self.store_id}"
        # Hash de la chaîne pour générer une seed unique
        hash_obj = hashlib.sha256(texte.encode())
        seed = int(hash_obj.hexdigest(), 16)
        # Initialisation du générateur aléatoire avec la seed
        random.seed(seed)

        heure = self.heure_obj.hour
        if heure >= 19 or heure < 9:
            # Période creuse (nuit)
            chance = random.random()
            if chance > 0.001:
                return 0
            return random.randint(1, 3)

        # Période active (journée)
        chance2 = random.random()
        if chance2 > 0.08:
            return random.randint(90, 120)

        chance3 = random.random()
        if chance3 < 0.50:
            return None
        if chance3 < 0.75:
            return random.randint(900, 1200)
        return random.randint(1, 2)


if __name__ == "__main__":
    # if len(sys.argv) > 2:
    #     year, month, day = [int(v) for v in sys.argv[1].split("-")]
    #     hour, minute = [int(v) for v in sys.argv[2].split("-")]
    # elif len(sys.argv) > 1:
    #     year, month, day = [int(v) for v in sys.argv[1].split("-")]
    #     hour, minute = 18, 50
    # else:
    #     year, month, day = 2023, 10, 25
    #     hour, minute = 18, 50
    #
    # queried_date = date(year, month, day)
    # queried_time = time(hour, minute)  # Par exemple, 20h00 (nuit)

    # d = date.fromisoformat("2023-04-15")
    # t = time(hour=15, minute=30)
    init_date = date(2023, 4, 12)

    while init_date < date(2023, 4, 30):
        init_date = init_date + timedelta(days=1)
        for hour in range(24):
            year, month, day = 2023, 10, 25
            minute = 0
            queried_date = date(year, month, day)
            queried_time = time(hour, minute)
            sensor = Sensor(
                store_id=1, sensor_id=1, date_obj=init_date, heure_obj=queried_time
            )
            print(
                f"{init_date.isoformat()} - {queried_time.isoformat()} -> {sensor.visiteur_jour_heure()}"
            )
