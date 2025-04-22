import unittest
from datetime import date, time
from src.sensor import Sensor
# import os
# import sys
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
# from sensor import Sensor



class TestSensor(unittest.TestCase):
    def test_sensor_open_time(self):
        d = date.fromisoformat("2023-04-15")
        t = time(hour=15, minute=0)
        sensor = Sensor(1,1, d, t)
        assert 0 != sensor.visiteur_jour_heure()
    def test_sensor_open_close(self):
        d = date.fromisoformat("2023-04-15")
        t = time(hour=20, minute=0)
        sensor = Sensor(1,1, d, t)
        assert 0 == sensor.visiteur_jour_heure()
    def test_sensor_null(self):
        d = date.fromisoformat("2023-04-13")
        t = time(hour=9, minute=0)
        sensor = Sensor(1, 1, d, t)
        assert sensor.visiteur_jour_heure() is None
    def test_sensor_malfunction_inf_norme(self):
        d = date.fromisoformat("2023-04-23")
        t = time(hour=14, minute=0)
        sensor = Sensor(1, 1, d, t)
        assert sensor.visiteur_jour_heure() < 10
    def test_sensor_malfunction_sup_norme(self):
        d = date.fromisoformat("2023-04-22")
        t = time(hour=9, minute=0)
        sensor = Sensor(1, 1, d, t)
        visit = sensor.visiteur_jour_heure()
        self.assertTrue(visit>900)


if __name__ == '__main__':
    unittest.main()