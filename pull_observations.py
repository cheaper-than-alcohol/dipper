import collections
import datetime
import json
import math
import random
import logging
from concurrent.futures import wait

import requests
from psycopg2 import IntegrityError
from retryz import retry
from scheduled_futures import ScheduledThreadPoolExecutor

import db

MapPoint = collections.namedtuple("MapPoint", ("id", "latitude", "longitude"))
settings_filepath = './settings.json'


class Weather:
    observation_url = "https://api.weather.gov/stations/KDLZ/observations/latest"
    gather_stations_url = "https://api.weather.gov/stations?state=OH"
    headers = {
        'Accept': 'application/id+json',
        'User-Agent': '(darylmathison.com, daryl.mathison@gmail.com)'
    }

    def __init__(self):
        self.next_reading = None
        self.last_reading = None

    def _time_to_wait(self, tries=3):
        _wait = random.randrange(5, 30, 1)
        logging.info("On Retry %s waiting another %s seconds", tries, _wait)
        return _wait

    def _check_for_bad_observation(self, observation):
        if observation.pressure == -1:
            logging.info("Weather observation failed")
            return True
        else:
            return False

    def _check_error(self, error):
        if isinstance(error, requests.HTTPError):
            if error.response.status_code >= 500:
                logging.warning("Server error, will try again")
                return True
            elif 400 < error.response.status_code > 500:
                logging.error("Bad request.  Please check the request")
                return False
            else:
                logging.error("Error found contacting", self.observation_url, repr(error))
        else:
            logging.error("Unknown error:", repr(error))
        return False

    @retry(on_return=_check_for_bad_observation, on_error=_check_error, wait=_time_to_wait)
    def make_observation(self):
        logging.info("Attempting observation at %s", datetime.datetime.now())
        response = requests.get(self.observation_url, headers=self.headers)
        response.raise_for_status()

        self.next_reading = response.headers["Expires"]

        weather_data = response.json()["properties"]
        self.last_reading = db.Observation(
            weather_data["@id"],
            weather_data["timestamp"],
            int(weather_data["temperature"]["value"] or -999),
            int(weather_data["barometricPressure"]["value"] or -1),
            float(weather_data["relativeHumidity"]["value"] or -1),
            int(float(weather_data["windSpeed"]["value"] or -999)),
            int(float(weather_data["windDirection"]["value"] or -999)),
            int(weather_data["dewpoint"]["value"] or -999)
        )
        return self.last_reading

    def find_closest_station(self, given_point=MapPoint('home', 40.2392361, -83.0418715)):
        def distance(start, end):
            return math.sqrt(math.pow(end.longitude - start.longitude, 2) + math.pow(end.latitude - start.latitude, 2))

        def sort_points(point):
            return distance(given_point, point)

        logging.info("Requesting stations")
        response = requests.get(self.gather_stations_url, headers=self.headers)
        logging.debug("got stations")
        stations = [MapPoint(station["properties"]["stationIdentifier"],
                             station["geometry"]["coordinates"][1],
                             station["geometry"]["coordinates"][0])
                    for station in response.json()["features"]]
        logging.debug("got station data")
        return sorted(stations, key=sort_points)[:5]


def work(settings):
    weather = Weather()
    with db.db_connection() as conn:
        logging.info("in the connection")
        data_to_keep = weather.make_observation()
        logging.info("Successful data retrieval %s", data_to_keep)
        try:
            db.add_observation(conn, data_to_keep)
        except IntegrityError as de:
            logging.info("Warning: %s", repr(de))


def main():
    logging.basicConfig(filename=f'log/observation.log', level=logging.DEBUG)
    with open(settings_filepath) as settings_file:
        settings = json.load(settings_file)

    work(settings)
    # with ScheduledThreadPoolExecutor() as stpool:
    #     future = stpool.schedule(work, (settings,), period=settings["sample_rate"])
    #     wait([future])
    # weather = Weather()
    # logging.info(weather.find_closest_station())


if __name__ == '__main__':
    main()
