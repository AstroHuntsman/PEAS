#!/usr/bin/env python3

import logging
import re
import requests
import xmltodict

import astropy.units as u
from astropy.time import TimeDelta

from datetime import datetime as dt

from . import load_config
from .weather_abstract import WeatherDataAbstract
from .weather_abstract import get_mongodb

class SkyMapWeather(WeatherDataAbstract):
    """ Gets the weather data from the SkyMapper telescope """

    def __init__(self, use_mongo=True):
        # Read configuration
        self.config = load_config()
        self.skymap_cfg = self.config['weather']['skymap']
        self.thresholds = self.skymap_cfg['thresholds']

        super().__init__(use_mongo=use_mongo)

        self.logger = logging.getLogger(name=self.skymap_cfg.get('name'))
        self.logger.setLevel(logging.INFO)

        self.max_age = TimeDelta(self.skymap_cfg.get('max_age', 60.), format='sec')

        self._safety_methods = {'rain_condition':self._get_rain_safety,
                                'wind_condition':self._get_wind_safety,
                                'gust_condition':self._get_gust_safety,
                                'sky_condition':self._get_cloud_safety}

        self.table_data = None

    def fetch_skymap_data(self):
        URL = "http://www.mso.anu.edu.au/metdata/skymap.xml"

        response = requests.get("http://www.mso.anu.edu.au/metdata/skymap.xml")

        with open('skymap.xml') as fd:
            doc = xmltodict.parse(fd.read())

        data = {}

        return(data)
