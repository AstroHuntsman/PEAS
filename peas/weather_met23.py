#!/usr/bin/env python3

import logging
import re
import requests
import xmltodict

import astropy.units as u
from astropy.time import Time, TimeDelta

from datetime import datetime as dt

from . import load_config
from .weather_abstract import WeatherDataAbstract
from .weather_abstract import get_mongodb

class Met23Weather(WeatherDataAbstract):
    """ Gets the weather information from the 2.3 m telescope """

    def __init__(self, use_mongo=True):
        # Read configuration
        self.config = load_config()
        self.met23_cfg = self.config['weather']['met23']
        self.thresholds = self.met23_cfg['thresholds']

        super().__init__(use_mongo=use_mongo)

        self.logger = logging.getLogger(name=self.met23_cfg.get('name'))
        self.logger.setLevel(logging.INFO)

        self.max_age = TimeDelta(self.met23_cfg.get('max_age', 60.), format='sec')

        self._safety_methods = {'rain_condition':self._get_rain_safety,
                                'wind_condition':self._get_wind_safety,
                                'gust_condition':self._get_gust_safety}

        self.table_data = None

    def capture(self, use_mongo=False, send_message=False, **kwargs):
        """ Update weather data. """
        self.logger.debug('Updating weather data')

        data = {}

        data['weather_data_name'] = self.met23_cfg.get('name')
        data['date'] = Time.now()
        self.table_data = self.fetch_met23_data()
        col_names = self.met23_cfg.get('column_names')
        for name in col_names:
            data[name] = self.table_data[name]

        self.weather_entries = data

        return super().capture(use_mongo=False, send_message=False, **kwargs)

    def fetch_met23_data(self):
        try:
            cache_age = Time.now() - self.weather_entries['date']
        except KeyError:
            cache_age = 61. * u.second

        if cache_age > self.max_age:
            met23_link = self.met23_cfg.get('link')
            response = requests.get(met23_link)


            with open('met23.xml', 'wb') as file:
                file.write(response.content)

            file = open('met23.xml', 'r')

            with open('met23.xml') as fd:
                    doc = xmltodict.parse(fd.read())

            met_23_data = {}

            met_23_data['wind_speed'] = float(doc['metsys']['data']['ws']['val']) # m / s
            met_23_data['wind_gust'] = float(doc['metsys']['data']['wgust']['val']) # m / s
            met_23_data['rain_sensor'] = doc['metsys']['data']['rsens']['val']

        return(met_23_data)

    def _get_rain_safety(self, statuses):
        """Gets the rain safety and weather conditions

        Args:
            statuses: The status of the weather data.

        Returns:
            The rain condition and the rain safety. For example:

                'No data', False
        """

        rain_condition = statuses['rain_sensor']

        if rain_condition == 'No rain':
            rain_safe = True
        elif rain_condition == 'Rain':
            rain_safe = False
        elif rain_condition == 'Invalid':
            rain_safe = False
        else:
            rain_condition = 'Unknown'
            rain_safe = False

        self.logger.debug('Rain Condition: {} '.format(rain_condition))

        return rain_condition, rain_safe
