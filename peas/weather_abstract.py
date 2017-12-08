#!/usr/bin/env python3

import logging
import numpy as np
import re
import serial
import sys
import time
import requests

from datetime import datetime as dt
from dateutil.parser import parse as date_parser

import astropy.units as u
from astropy.units import cds
from astropy.table import Table
from astropy.time import Time, TimeISO, TimeDelta

from pocs.utils.messaging import PanMessaging

from . import load_config
from .PID import PID

def get_mongodb():
    from pocs.utils.database import PanMongo
    return PanMongo()

# -----------------------------------------------------------------------------
#   Base class to read and check weather data
# -----------------------------------------------------------------------------
class WeatherAbstract(object):

    """ Base class for checking generic weather data and sending it to the
        required location.
    """

    def __init__(self, use_mongo=True):
        self.config = load_config()

        self.thresholds = self.config.get('thresholds')
        self.safety_delay = self.config.get('safety_delay', 15.)

        self.db = None
        if use_mongo:
            self.db = get_mongodb()

        self.messaging = None
        self.weather_entries = list()

    def send_message(self, msg, channel='weather'):
        if self.messaging is None:
            self.messaging = PanMessaging.create_publisher(6510)

        self.messaging.send_message(channel, msg)

    def capture(self, current_values):
        # Make Safety Decision and store current weather
        current_weather = self.make_safety_decision(current_values)
        self.weather_entries.append(current_weather)

        if send_message:
            self.send_message({'data': current_weather}, channel='weather')

        if use_mongo:
            self.db.insert_current('weather', current_weather)

        return current_weather

    def make_safety_decision(self, current_values):

        self.logger.debug('Making safety decision')
        self.logger.debug('Found {} weather data entries in last {:.0f} minutes'.format(
                          len(self.weather_entries), self.safety_delay))

        current_status = self._get_status(current_values)

        results = {}
        safe = True

        for category, method in self._safety_methods.items():
            result = method(current_status)
            results[category] = result[0]
            safe = safe and result[1]

            results['Safe'] = safe

        return results

    def _get_cloud_safety(self, statuses):
        safety_delay = self.safety_delay

        cloud_condition = statuses['sky-ambient']

        if cloud_condition == 'very_cloudy':
            self.logger.debug('UNSAFE:  Very cloudy in last {:.0f} min.'.format(safety_delay))
            cloud_safe = False
        elif cloud_condition == 'cloudy':
            self.logger.debug('UNSAFE:  Cloudy in last {:.0f} min.'.format(safety_delay))
            cloud_safe = False
        elif cloud_condition == 'invalid':
            self.logger.debug('UNSAFE:  Cloud condition is invalid.')
            cloud_safe = False
        elif cloud_condition == 'clear':
            cloud_safe = True
        else:
            self.logger.debug('UNSAFE:  Cloud condition is unknown.')
            cloud_condition = 'Unknown'
            cloud_safe = False

        self.logger.debug('Cloud Condition: {} '.format(cloud_condition))

        return cloud_condition, cloud_safe

    def _get_wind_safety(self, statuses):
        safety_delay = self.safety_delay

        wind_condition = statuses['wind_speed']

        if wind_condition == 'very_windy':
            self.logger.debug('UNSAFE:  Very windy in last {:.0f} min.'.format(safety_delay))
            wind_safe = False
        elif wind_condition == 'windy':
            self.logger.debug('UNSAFE:  Windy in last {:.0f} min.'.format(safety_delay))
            wind_safe = False
        elif wind_condition == 'invalid':
            self.logger.debug('UNSAFE:  Wind condition is invalid')
            wind_safe = False
        elif wind_condition == 'calm':
            wind_safe = True
        else:
            self.logger.debug('UNSAFE:  Wind condition is unknown.')
            wind_condition = 'unknown'
            wind_safe = False

        self.logger.debug('Wind Condition: {} '.format(wind_condition))

        return wind_condition, wind_safe

    def _get_gust_safety(self, statuses):
        safety_delay = self.safety_delay

        gust_condition = statuses['wind_gust']

        if gust_condition == 'very_gusty':
            self.logger.debug('UNSAFE:  Very gusty in last {:.0f} min.'.format(safety_delay))
            gust_safe = False
        elif gust_condition == 'gusty':
            self.logger.debug('UNSAFE:  Gusty in last {:.0f} min.'.format(safety_delay))
            gust_safe = False
        elif gust_condition == 'invalid':
            self.logger.debug('UNSAFE:  Gust condition is invalid.')
            gust_safe = False
        elif gust_condition == 'calm':
            gust_safe = True
        else:
            self.logger.debug('UNSAFE:  Gust condition is unknown.')
            gust_condition = 'unknown'
            gust_safe = False

        self.logger.debug('Gust condition: {} '.format(gust_condition))

        return gust_condition, gust_safe

    def _get_rain_safety(self, statuses):
        """
        Get the rain safety and weather condition.

        Note: this only needs to be implemented when the rain data shares the same thresholds,
        e.g. having two AAG cloud sensors or two met data files.
        """
        raise NotImplementedError

    def _get_status(self):
        current_statuses = {}

        for col_name, thresholds in self.thresholds.items():
            # gets the value of the specfic "col_name"
            current_value = self.weather_entries[col_name]
            current_statuses[col_name] = 'invalid'

            for status, threshold in thresholds.items():
                if len(threshold) == 1:
                    if current_value == threshold:
                        current_statuses[col_name] = status
                elif len(threshold) == 2:
                    if current_value > threshold[0] and current_value <= threshold[1]:
                        current_statuses[col_name] = status
                else:
                    raise ValueError("Threshold values should be 1 or 2 numbers, got {}!".format(len(threshold)))

        return current_statuses
