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

        self.thresholds = self.config.get['thresholds']
        self.safety_delay = self.config.get('safety_delay', 15.)

        self.db = None
        if use_mongo:
            self.db = get_mongodb()

        self.messaging = None
        self.safe_dict = None
        self.weather_entries = list()

    def send_message(self, msg, channel='weather'):
        if self.messaging is None:
            self.messaging = PanMessaging.create_publisher(6510)

        self.messaging.send_message(channel, msg)

    def capture(self, data):
        # Make Safety Decision
        self.safe_dict = self.make_safety_decision(data)

        data['Safe'] = self.safe_dict['Safe']
        data['Sky condition'] = self.safe_dict['Sky']
        data['Wind condition'] = self.safe_dict['Wind']
        data['Gust condition'] = self.safe_dict['Gust']
        data['Rain condition'] = self.safe_dict['Rain']

        # Store current weather
        self.weather_entries.append(data)

        if send_message:
            self.send_message({'data': data}, channel='weather')

        if use_mongo:
            self.db.insert_current('weather', data)

        return data

    def make_safety_decision(self, current_values):
        self.logger.debug('Making safety decision')
        self.logger.debug('Found {} weather data entries in last {:.0f} minutes'.format(
            len(self.weather_entries), self.safety_delay))
        safe = False

        # Tuple with condition,safety
        cloud = self._get_cloud_safety(current_values)

        try:
            wind, gust = self._get_wind_safety(current_values)
        except Exception as e:
            self.logger.warning('Problem getting wind safety: {}'.format(e))
            wind = ['N/A']
            gust = ['N/A']

        rain = self._get_rain_safety(current_values)

        safe = cloud[1] & wind[1] & gust[1] & rain[1]
        self.logger.debug('Weather Safe: {}'.format(safe))

        return {'Safe': safe,
                'Sky': cloud[0],
                'Wind': wind[0],
                'Gust': gust[0],
                'Rain': rain[0]}

    def _get_cloud_safety(self, sky_diff, last_cloud):
        safety_delay = self.safety_delay

        # get threshholds for sky-ambient
        amb_thresholds = self.thresholds['sky-ambient']

        threshold_cloudy = amb_thresholds.get('cloudy')
        threshold_very_cloudy = amb_thresholds.get('very_cloudy')

        if len(sky_diff) == 0:
            self.logger.debug('UNSAFE: no sky temperatures found')
            sky_safe = False
            cloud_condition = 'Unknown'
        else:
            if sky_diff > threshold_very_cloudy[0] and sky_diff <= threshold_very_cloudy[1]:
                self.logger.debug('UNSAFE: Very cloudy in last {} min. Max sky diff {:.1f} C'.format(
                                  safety_delay, sky_diff))
                sky_safe = False
            elif sky_diff > threshold_cloudy[0] and sky_diff <= threshold_cloudy[1]:
                self.logger.debug('UNSAFE: Cloudy in last {} min. Max sky diff {:.1f} C'.format(
                                  safety_delay, sky_diff))
                sky_safe = False
            else:
                sky_safe = True

            if last_cloud > threshold_very_cloudy[0] and last_cloud <= threshold_very_cloudy[1]:
                cloud_condition = 'Very Cloudy'
            elif last_cloud > threshold_cloudy[0] and last_cloud <= threshold_cloudy[1]:
                cloud_condition = 'Cloudy'
            else:
                cloud_condition = 'Clear'

        self.logger.debug('Cloud Condition: {} (Sky-Amb={:.1f} C)'.format(cloud_condition, last_cloud))

        return cloud_condition, sky_safe

    def _get_wind_safety(self, wind_speed, wind_gust):
        safety_delay = self.safety_delay

        wind_thresholds = self.thresholds['wind_speed']

        threshold_windy = wind_thresholds.get('windy')
        threshold_very_windy = wind_thresholds.get('very_windy')

        gust_thresholds = self.thresholds['wind_gust']

        threshold_gusty = gust_thresholds.get('gusty')
        threshold_very_gusty = gust_thresholds.get('very_gusty')

        if len(wind_speed) == 0:
            self.logger.debug('UNSAFE: no average wind speed readings found')
            wind_safe = False
            wind_condition = 'Unknown'
        else:
            # Windy?
            if wind_speed > threshold_very_windy[0] and wind_speed <= threshold_very_windy[1]:
                self.logger.debug('UNSAFE:  Very windy in last {:.0f} min. Average wind speed {:.1f} kph'.format(
                                  safety_delay, wind_speed))
                wind_safe = False
            elif wind_speed > threshold_windy[0] and wind_speed <= threshold_windy[1]:
                self.logger.debug('UNSAFE:  Windy in last {:.0f} min. Average wind speed {:.1f} kph'.format(
                                  safety_delay, wind_speed))
                wind_safe = False
            else:
                wind_safe = True

            if wind_speed > threshold_very_windy[0] and wind_speed <= threshold_very_windy[1]:
                wind_condition = 'Very Windy'
            elif wind_speed > threshold_windy[0] and wind_speed <= threshold_windy[1]:
                wind_condition = 'Windy'
            else:
                wind_condition = 'Calm'

            self.logger.debug('Wind Condition: {} ({:.1f} km/h)'.format(wind_condition, wind_speed))

        if len(wind_gust) == 0:
            self.logger.debug('UNSAFE: no maximum wind gust readings found')
            gust_safe = False
            gust_condition = 'Unknown'
        else:
            # Gusty?
            if wind_gust > threshold_very_gusty[0] and wind_gust <= threshold_very_gusty[1]:
                self.logger.debug('UNSAFE:  Very gusty in last {:.0f} min. Max gust speed {:.1f} kph'.format(
                                  safety_delay, wind_gust))
                gust_safe = False
            elif wind_gust > threshold_gusty[0] and wind_gust <= threshold_gusty[1]:
                self.logger.debug('UNSAFE:  Very gusty in last {:.0f} min. Max gust speed {:.1f} kph'.format(
                                  safety_delay, wind_gust))
                gust_safe = False
            else:
                gust_safe = True

            if wind_gust > threshold_very_gusty[0] and wind_gust <= threshold_very_gusty[1]:
                gust_condition = 'Very Gusty'
            elif wind_gust > threshold_gusty[0] and wind_gust <= threshold_gusty[1]:
                gust_condition = 'Gusty'
            else:
                gust_condition = 'Calm'

            self.logger.debug('Gust Condition: {} ({:.1f} km/h)'.format(gust_condition, wind_gust))

        return (wind_condition, wind_safe), (gust_condition, gust_safe)

    def _get_rain_safety(self, rain_flag, wet_flag):
        safety_delay = self.safety_delay

        # not sure how to implement this properly
        for col_name, thresholds in self.thresholds.items():
            current_value = self.current_data[col_name]
            current_status = 'invalid'

            for status, threshold in thresholds.items():
                if len(threshold) == 1:
                    if current_value == threshold:
                        current_status=status
                elif len(threshold) == 2:
                    if current_value > threshold[0] and current_value <= threshold[1]:
                    current_status = status
                else:
                    raise ValueError("Threshold values should be 1 or 2 numbers, got {}!".format(len(threshold))


        self.logger.debug('Rain Condition: {}'.format(rain_condition))

        return rain_condition, rain_safe
