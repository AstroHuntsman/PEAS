#!/usr/bin/env python3

import requests
import logging

import astropy.units as u
from astropy.units import cds
from astropy.table import Table
from astropy.time import Time, TimeISO, TimeDelta

from pocs.utils.messaging import PanMessaging
from . import load_config
from .weather_abstract import WeatherAbstract
from .weather_abstract import get_mongodb


class MixedUpTime(TimeISO):

    """
    Subclass the astropy.time.TimeISO time format to handle the mixed up
    American style time format that the AAT met system uses.
    """

    name= 'mixed_up_time'
    subfmts = (('date_hms',
                '%m-%d-%Y %H:%M:%S',
                '{mon:02d}-{day:02d}-{year:d} {hour:02d}:{min:02d}:{sec:02d}'),
               ('date_hm',
                '%m-%d-%Y %H:%M',
                '{mon:02d}-{day:02d}-{year:d} {hour:02d}:{min:02d}'),
               ('date',
                '%m-%d-%Y',
                '{mon:02d}-{day:02d}-{year:d}'))

# -----------------------------------------------------------------------------
# External weather data class
# -----------------------------------------------------------------------------
class WeatherData(WeatherAbstract):

    """ Gets AAT weather data from  http://site.aao.gov.au/AATdatabase/met.html

    Turns the weather data into a useable and meaningful table whose columns are the
    entries of the data. The table only features one row (Excluding the title row)
    which has the numerical data of all the entries.

    The data is then compared with specified parameters and if the data is not within
    the parameters then the safety condition of that entry is defined as False, i.e.
    not safe.
    Data is also given the weather condition, for example if the wind value is greater
    that the one specified the weather condition will be 'windy'.

    Once all the required data has been defined and given conditions, it will then
    decide if the system is safe. If one value is False then the safety condition of
    the system is False. All entries must be True so that the safety condition can
    return True.

    The final conditions and values are then sent to the dome controller to either
    leave, open or close the dome. They are also saved in a database so that previous
    entries can be retrived.
    """

    def __init__(self, use_mongo=True):
        super().__init__(use_mongo=use_mongo)

        # Read configuration
        self.web_config = self.config['weather']['web_service']

        self.logger = logging.getLogger(self.web_config.get('name'))
        self.logger.setLevel(logging.INFO)

        self.max_age = TimeDelta(self.web_config.get('max_age', 60.), format='sec')

        self._safety_methods = {'Rain condition':self._get_rain_safety,
                                'Wetness condition':self._get_wetness_safety,
                                'Wind condition':self._get_wind_safety,
                                'Gust condition':self._get_gust_safety,
                                'Cloud condition':self._get_cloud_safety}

        self.table_data = None

    def capture(self, use_mongo=False, send_message=False, **kwargs):
        self.logger.debug('Updating weather data')

        data = {}

        data['weather_data_from'] = self.web_config.get('name')
        self.table_data = self.fetch_met_data()
        col_names = self.web_config.get('column_names')
        for name in col_names:
            data[name] = self.table_data[name][0]

        return super().capture(data)

    def fetch_met_data(self):
        try:
            cache_age = Time.now() - self._met_data['Time (UTC)'][0] * 86400
        except AttributeError:
            cache_age = 1.382e10 * u.year

        if cache_age > self.max_age:
            # Download met data file
            m = requests.get(self.web_config.get('link'))
            # Remove the annoying " and newline between the date and time
            met = m.text.replace('."\n',' ')
            # Remove the " before the date
            met = met.replace('" ', '')

            # Parse the tab delimited met data into a Table
            t = Table.read(met, format='ascii.no_header', delimiter='\t',
                                names=self.web_config.get('column_names'))

            # Convert time strings to Time
            t['Time (UTC)'] = Time(t['Time (UTC)'], format='mixed_up_time')
            # Change string format to ISO
            t['Time (UTC)'].format = 'iso'
            # Convert from AAT standard time to UTC
            t['Time (UTC)'] = t['Time (UTC)'] - 10 * u.hour

            col_names = self.web_config.get('column_names')
            col_units = self.web_config.get('column_units')

            if len(col_names) != len(col_units):
                self.logger.debug('Number of columns does not match number of units given')

            # Set units for items that have them
            for name, unit in zip(col_names, col_units):
                t[name].unit = unit

            self._met_data = t

        return self._met_data

    def _get_rain_safety(self, statuses):
        safety_delay = self.safety_delay

        rain_sensor = statuses['rain_sensor']
        rain_flag = statuses['boltwood_rain_flag']

        if rain_sensor == 'no_data' or rain_flag == 'no_data':
            self.logger.debug('UNSAFE:  no rain data found')
            rain_condition = 'No Data'
            rain_safe = False
        elif rain_sensor == 'rain' or rain_flag == 'rain':
            self.logger.debug('UNSAFE:  Rain in last {} min.'.format(safety_delay))
            rain_condition = 'Rain'
            rain_safe = False
        elif rain_sensor == 'no_rain' or rain_flag == 'no_rain':
            rain_condition = 'No Rain'
            rain_safe = True
        elif rain_sensor == 'invalid' or rain_flag == 'invalid':
            self.logger.debug('UNSAFE:  rain data is invalid')
            rain_condition = 'Invalid'
            rain_safe = False
        else:
            self.logger.debug('UNSAFE:  unknown rain data')
            rain_condition = 'unknown'
            rain_safe = False

        self.logger.debug('Rain Condition: {} '.format(rain_condition))

        return rain_condition, rain_safe

    def _get_wetness_safety(self, statuses):
        safety_delay = self.safety_delay

        wetness_condition = statuses['boltwood_wet_flag']

        if wetness_condition == 'no_data':
            self.logger.debug('UNSAFE:  no wetness data found')
            wetness_safe = False
        elif wetness_condition == 'wet':
            self.logger.debug('UNSAFE:  Wet in last {} min.'.format(safety_delay))
            wetness_safe = False
        elif wetness_condition == 'invalid':
            self.logger.debug('UNSAFE:  wetness data is invalid')
            wetness_safe = False
        elif wetness_condition == 'dry':
            wetness_safe = True
        else:
            self.logger.debug('UNSAFE:  wetness data is unknown')
            wetness_condition = 'unknown'
            wetness_safe = False

        self.logger.debug('Wetness Condition: {} '.format(wetness_condition))

        return wetness_condition, wetness_safe
