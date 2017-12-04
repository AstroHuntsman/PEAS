#!/usr/bin/env python3

import requests
import logging

import astropy.units as u
from astropy.units import cds
from astropy.table import Table
from astropy.time import Time, TimeISO, TimeDelta

from pocs.utils.messaging import PanMessaging
from . import load_config
from weather_abstract import WeatherAbstract
from weather_abstract import get_mongodb


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
        super.__init__(self, use_mongo=use_mongo)

        # Read configuration
        self.web_data = self.config['weather']['web_service']

        self.logger = logging.getLogger(self.web_data.get('name'))
        self.logger.setLevel(logging.INFO)

        self.max_age = TimeDelta(self.web_data.get('max_age', 60.), format='sec')

        self.table_data = None


    def capture(self, use_mongo=False, send_message=False, **kwargs):
        self.logger.debug('Updating weather data')

        data = {}

        data['weather_data_from'] = self.web_data.get('name')
        self.table_data = self.fetch_met_data()
        col_names = self.web_data.get('column_names')
        for name in col_names:
            data[name] = self.table_data[name][0]

        return super.capture(data)

    def fetch_met_data(self):
        try:
            cache_age = Time.now() - self._met_data['Time (UTC)'][0] * 86400
        except AttributeError:
            cache_age = 1.382e10 * u.year

        if cache_age > self.max_age:
            # Download met data file
            m = requests.get(self.web_data.get('link'))
            # Remove the annoying " and newline between the date and time
            met = m.text.replace('."\n',' ')
            # Remove the " before the date
            met = met.replace('" ', '')

            # Parse the tab delimited met data into a Table
            t = Table.read(met, format='ascii.no_header', delimiter='\t',
                                names=self.web_data.get('column_names'))

            # Convert time strings to Time
            t['Time (UTC)'] = Time(t['Time (UTC)'], format='mixed_up_time')
            # Change string format to ISO
            t['Time (UTC)'].format = 'iso'
            # Convert from AAT standard time to UTC
            t['Time (UTC)'] = t['Time (UTC)'] - 10 * u.hour

            col_names = self.web_data.get('column_names')
            col_units = self.web_data.get('column_units')

            if len(col_names) != len(col_units):
                self.logger.debug('Number of columns does not match number of units given')

            # Set units for items that have them
            for name, unit in zip(col_names, col_units):
                t[name].unit = unit

            self._met_data = t

        return self._met_data

    def _get_cloud_safety(self, current_values):
        sky_diff = self.weather_entries['sky-ambient']
        sky_diff_u = self.weather_entries['sky-ambient uncertainty']

        max_sky_diff = sky_diff + sky_diff_u
        last_cloud = sky_diff

        return super._get_cloud_safety(max_sky_diff, last_cloud)

    def _get_wind_safety(self, current_values):
        wind_speed = self.weather_entries['Average wind speed']
        wind_gust = self.weather_entries['Maximum wind gust']

        return super._get_wind_safety(wind_speed, wind_gust)

    def _get_rain_safety(self, current_values):
        rain_sensor = self.weather_entries['Rain sensor']
        rain_flag = self.weather_entries['Boltwood rain flag']
        wet_flag = self.weather_entries['Boltwood wet flag']

        if rain_sensor > 0 and rain_flag > 0:
            rain_flag = 1
        elif rain_sensor < 0 or rain_flag < 0:
            rain_flag = -1
        else:
            rain_flag = 0

        return super._get_rain_safety(rain_flag, wet_flag)
