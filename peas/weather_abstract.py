
import logging
from pocs.utils.messaging import PanMessaging

def get_mongodb():
    from pocs.utils.database import PanMongo
    return PanMongo()

# -----------------------------------------------------------------------------
#   Base Weather Abstract Class
# -----------------------------------------------------------------------------
class WeatherAbstract(object):
    """Base class for checking generic weather data.

    Parses the data through customizable parameters and assigns the corresponding
    entry to its condition (i.e. raining or not raining), along with its safety
    conditions. Conditions are stored in mongodb and sent to POCS.

    Attributes:
        self.safety_delay:
        self.db:
        self.messaging:
        self.weather_entries:
    """

    def __init__(self, use_mongo=True):
        self.safety_delay = self.config.get('safety_delay', 15.)

        self.db = None
        if use_mongo:
            self.db = get_mongodb()

        self.messaging = None
        self.weather_entries = {}

    def send_message(self, msg, channel='weather'):
        # Sends weather data to POCS
        if self.messaging is None:
            self.messageing = PanMessaging.create_publisher(6510)

        self.messaging.send_message(channel, msg)

    def capture(self, current_values, use_mongo=False, send_message=False, **kwargs):
        """Gets result from safety conditions and stores the current data

        Args:
            current_values:
            use_mongo:
            send_message:

        Returns:

        """
        self.weather_entries = current_values
        current_weather = self.make_safety_decision()

        if send_message:
            self.send_message({'Data': current_weather}, channel='weather')

        if use_mongo:
            self.db.insert_current('Weather', current_weather)

        return current_weather

    def make_safety_decision(self):
        """Decides if the weather is safe

        Returns:
            Dictionary of the weather data plus the safety condition.
        """
        self.logger.debug('Making safety decision')
        self.logger.debug('Found {} weather data entries in last {:.0f} minutes'.format(
                          len(self.weather_entries), self.safety_delay))

        current_status = self._get_status()

        results = self.weather_entries
        safe = True

        for category, method in self._safety_methods.items():
            result = method(current_status)
            results[category] = result[0]
            safe = safe and result[1]

            results['Safe'] = safe

        return results

    def _get_cloud_safety(self, statuses):
        """Gets the sky safety and weather conditions

        Args:
            statuses: The status of the weather data.

        Returns:
            The cloud condition and the cloud safety. For example:

            'Very cloudy', False
        """
        safety_delay = self.safety_delay

        cloud_condition = statuses['Sky-ambient']

        if cloud_condition == 'Very cloudy':
            self.logger.debug('UNSAFE:  Very cloudy in last {:.0f} min.'.format(safety_delay))
            cloud_safe = False
        elif cloud_condition == 'Cloudy':
            self.logger.debug('UNSAFE:  Cloudy in last {:.0f} min.'.format(safety_delay))
            cloud_safe = False
        elif cloud_condition == 'Invalid':
            self.logger.debug('UNSAFE:  Cloud condition is invalid.')
            cloud_safe = False
        elif cloud_condition == 'Clear':
            cloud_safe = True
        else:
            self.logger.debug('UNSAFE:  Cloud condition is unknown.')
            cloud_condition = 'Unknown'
            cloud_safe = False

        self.logger.debug('Cloud Condition: {} (Sky-ambient is {})'.format(
                          cloud_condition, self.weather_entries['Sky-ambient']))

        return cloud_condition, cloud_safe

    def _get_wind_safety(self, statuses):
        """Gets the wind safety and weather conditions

        Args:
            statuses: The status of the weather data.

        Returns:
            The wind condition and the wind safety. For example:

                'Calm', True
        """
        safety_delay = self.safety_delay

        wind_condition = statuses['Wind speed']

        if wind_condition == 'Very windy':
            self.logger.debug('UNSAFE:  Very windy in last {:.0f} min.'.format(safety_delay))
            wind_safe = False
        elif wind_condition == 'Windy':
            self.logger.debug('UNSAFE:  Windy in last {:.0f} min.'.format(safety_delay))
            wind_safe = False
        elif wind_condition == 'Invalid':
            self.logger.debug('UNSAFE:  Wind condition is invalid')
            wind_safe = False
        elif wind_condition == 'Calm':
            wind_safe = True
        else:
            self.logger.debug('UNSAFE:  Wind condition is unknown.')
            wind_condition = 'Unknown'
            wind_safe = False

        self.logger.debug('Wind Condition: {} (Wind speed is {})'.format(
                          wind_condition,  self.weather_entries['Wind speed']))

        return wind_condition, wind_safe

    def _get_gust_safety(self, statuses):
        """Gets the gust safety and weather conditions

        Args:
            statuses: The status of the weather data.

        Returns:
            The gust conditiona and the gust safety. For example:

                'Very gusty', False
        """
        safety_delay = self.safety_delay

        gust_condition = statuses['Wind gust']

        if gust_condition == 'Very gusty':
            self.logger.debug('UNSAFE:  Very gusty in last {:.0f} min.'.format(safety_delay))
            gust_safe = False
        elif gust_condition == 'Gusty':
            self.logger.debug('UNSAFE:  Gusty in last {:.0f} min.'.format(safety_delay))
            gust_safe = False
        elif gust_condition == 'Invalid':
            self.logger.debug('UNSAFE:  Gust condition is invalid.')
            gust_safe = False
        elif gust_condition == 'Calm':
            gust_safe = True
        else:
            self.logger.debug('UNSAFE:  Gust condition is unknown.')
            gust_condition = 'Unknown'
            gust_safe = False

        self.logger.debug('Gust condition: {} (Gust speed is {})'.format(
                          gust_condition,  self.weather_entries['Wind gust']))

        return gust_condition, gust_safe

    def _get_rain_safety(self, statuses):
        """
        Get the rain safety and weather condition.

        Args:
            statuses: The status of the weather data.

        NOTE:
            This only needs to be implemented when the rain data shares the same thresholds,
            e.g. having two AAG cloud sensors or two met data files.
        """
        raise NotImplementedError

    def _get_status(self):
        """Gets the status of the entries that have been given thresholds.

        Returns:
            A dictionary of the current statuses.For example:

                {'Sky-ambient': 'Clear', 'Wind speed': 'Very windy', ... , etc. }
        """
        current_statuses = {}

        for col_name, thresholds in self.thresholds.items():
            # gets the value of the specfic "col_name"
            current_value = self.weather_entries[col_name]
            current_statuses[col_name] = 'Invalid'

            for status, threshold in thresholds.items():
                if len(threshold) == 1:
                    if current_value == threshold:
                        current_statuses[col_name] = status
                elif len(threshold) == 2:
                    if current_value > threshold[0] and current_value <= threshold[1]:
                        current_statuses[col_name] = status
                else:
                    raise ValueError("Threshold values should be 1 or 2 numbers, got {} for {}!".format(len(threshold), col_name))

        return current_statuses
