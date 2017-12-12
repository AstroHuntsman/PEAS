#!/usr/bin/env python3

import logging
import numpy as np
import re
import serial
import sys
import time

from datetime import datetime as dt
from dateutil.parser import parse as date_parser

import astropy.units as u

from . import load_config
from .PID import PID
from .weather_abstract import WeatherAbstract
from .weather_abstract import get_mongodb

def movingaverage(interval, window_size):
    """ A simple moving average function """
    window = np.ones(int(window_size)) / float(window_size)
    return np.convolve(interval, window, 'same')


# -----------------------------------------------------------------------------
# AAG Cloud Sensor Class
# -----------------------------------------------------------------------------
class AAGCloudSensor(WeatherAbstract):

    """
    This class is for the AAG Cloud Sensor device which can be communicated with
    via serial commands.

    http://www.aagware.eu/aag/cloudwatcherNetwork/TechInfo/Rs232_Comms_v100.pdf
    http://www.aagware.eu/aag/cloudwatcherNetwork/TechInfo/Rs232_Comms_v110.pdf
    http://www.aagware.eu/aag/cloudwatcherNetwork/TechInfo/Rs232_Comms_v120.pdf

    Command List (from Rs232_Comms_v100.pdf)
    !A = Get internal name (recieves 2 blocks)
    !B = Get firmware version (recieves 2 blocks)
    !C = Get values (recieves 5 blocks)
         Zener voltage, Ambient Temperature, Ambient Temperature, Rain Sensor Temperature, HSB
    !D = Get internal errors (recieves 5 blocks)
    !E = Get rain frequency (recieves 2 blocks)
    !F = Get switch status (recieves 2 blocks)
    !G = Set switch open (recieves 2 blocks)
    !H = Set switch closed (recieves 2 blocks)
    !Pxxxx = Set PWM value to xxxx (recieves 2 blocks)
    !Q = Get PWM value (recieves 2 blocks)
    !S = Get sky IR temperature (recieves 2 blocks)
    !T = Get sensor temperature (recieves 2 blocks)
    !z = Reset RS232 buffer pointers (recieves 1 blocks)
    !K = Get serial number (recieves 2 blocks)

    Return Codes
    '1 '    Infra red temperature in hundredth of degree Celsius
    '2 '    Infra red sensor temperature in hundredth of degree Celsius
    '3 '    Analog0 output 0-1023 => 0 to full voltage (Ambient Temp NTC)
    '4 '    Analog2 output 0-1023 => 0 to full voltage (LDR ambient light)
    '5 '    Analog3 output 0-1023 => 0 to full voltage (Rain Sensor Temp NTC)
    '6 '    Analog3 output 0-1023 => 0 to full voltage (Zener Voltage reference)
    'E1'    Number of internal errors reading infra red sensor: 1st address byte
    'E2'    Number of internal errors reading infra red sensor: command byte
    'E3'    Number of internal errors reading infra red sensor: 2nd address byte
    'E4'    Number of internal errors reading infra red sensor: PEC byte NB: the error
            counters are reset after being read.
    'N '    Internal Name
    'V '    Firmware Version number
    'Q '    PWM duty cycle
    'R '    Rain frequency counter
    'X '    Switch Opened
    'Y '    Switch Closed

    Advice from the manual:

    * When communicating with the device send one command at a time and wait for
    the respective reply, checking that the correct number of characters has
    been received.

    * Perform more than one single reading (say, 5) and apply a statistical
    analysis to the values to exclude any outlier.

    * The rain frequency measurement is the one that takes more time - 280 ms

    * The following reading cycle takes just less than 3 seconds to perform:
        * Perform 5 times:
            * get IR temperature
            * get Ambient temperature
            * get Values
            * get Rain Frequency
        * get PWM value
        * get IR errors
        * get SWITCH Status

    """

    def __init__(self, serial_address=None, use_mongo=True):
        # Read configuration
        self.config = load_config()
        self.sensor_data = self.config['weather']['aag_cloud']
        self.thresholds = self.sensor_data['thresholds']

        super().__init__(use_mongo=use_mongo)

        self.logger = logging.getLogger(self.sensor_data.get('name'))
        self.logger.setLevel(logging.INFO)

        self._safety_methods = {'Rain condition':self._get_rain_safety,
                                'Wind condition':self._get_wind_safety,
                                'Gust condition':self._get_gust_safety,
                                'Sky condition':self._get_cloud_safety}

        # Initialize Serial Connection
        if serial_address is None:
            serial_address = self.sensor_data.get('serial_port', '/dev/ttyUSB0')

        self.logger.debug('Using serial address: {}'.format(serial_address))

        if serial_address:
            self.logger.info('Connecting to AAG Cloud Sensor')
            try:
                self.AAG = serial.Serial(serial_address, 9600, timeout=2)
                self.logger.info("  Connected to Cloud Sensor on {}".format(serial_address))
            except OSError as e:
                self.logger.error('Unable to connect to AAG Cloud Sensor')
                self.logger.error('  {}'.format(e.errno))
                self.logger.error('  {}'.format(e.strerror))
                self.AAG = None
            except:
                self.logger.error("Unable to connect to AAG Cloud Sensor")
                self.AAG = None
        else:
            self.AAG = None

        # Thresholds

        # Initialize Values
        self.last_update = None
        self.safe = None
        self.ambient_temp = None
        self.sky_temp = None
        self.wind_speed = None
        self.internal_voltage = None
        self.LDR_resistance = None
        self.rain_sensor_temp = None
        self.PWM = None
        self.errors = None
        self.switch = None
        self.hibernate = 0.500  # time to wait after failed query

        # Set Up Heater
        if 'heater' in self.sensor_data:
            self.heater_sensor_data = self.sensor_data['heater']
        else:
            self.heater_sensor_data = {
                'low_temp': 0,
                'low_delta': 6,
                'high_temp': 20,
                'high_delta': 4,
                'min_power': 10,
                'impulse_temp': 10,
                'impulse_duration': 60,
                'impulse_cycle': 600,
            }
        self.heater_PID = PID(Kp=3.0, Ki=0.02, Kd=200.0,
                              max_age=300,
                              output_limits=[self.heater_sensor_data['min_power'], 100])

        self.impulse_heating = None
        self.impulse_start = None

        # Command Translation
        self.commands = {'!A': 'Get internal name',
                         '!B': 'Get firmware version',
                         '!C': 'Get values',
                         '!D': 'Get internal errors',
                         '!E': 'Get rain frequency',
                         '!F': 'Get switch status',
                         '!G': 'Set switch open',
                         '!H': 'Set switch closed',
                         'P\d\d\d\d!': 'Set PWM value',
                         '!Q': 'Get PWM value',
                         '!S': 'Get sky IR temperature',
                         '!T': 'Get sensor temperature',
                         '!z': 'Reset RS232 buffer pointers',
                         '!K': 'Get serial number',
                         'v!': 'Query if anemometer enabled',
                         'V!': 'Get wind speed',
                         'M!': 'Get electrical constants',
                         '!Pxxxx': 'Set PWM value to xxxx',
                         }
        self.expects = {'!A': '!N\s+(\w+)!',
                        '!B': '!V\s+([\d\.\-]+)!',
                        '!C': '!6\s+([\d\.\-]+)!4\s+([\d\.\-]+)!5\s+([\d\.\-]+)!',
                        '!D': '!E1\s+([\d\.]+)!E2\s+([\d\.]+)!E3\s+([\d\.]+)!E4\s+([\d\.]+)!',
                        '!E': '!R\s+([\d\.\-]+)!',
                        '!F': '!Y\s+([\d\.\-]+)!',
                        'P\d\d\d\d!': '!Q\s+([\d\.\-]+)!',
                        '!Q': '!Q\s+([\d\.\-]+)!',
                        '!S': '!1\s+([\d\.\-]+)!',
                        '!T': '!2\s+([\d\.\-]+)!',
                        '!K': '!K(\d+)\s*\\x00!',
                        'v!': '!v\s+([\d\.\-]+)!',
                        'V!': '!w\s+([\d\.\-]+)!',
                        'M!': '!M(.{12})',
                        }
        self.delays = {
            '!E': 0.350,
            'P\d\d\d\d!': 0.750,
        }

        if self.AAG:
            # Query Device Name
            result = self.query('!A')
            if result:
                self.name = result[0].strip()
                self.logger.info('  Device Name is "{}"'.format(self.name))
            else:
                self.name = ''
                self.logger.warning('  Failed to get Device Name')
                sys.exit(1)

            # Query Firmware Version
            result = self.query('!B')
            if result:
                self.firmware_version = result[0].strip()
                self.logger.info('  Firmware Version = {}'.format(self.firmware_version))
            else:
                self.firmware_version = ''
                self.logger.warning('  Failed to get Firmware Version')
                sys.exit(1)

            # Query Serial Number
            result = self.query('!K')
            if result:
                self.serial_number = result[0].strip()
                self.logger.info('  Serial Number: {}'.format(self.serial_number))
            else:
                self.serial_number = ''
                self.logger.warning('  Failed to get Serial Number')
                sys.exit(1)

    def get_reading(self):
        """ Calls commands to be performed each time through the loop """
        weather_data = dict()

        if self.db is None:
            self.db = get_mongodb()
        else:
            weather_data = self.update_weather()
            self.calculate_and_set_PWM()

        return weather_data

    def send(self, send, delay=0.100):

        found_command = False
        for cmd in self.commands.keys():
            if re.match(cmd, send):
                self.logger.debug('Sending command: {}'.format(self.commands[cmd]))
                found_command = True
                break
        if not found_command:
            self.logger.warning('Unknown command: "{}"'.format(send))
            return None

        self.logger.debug('  Clearing buffer')
        cleared = self.AAG.read(self.AAG.inWaiting())
        if len(cleared) > 0:
            self.logger.debug('  Cleared: "{}"'.format(cleared.decode('utf-8')))

        self.AAG.write(send.encode('utf-8'))
        time.sleep(delay)

        result = None
        try:
            response = self.AAG.read(self.AAG.inWaiting()).decode('utf-8')
        except UnicodeDecodeError:
            self.logger.debug("Error reading from serial line")
        else:
            self.logger.debug('  Response: "{}"'.format(response))
            ResponseMatch = re.match('(!.*)\\x11\s{12}0', response)
            if ResponseMatch:
                result = ResponseMatch.group(1)
            else:
                result = response

        return result

    def query(self, send, maxtries=5):
        found_command = False
        for cmd in self.commands.keys():
            if re.match(cmd, send):
                self.logger.debug('Sending command: {}'.format(self.commands[cmd]))
                found_command = True
                break
        if not found_command:
            self.logger.warning('Unknown command: "{}"'.format(send))
            return None

        if cmd in self.delays.keys():
            self.logger.debug('  Waiting delay time of {:.3f} s'.format(self.delays[cmd]))
            delay = self.delays[cmd]
        else:
            delay = 0.200
        expect = self.expects[cmd]
        count = 0
        result = None
        while not result and (count <= maxtries):
            count += 1
            result = self.send(send, delay=delay)

            MatchExpect = re.match(expect, result)
            if not MatchExpect:
                self.logger.debug('Did not find {} in response "{}"'.format(expect, result))
                result = None
                time.sleep(self.hibernate)
            else:
                self.logger.debug('Found {} in response "{}"'.format(expect, result))
                result = MatchExpect.groups()
        return result

    def get_ambient_temperature(self, n=5):
        """
        Populates the self.ambient_temp property

        Calculation is taken from Rs232_Comms_v100.pdf section "Converting values
        sent by the device to meaningful units" item 5.
        """
        self.logger.debug('Getting ambient temperature')
        values = []

        for i in range(0, n):
            try:
                value = float(self.query('!T')[0])
                ambient_temp = value / 100.

            except Exception:
                pass
            else:
                self.logger.debug('  Ambient Temperature Query = {:.1f}\t{:.1f}'.format(value, ambient_temp))
                values.append(ambient_temp)

        if len(values) >= n - 1:
            self.ambient_temp = np.median(values) * u.Celsius
            self.logger.debug('  Ambient Temperature = {:.1f}'.format(self.ambient_temp))
        else:
            self.ambient_temp = None
            self.logger.debug('  Failed to Read Ambient Temperature')

        return self.ambient_temp

    def get_sky_temperature(self, n=9):
        """
        Populates the self.sky_temp property

        Calculation is taken from Rs232_Comms_v100.pdf section "Converting values
        sent by the device to meaningful units" item 1.

        Does this n times as recommended by the "Communication operational
        recommendations" section in Rs232_Comms_v100.pdf
        """
        self.logger.debug('Getting sky temperature')
        values = []
        for i in range(0, n):
            try:
                value = float(self.query('!S')[0]) / 100.
            except Exception:
                pass
            else:
                self.logger.debug('  Sky Temperature Query = {:.1f}'.format(value))
                values.append(value)
        if len(values) >= n - 1:
            self.sky_temp = np.median(values) * u.Celsius
            self.logger.debug('  Sky Temperature = {:.1f}'.format(self.sky_temp))
        else:
            self.sky_temp = None
            self.logger.debug('  Failed to Read Sky Temperature')
        return self.sky_temp

    def get_values(self, n=5):
        """
        Populates the self.internal_voltage, self.LDR_resistance, and
        self.rain_sensor_temp properties

        Calculation is taken from Rs232_Comms_v100.pdf section "Converting values
        sent by the device to meaningful units" items 4, 6, 7.
        """
        self.logger.debug('Getting "values"')
        ZenerConstant = 3
        LDRPullupResistance = 56.
        RainPullUpResistance = 1
        RainResAt25 = 1
        RainBeta = 3450.
        ABSZERO = 273.15
        internal_voltages = []
        LDR_resistances = []
        rain_sensor_temps = []
        for i in range(0, n):
            responses = self.query('!C')
            try:
                internal_voltage = 1023 * ZenerConstant / float(responses[0])
                internal_voltages.append(internal_voltage)
                LDR_resistance = LDRPullupResistance / ((1023. / float(responses[1])) - 1.)
                LDR_resistances.append(LDR_resistance)
                r = np.log((RainPullUpResistance / ((1023. / float(responses[2])) - 1.)) / RainResAt25)
                rain_sensor_temp = 1. / ((r / RainBeta) + (1. / (ABSZERO + 25.))) - ABSZERO
                rain_sensor_temps.append(rain_sensor_temp)
            except Exception:
                pass

        # Median Results
        if len(internal_voltages) >= n - 1:
            self.internal_voltage = np.median(internal_voltages) * u.volt
            self.logger.debug('  Internal Voltage = {:.2f}'.format(self.internal_voltage))
        else:
            self.internal_voltage = None
            self.logger.debug('  Failed to read Internal Voltage')

        if len(LDR_resistances) >= n - 1:
            self.LDR_resistance = np.median(LDR_resistances) * u.kohm
            self.logger.debug('  LDR Resistance = {:.0f}'.format(self.LDR_resistance))
        else:
            self.LDR_resistance = None
            self.logger.debug('  Failed to read LDR Resistance')

        if len(rain_sensor_temps) >= n - 1:
            self.rain_sensor_temp = np.median(rain_sensor_temps) * u.Celsius
            self.logger.debug('  Rain Sensor Temp = {:.1f}'.format(self.rain_sensor_temp))
        else:
            self.rain_sensor_temp = None
            self.logger.debug('  Failed to read Rain Sensor Temp')

        return (self.internal_voltage, self.LDR_resistance, self.rain_sensor_temp)

    def get_rain_frequency(self, n=5):
        """
        Populates the self.rain_frequency property
        """
        self.logger.debug('Getting rain frequency')
        values = []
        for i in range(0, n):
            try:
                value = float(self.query('!E')[0])
                self.logger.debug('  Rain Freq Query = {:.1f}'.format(value))
                values.append(value)
            except Exception:
                pass
        if len(values) >= n - 1:
            self.rain_frequency = np.median(values)
            self.logger.debug('  Rain Frequency = {:.1f}'.format(self.rain_frequency))
        else:
            self.rain_frequency = None
            self.logger.debug('  Failed to read Rain Frequency')
        return self.rain_frequency

    def get_PWM(self):
        """
        Populates the self.PWM property.

        Calculation is taken from Rs232_Comms_v100.pdf section "Converting values
        sent by the device to meaningful units" item 3.
        """
        self.logger.debug('Getting PWM value')
        try:
            value = self.query('!Q')[0]
            self.PWM = float(value) * 100. / 1023.
            self.logger.debug('  PWM Value = {:.1f}'.format(self.PWM))
        except Exception:
            self.PWM = None
            self.logger.debug('  Failed to read PWM Value')
        return self.PWM

    def set_PWM(self, percent, ntries=15):
        """
        """
        count = 0
        success = False
        if percent < 0.:
            percent = 0.
        if percent > 100.:
            percent = 100.
        while not success and count <= ntries:
            self.logger.debug('Setting PWM value to {:.1f} %'.format(percent))
            send_digital = int(1023. * float(percent) / 100.)
            send_string = 'P{:04d}!'.format(send_digital)
            try:
                result = self.query(send_string)
            except Exception:
                result = None
            count += 1
            if result is not None:
                self.PWM = float(result[0]) * 100. / 1023.
                if abs(self.PWM - percent) > 5.0:
                    self.logger.debug('  Failed to set PWM value!')
                    time.sleep(2)
                else:
                    success = True
                self.logger.debug('  PWM Value = {:.1f}'.format(self.PWM))

    def get_errors(self):
        """
        Populates the self.IR_errors property
        """
        self.logger.debug('Getting errors')
        response = self.query('!D')
        if response:
            self.errors = {'error_1': str(int(response[0])),
                           'error_2': str(int(response[1])),
                           'error_3': str(int(response[2])),
                           'error_4': str(int(response[3]))}
            self.logger.debug("  Internal Errors: {} {} {} {}".format(
                self.errors['error_1'],
                self.errors['error_2'],
                self.errors['error_3'],
                self.errors['error_4'],
            ))

        else:
            self.errors = {'error_1': None,
                           'error_2': None,
                           'error_3': None,
                           'error_4': None}
        return self.errors

    def get_switch(self, maxtries=3):
        """
        Populates the self.switch property

        Unlike other queries, this method has to check if the return matches a
        !X or !Y pattern (indicating open and closed respectively) rather than
        read a value.
        """
        self.logger.debug('Getting switch status')
        self.switch = None
        tries = 0
        status = None
        while not status:
            tries += 1
            response = self.send('!F')
            if re.match('!Y            1!', response):
                status = 'OPEN'
            elif re.match('!X            1!', response):
                status = 'CLOSED'
            else:
                status = None
            if not status and tries >= maxtries:
                status = 'UNKNOWN'
        self.switch = status
        self.logger.debug('  Switch Status = {}'.format(self.switch))
        return self.switch

    def wind_speed_enabled(self):
        """
        Method returns true or false depending on whether the device supports
        wind speed measurements.
        """
        self.logger.debug('Checking if wind speed is enabled')
        try:
            enabled = bool(self.query('v!')[0])
            if enabled:
                self.logger.debug('  Anemometer enabled')
            else:
                self.logger.debug('  Anemometer not enabled')
        except Exception:
            enabled = None
        return enabled

    def get_wind_speed(self, n=3):
        """
        Populates the self.wind_speed property

        Based on the information in Rs232_Comms_v120.pdf document

        Medians n measurements.  This isn't mentioned specifically by the manual
        but I'm guessing it won't hurt.
        """
        self.logger.debug('Getting wind speed')
        if self.wind_speed_enabled():
            values = []
            for i in range(0, n):
                result = self.query('V!')
                if result:
                    value = float(result[0])
                    self.logger.debug('  Wind Speed Query = {:.1f}'.format(value))
                    values.append(value)
            if len(values) >= 3:
                self.wind_speed = np.median(values) * u.km / u.hr
                self.logger.debug('  Wind speed = {:.1f}'.format(self.wind_speed))
            else:
                self.wind_speed = None
        else:
            self.wind_speed = None
        return self.wind_speed

    def capture(self, use_mongo=False, send_message=False, **kwargs):
        """ Query the CloudWatcher """

        self.logger.debug("Updating weather data")

        current_values = {}
        current_values['Weather data from'] = self.sensor_data.get('name')
        current_values['Weather sensor firmware version'] = self.firmware_version
        current_values['Weather sensor serial number'] = self.serial_number
        current_values['Date'] = dt.utcnow()

        if self.get_sky_temperature():
            current_values['Sky temperature'] = self.sky_temp.value
        if self.get_ambient_temperature():
            current_values['Ambient temperature'] = self.ambient_temp.value
        self.get_values()
        if self.internal_voltage:
            current_values['Internal voltage'] = self.internal_voltage.value
        if self.LDR_resistance:
            current_values['LDR resistance'] = self.LDR_resistance.value
        if self.rain_sensor_temp:
            current_values['Rain sensor temperature'] = "{:.02f}".format(self.rain_sensor_temp.value)
        if self.get_rain_frequency():
            current_values['Rain frequency'] = self.rain_frequency
        if self.get_PWM():
            current_values['PWM value'] = self.PWM
        if self.get_errors():
            current_values['Errors'] = self.errors
        if self.get_wind_speed():
            current_values['Wind speed'] = self.wind_speed.value

        return super().capture(current_values, use_mongo=False, send_message=False, **kwargs)

    def AAG_heater_algorithm(self, target, last_entry):
        """
        Uses the algorithm described in RainSensorHeaterAlgorithm.pdf to
        determine PWM value.

        Values are for the default read cycle of 10 seconds.
        """
        deltaT = last_entry['Rain sensor temperature'] - target
        scaling = 0.5
        if deltaT > 8.:
            deltaPWM = -40 * scaling
        elif deltaT > 4.:
            deltaPWM = -20 * scaling
        elif deltaT > 3.:
            deltaPWM = -10 * scaling
        elif deltaT > 2.:
            deltaPWM = -6 * scaling
        elif deltaT > 1.:
            deltaPWM = -4 * scaling
        elif deltaT > 0.5:
            deltaPWM = -2 * scaling
        elif deltaT > 0.3:
            deltaPWM = -1 * scaling
        elif deltaT < -0.3:
            deltaPWM = 1 * scaling
        elif deltaT < -0.5:
            deltaPWM = 2 * scaling
        elif deltaT < -1.:
            deltaPWM = 4 * scaling
        elif deltaT < -2.:
            deltaPWM = 6 * scaling
        elif deltaT < -3.:
            deltaPWM = 10 * scaling
        elif deltaT < -4.:
            deltaPWM = 20 * scaling
        elif deltaT < -8.:
            deltaPWM = 40 * scaling
        return int(deltaPWM)

    def calculate_and_set_PWM(self):
        """
        Uses the algorithm described in RainSensorHeaterAlgorithm.pdf to decide
        whether to use impulse heating mode, then determines the correct PWM
        value.
        """
        self.logger.debug('Calculating new PWM Value')
        # Get Last n minutes of rain history
        now = dt.utcnow()

        entries = self.weather_entries

        self.logger.debug('  Found {} entries in last {:d} seconds.'.format(
            len(entries), int(self.heater_sensor_data['impulse_cycle']), ))

        last_entry = self.weather_entries[-1]
        rain_history = [x['rain_safe'] for x in entries if 'rain_safe' in x.keys()]

        if 'Ambient temperature' not in last_entry.keys():
            self.logger.warning('  Do not have Ambient Temperature measurement.  Can not determine PWM value.')
        elif 'Rain sensor temperature' not in last_entry.keys():
            self.logger.warning('  Do not have Rain Sensor Temperature measurement.  Can not determine PWM value.')
        else:
            # Decide whether to use the impulse heating mechanism
            if len(rain_history) > 3 and not np.any(rain_history):
                self.logger.debug('  Consistent wet/rain in history.  Using impulse heating.')
                if self.impulse_heating:
                    impulse_time = (now - self.impulse_start).total_seconds()
                    if impulse_time > float(self.heater_sensor_data['impulse_duration']):
                        self.logger.debug('  Impulse heating has been on for > {:.0f} seconds.  Turning off.'.format(
                            float(self.heater_sensor_data['impulse_duration'])
                        ))
                        self.impulse_heating = False
                        self.impulse_start = None
                    else:
                        self.logger.debug('  Impulse heating has been on for {:.0f} seconds.'.format(
                            impulse_time))
                else:
                    self.logger.debug('  Starting impulse heating sequence.')
                    self.impulse_start = now
                    self.impulse_heating = True
            else:
                self.logger.debug('  No impulse heating needed.')
                self.impulse_heating = False
                self.impulse_start = None

            # Set PWM Based on Impulse Method or Normal Method
            if self.impulse_heating:
                target_temp = float(last_entry['Ambient temperature']) + float(self.heater_sensor_data['impulse_temp'])
                if last_entry['Rain sensor temperature'] < target_temp:
                    self.logger.debug('  Rain sensor temp < target.  Setting heater to 100 %.')
                    self.set_PWM(100)
                else:
                    new_PWM = self.AAG_heater_algorithm(target_temp, last_entry)
                    self.logger.debug('  Rain sensor temp > target.  Setting heater to {:d} %.'.format(new_PWM))
                    self.set_PWM(new_PWM)
            else:
                if last_entry['Ambient temperature'] < self.heater_sensor_data['low_temp']:
                    deltaT = self.heater_sensor_data['low_delta']
                elif last_entry['Ambient temperature'] > self.heater_sensor_data['high_temp']:
                    deltaT = self.heater_sensor_data['high_delta']
                else:
                    frac = (last_entry['Ambient temperature'] - self.heater_sensor_data['low_temp']) /\
                           (self.heater_sensor_data['high_temp'] - self.heater_sensor_data['low_temp'])
                    deltaT = self.heater_sensor_data['low_delta'] + frac * \
                        (self.heater_sensor_data['high_delta'] - self.heater_sensor_data['low_delta'])
                target_temp = last_entry['Ambient temperature'] + deltaT
                new_PWM = int(self.heater_PID.recalculate(float(last_entry['Rain sensor temperature']),
                                                          new_set_point=target_temp))
                self.logger.debug('  last PID interval = {:.1f} s'.format(self.heater_PID.last_interval))
                self.logger.debug('  target={:4.1f}, actual={:4.1f}, new PWM={:3.0f}, P={:+3.0f}, I={:+3.0f} ({:2d}), D={:+3.0f}'.format(
                    target_temp, float(last_entry['Rain sensor temperature']),
                    new_PWM, self.heater_PID.Kp * self.heater_PID.Pval,
                    self.heater_PID.Ki * self.heater_PID.Ival,
                    len(self.heater_PID.history),
                    self.heater_PID.Kd * self.heater_PID.Dval,
                ))
                self.set_PWM(new_PWM)

    def _get_cloud_safety(self):
        """Gets Sky-ambient temperature to be used in base method."""
        sky_diff = [x['Sky temperature'] - x['Ambient temperature']
                    for x in self.weather_entries
                    if ('Ambient temperature' and 'Sky temperature') in x.keys()]

        self.weather_entries['Sky-ambient'] = max(sky_diff)

        return super()._get_cloud_safety(statuses)

    def _get_wind_safety(self):
        """Gets wind speed and assigns its moving average to the wind speed to
        be used in the base method.
        """
        end_time = dt.utcnow()

        wind_speed = [x['Wind speed']
                      for x in self.weather_entries
                      if 'Wind speed' in x.keys()]

        moving_avg_seconds = 120.

        if type(start_time) == str:
                start_time = date_parser(entries[0]['date'])

        typical_data_interval = (end_time - start_time).total_seconds() / len(entries)

        mavg_count = int(np.ceil(moving_avg_seconds / typical_data_interval))
        wind_mavg = movingaverage(wind_speed, mavg_count)

        self.weather_entries['Wind speed'] = max(wind_mavg)

        return super()._get_wind_safety(statuses)

    def _get_gust_safety(self):
        """Gets wind speed and assigns its maximum value to the wind gust to be
        used in the base method.
        """
        wind_speed = [x['Wind speed']
                      for x in self.weather_entries
                      if 'Wind speed' in x.keys()]

        self.weather_entries['Wind gust'] = max(wind_speed)

        return super()._get_gust_safety(statuses)

    def _get_rain_safety(self, statuses):
        """Gets the rain safety and weather conditions

        Args:
            statuses: The status of the weather data.

        Returns:
            The rain condition and the rain safety. For example:

            Rainy, False
        """
        safety_delay = self.safety_delay

        rain_condition = statuses['Rain frequency']

        if rain_condition == 'Rainy':
            self.logger.debug('UNSAFE:  Rain in last {:.0f} min.'.format(safety_delay))
            rain_safe = False
        elif rain_condition == 'Wet':
            self.logger.debug('UNSAFE:  Wet in last {:.0f} min.'.format(safety_delay))
            rain_safe = False
        elif rain_condition == 'Invalid':
            self.logger.debug('UNSAFE:  rain data is invalid')
            rain_safe = False
        elif rain_condition == 'Dry':
            rain_safe = True
        else:
            self.logger.debug('UNSAFE:  no rain data found')
            rain_condition = 'Unknown'
            rain_safe = False

        self.logger.debug('Rain Condition: {} (Rain frequency is {})'.format(
                          rain_condition, self.weather_entries['Rain frequency']))

        return rain_condition, rain_safe
