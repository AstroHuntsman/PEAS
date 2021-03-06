#!/usr/bin/env python3

# This is only to help me run the code on Jermaine's laptop
import sys
sys.path.append("C:\\Users\\tiger.JERMAINE\\Documents\\HWM\\PEAS")
sys.path.append("C:\\Users\\tiger.JERMAINE\\Documents\\HWM\\POCS")

import datetime
import pandas
import time

from datetime import datetime as dt

from plotly import graph_objs as plotly_go
from plotly import plotly
from plotly import tools as plotly_tools

from peas import weather
from peas import weather_metdata
from peas import weather_met23
from peas import weather_skymap


def get_plot(filename=None):
    stream_tokens = plotly_tools.get_credentials_file()['stream_ids']
    token_1 = stream_tokens[0]
    token_2 = stream_tokens[1]
    token_3 = stream_tokens[2]
    stream_id1 = dict(token=token_1, maxpoints=1500)
    stream_id2 = dict(token=token_2, maxpoints=1500)
    stream_id3 = dict(token=token_3, maxpoints=1500)

    # Get existing data
    x_data = {
        'time': [],
    }
    y_data = {
        'temp': [],
        'cloudiness': [],
        'rain': [],
    }

    if filename is not None:
        data = pandas.read_csv(filename, names=names)
        data.date = pandas.to_datetime(data.date)
        # Convert from UTC
        data.date = data.date + datetime.timedelta(hours=11)
        x_data['time'] = data.date
        y_data['temp'] = data.ambient_temp_C
        y_data['cloudiness'] = data.sky_temp_C
        y_data['rain'] = data.rain_frequency

    trace1 = plotly_go.Scatter(
        x=x_data['time'], y=y_data['temp'], name='Temperature', mode='lines', stream=stream_id1)
    trace2 = plotly_go.Scatter(
        x=x_data['time'], y=y_data['cloudiness'], name='Cloudiness', mode='lines', stream=stream_id2)
    trace3 = plotly_go.Scatter(
        x=x_data['time'], y=y_data['rain'], name='Rain', mode='lines', stream=stream_id3)

    fig = plotly_tools.make_subplots(rows=3, cols=1, shared_xaxes=True, shared_yaxes=False)
    fig.append_trace(trace1, 1, 1)
    fig.append_trace(trace2, 2, 1)
    fig.append_trace(trace3, 3, 1)

    fig['layout'].update(title="Observatory Weather")

    fig['layout']['xaxis1'].update(title="Time [AEDT]")

    fig['layout']['yaxis1'].update(title="Temp [C]")
    fig['layout']['yaxis2'].update(title="Cloudiness")
    fig['layout']['yaxis3'].update(title="Rain Sensor")

    url = plotly.plot(fig, filename='MQObs Weather - Temp')
    print("Plot available at {}".format(url))

    stream_temp = plotly.Stream(stream_id=token_1)
    stream_temp.open()

    stream_cloudiness = plotly.Stream(stream_id=token_2)
    stream_cloudiness.open()

    stream_rain = plotly.Stream(stream_id=token_3)
    stream_rain.open()

    streams = {
        'temp': stream_temp,
        'cloudiness': stream_cloudiness,
        'rain': stream_rain,
    }

    return streams

def write_capture_skymap(filename=None, data=None):
    """ A function that reads the AAT met data weather can calls itself on a timer """
    entry = "{} ({}): Safe={}; Gust={}, Wind={}, Sky={}, Rain={}.\n".format(
        data['weather_data_name'],
        data['date'],
        data['safe'],
        data['gust_condition'],
        data['wind_condition'],
        data['sky_condition'],
        data['rain_condition'],
    )

    if filename is not None:
        with open(filename, 'a') as f:
            f.write(entry)

def write_capture_met23(filename=None, data=None):
    """ A function that reads the AAT met data weather can calls itself on a timer """
    entry = "{} ({}): Safe={}; Gust={}, Wind={}, Rain={}.\n".format(
        data['weather_data_name'],
        data['date'],
        data['safe'],
        data['gust_condition'],
        data['wind_condition'],
        data['rain_condition'],
    )

    if filename is not None:
        with open(filename, 'a') as f:
            f.write(entry)

def write_capture_aat(filename=None, data=None):
    """ A function that reads the AAT met data weather can calls itself on a timer """
    entry = "{} ({}): Safe={}; Gust={}, Wind={}, Sky={}, Rain={}, Wetness={}.\n".format(
        data['weather_data_name'],
        data['time_UTC'],
        data['safe'],
        data['gust_condition'],
        data['wind_condition'],
        data['sky_condition'],
        data['rain_condition'],
        data['wetness_condition']
    )

    if filename is not None:
        with open(filename, 'a') as f:
            f.write(entry)

def write_capture_aag(filename=None, data=None):
    """ A function that reads the AAG CloudWatcher weather can calls itself on a timer """
    entry = "{} ({}): Safe={}; Gust={}, Wind={}, Sky={}, Rain={}.\nSky temp. = {}\nAmbient temp. = {}\nRain sensor temp. = {}\nRain frequency = {}\nWind speed = {}\n".format(
        data['weather_sensor_name'],
        data['date'].strftime('%d-%m-%Y %H:%M:%S'),
        data['safe'],
        data['gust_condition'],
        data['wind_condition'],
        data['sky_condition'],
        data['rain_condition']
    )

    if filename is not None:
        with open(filename, 'a') as f:
            f.write(entry)

if __name__ == '__main__':
    import argparse

    # Get the command line option
    parser = argparse.ArgumentParser(
        description="Make a plot of the weather for a give date.")

    parser.add_argument('--loop', action='store_true', default=True,
                        help="If should keep reading, defaults to True")
    parser.add_argument("-d", "--delay", dest="delay", default=60.0, type=float,
                        help="Interval to read weather")
    parser.add_argument("-f", "--filename", dest="filename", default=None,
                        help="Where to save results")
    parser.add_argument('--serial-port', dest='serial_port', default=None,
                        help='Serial port to connect')
    parser.add_argument('--plotly-stream', action='store_true', default=False, help="Stream to plotly")
    parser.add_argument('--store-mongo', action='store_true', default=True, help="Save to mongo")
    parser.add_argument('--send-message', action='store_true', default=True, help="Send message")
    args = parser.parse_args()

    # Weather objects
    aag = weather.AAGCloudSensor(serial_address=args.serial_port, use_mongo=args.store_mongo)
    aat = weather_metdata.AATMetData(use_mongo=args.store_mongo)
    met23 = weather_met23.Met23Weather(use_mongo=args.store_mongo)
    skymap = weather_skymap.SkyMapWeather(use_mongo=args.store_mongo)

    if args.plotly_stream:
        streams = None
        streams = get_plot(filename=args.filename)

    while True:
        aag_data = aag.capture(use_mongo=args.store_mongo, send_message=args.send_message)
        aat_data = aat.capture(use_mongo=args.store_mongo, send_message=args.send_message)
        skymap_data = skymap.capture(use_mongo=args.store_mongo, send_message=args.send_message)
        met23_data = met23.capture(use_mongo=args.store_mongo, send_message=args.send_message)

        # Save data to file
        if args.filename is not None:
            write_capture_aag(filename=args.filename, data=aag_data)
            write_capture_aat(filename=args.filename, data=aat_data)
            write_capture_skymap(filename=args.filename, data=skymap_data)
            write_capture_met23(filename=args.filename, data=met23_data)

        # Plot the weather data from the AAG sensor
        if args.plotly_stream:
            now = datetime.datetime.now()
            streams['temp'].write({'x': now, 'y': aag_data['Ambient temperature']})
            streams['cloudiness'].write({'x': now, 'y': aag_data['Sky temperature']})
            streams['rain'].write({'x': now, 'y': aag_data['Rain frequency']})

        if not args.loop:
            break

        time.sleep(args.delay)
