import argparse
from datetime import datetime
import json
import logging
import sys
import os
import pandas as pd
import signal
import time

import zmq
import zlib

import query_weather as query_weather
sys.path.append('/usr/src/gridappsd-python')
from gridappsd import GridAPPSD
from gridappsd.topics import fncs_input_topic, fncs_output_topic
import subprocess
import solar_forecasting_application_simple_v2 as solar_forecasting

ghi_forecast_topic = '/topic/goss.gridappsd.forecast.ghi'
ghi_weather_topic = '/topic/goss.gridappsd.weather.ghi'

logging.basicConfig(filename='app.log',
                    filemode='w',
                    # stream=sys.stdout,
                    level=logging.INFO,
                    format="%(asctime)s - %(name)s;%(levelname)s|%(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
# Only log errors to the stomp logger.
logging.getLogger('stomp.py').setLevel(logging.ERROR)

_log = logging.getLogger(__name__)


class Solar_Forecast(object):
    """ A simple class that handles publishing the solar forecast
    """

    def __init__(self, simulation_id, gridappsd_obj, model_mrid, start_time):
        """ Create a ``Solar_Forecast`` object


        Note
        ----
        This class does not subscribe only publishes.

        Parameters
        ----------
        simulation_id: str
            The simulation_id to use for publishing to a topic.
        gridappsd_obj: GridAPPSD
            An instatiated object that is connected to the gridappsd message bus
            usually this should be the same object which subscribes, but that
            isn't required.

        """
        _log.info("Init application")
        self.simulation_id = simulation_id
        self._gapps = gridappsd_obj
        self.fidselect = model_mrid
        self._publish_to_topic = fncs_input_topic(simulation_id)
        self._start_time = start_time
        self._end_time = start_time + 3600 * 24
        self._message_count = 0
        # self._weather_df = query_weather.query_weather(self._start_time,self._end_time)
        ctx = zmq.Context()
        self._skt = ctx.socket(zmq.PUB)
        if running_on_host():
            self._skt.bind('tcp://127.0.0.1:9000')
        else:
            self._skt.bind('tcp://*:9000')

        time.sleep(.3)
        signal.signal(signal.SIGINT, self.signal_handler)

        obj = dict(first=dict(first=1, time=1))
        jobj = json.dumps(obj).encode('utf8')
        zobj = zlib.compress(jobj)
        print('zipped pickle is %i bytes' % len(zobj))
        self._skt.send(zobj)


    def signal_handler(self, signal, frame):
        print('You pressed Ctrl+C! Saving output')
        self._skt.close()
        sys.exit(0)

    def _send_simulation_status(self, status, message, log_level):
        """send a status message to the GridAPPS-D log manager

        Function arguments:
            status -- Type: string. Description: The status of the simulation.
                Default: 'localhost'.
            stomp_port -- Type: string. Description: The port for Stomp
            protocol for the GOSS server. It must not be an empty string.
                Default: '61613'.
            username -- Type: string. Description: User name for GOSS connection.
            password -- Type: string. Description: Password for GOSS connection.

        Function returns:
            None.
        Function exceptions:
            RuntimeError()
        """
        simulation_status_topic = "goss.gridappsd.process.simulation.log.{}".format(self.simulation_id)

        valid_status = ['STARTING', 'STARTED', 'RUNNING', 'ERROR', 'CLOSED', 'COMPLETE']
        valid_level = ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL']
        if status in valid_status:
            if log_level not in valid_level:
                log_level = 'INFO'
            t_now = datetime.utcnow()
            status_message = {
                "source": os.path.basename(__file__),
                "processId": str(self.simulation_id),
                "timestamp": int(time.mktime(t_now.timetuple())),
                "processStatus": status,
                "logMessage": str(message),
                "logLevel": log_level,
                "storeToDb": True
            }
            status_str = json.dumps(status_message)
            _log.info("{}".format(status_str))
            # debugFile.write("{}\n\n".format(status_str))
            self._gapps.send(simulation_status_topic, status_str)

    def on_message(self, headers, message):
        """ Handle incoming messages on the fncs_output_topic for the simulation_id

        Parameters
        ----------
        headers: dict
            A dictionary of headers that could be used to determine topic of origin and
            other attributes.
        message: object
            A data structure following the protocol defined in the message structure
            of ``GridAPPSD``.  Most message payloads will be serialized dictionaries, but that is
            not a requirement.
        """


        print(message[:200])
        self._send_simulation_status('STARTED', "Rec message " + message[:200], 'INFO')
        if message == '{}':
            return

        message = json.loads(message)
        # sim_time = message["message"]["timestamp"]
        # print(sim_time)
        # self._gapps.send(ghi_forecast_topic, json.dumps({'ghi':42.0}))
        epoch_time = int(message['time']) - (7 * 60 * 60)
        epoch_time = int(message['time'])
        # epoch_time = int(message['time']) - (3600 * 14)
        # epoch_time = int(message['time']) - 40000 + 3600 * 2

        # {'type': 'AtmosphericAnalogKind.irradanceGlobalHorizontal', 'time': '1357088820', 'measurement': '30.7117181'}
        # temp       = 1357088820
        # epoch_time = 1357048800 + 3600 * 2
        ghi_obs = float(message['measurement']) * 10.7639
        ghi_obs = float(message['measurement'])

        # epoch_time = int(time.time()) + 3600
        current_date = pd.to_datetime(epoch_time, unit='s').round('min')
        forecast_date = pd.to_datetime(epoch_time + (60 * 30), unit='s').round('min')
        ghi_forecast_final = solar_forecasting.the_forecast(ghi_obs, current_date)
        final_ = ghi_forecast_final
        if type(ghi_forecast_final) != int:
            final_ = float(ghi_forecast_final[0])

        print(epoch_time, current_date, ghi_obs, forecast_date, str(final_))
        _log.info("REC {} {} {}".format(epoch_time, current_date, ghi_obs))
        _log.info("FORECAST {} {}".format(forecast_date, str(final_)))

        obj = {}

        temp_dict = dict()
        temp_dict[u'GHI'] = {'data': ghi_obs, 'time': epoch_time}
        temp_dict[u'Forecast GHI'] = {'data': final_, 'time': epoch_time + (60 * 30)}
        # temp_dict[u'time'] = epoch_time
        obj[u'GHI'] = temp_dict

        # temp_dict = {}
        # temp_dict[u'Forecast GHI'] = str(ghi_forecast_final[0])
        # temp_dict[u'time'] = epoch_time + (60 * 30)
        # obj[u'Forecast GHI'] = temp_dict

        # pobj = pickle.dumps(obj, 0)
        # zobj = zlib.compress(pobj)
        jobj = json.dumps(obj).encode('utf8')
        zobj = zlib.compress(jobj)
        print('zipped pickle is %i bytes' % len(zobj))
        self._skt.send(zobj)


        self._message_count += 1

        # Every message_period messages we are going to turn the capcitors on or off depending
        # on the current capacitor state.
        # if self._message_count % message_period == 0:
        #     if self._last_toggle_on:
        #         _log.debug("count: {} toggling off".format(self._message_count))
        #         msg = self._close_diff.get_message()
        #         self._last_toggle_on = False
        #     else:
        #         _log.debug("count: {} toggling on".format(self._message_count))
        #         msg = self._open_diff.get_message()
        #         self._last_toggle_on = True
        #
        #     self._gapps.send(self._publish_to_topic, json.dumps(msg))




def _main_local():
    simulation_id =1543123248
    listening_to_topic = fncs_output_topic(simulation_id)

    model_mrid = '_E407CBB6-8C8D-9BC9-589C-AB83FBF0826D'

    gapps = GridAPPSD(simulation_id)
    # __GRIDAPPSD_URI__ = os.environ.get("GRIDAPPSD_URI", "localhost:61613")
    # gapps = GridAPPSD(simulation_id, address=__GRIDAPPSD_URI__)
    solar_forecast = Solar_Forecast(simulation_id, gapps, model_mrid, 1357048800)
    gapps.subscribe(ghi_weather_topic, solar_forecast)

    while True:
        time.sleep(0.1)


def _main():
    from gridappsd import utils
    _log.info("Starting application")
    # _log.info("Run local only -JEFF")
    # exit(0)
    parser = argparse.ArgumentParser()
    parser.add_argument("simulation_id",
                        help="Simulation id to use for responses on the message bus.")
    parser.add_argument("request",
                        help="Simulation Request")
    opts = parser.parse_args()
    listening_to_topic = fncs_output_topic(opts.simulation_id)

    sim_request = json.loads(opts.request.replace("\'",""))
    model_mrid = sim_request["power_system_config"]["Line_name"]
    start_time = sim_request["power_system_config"]["simulation_config"]["start_time"]
    durattopn = sim_request["power_system_config"]["simulation_config"]["duration"]
    _log.info("Model mrid is: {}".format(model_mrid))
    gapps = GridAPPSD(opts.simulation_id, address=utils.get_gridappsd_address(),
                      username=utils.get_gridappsd_user(), password=utils.get_gridappsd_pass())

    # gapps = GridAPPSD(opts.simulation_id)
    solar_forecast = Solar_Forecast(opts.simulation_id, gapps, model_mrid, start_time)
    gapps.subscribe(ghi_weather_topic, solar_forecast)


    # ls_output = subprocess.Popen(["python solar_forecasting/util/post_goss_ghi.py","1357048800","720" , "10"], stdout=subprocess.PIPE)

    while True:
        time.sleep(0.1)

def running_on_host():
    __GRIDAPPSD_URI__ = os.environ.get("GRIDAPPSD_URI", "localhost:61613")
    if __GRIDAPPSD_URI__ == 'localhost:61613':
        return True
    return False

if __name__ == '__main__':
    if running_on_host():

        # post_output = subprocess.Popen(["python", "/Users/jsimpson/git/adms/Solar-Forecasting/solar_forecasting/util/post_goss_ghi.py", "--start_time", "1357140600 ", "720", "10"],
        #                              stdout=subprocess.PIPE)

        _main_local()
    else:
        p = subprocess.Popen(
            ["python", "/usr/src/gridappsd-solar-forecasting/solar_forecasting/util/post_goss_ghi.py",
             "--start_time", "1357140600 ", "--duration", "28800", "--interval", "60", "-t", ".1"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
        (child_stdout, child_stdin) = (p.stdout, p.stdin)
        print(child_stdout.readlines())
        _main()
