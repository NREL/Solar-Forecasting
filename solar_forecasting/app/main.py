# BSD 3-Clause License
#
# Copyright (c) 2019 Alliance for Sustainable Energy, LLC
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import argparse
import csv
from datetime import datetime, timezone
from dateutil import tz
import json
import logging
import sys
from matplotlib import pyplot as plt
import os
import pandas as pd
import shutil
import signal
import time

import zmq
import zlib

import query_weather as query_weather
from ResultCSV import ResultCSV
sys.path.append('/usr/src/gridappsd-python')
from gridappsd import GridAPPSD
from gridappsd.topics import simulation_input_topic, simulation_output_topic
# import subprocess
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

    def __init__(self, simulation_id, gridappsd_obj, model_mrid, start_time, app_config={}):
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
        self._publish_to_topic = simulation_input_topic(simulation_id)
        self._start_time = start_time
        self._end_time = start_time + 3600 * 24
        self._message_count = 0
        self._run_freq = app_config.get('run_freq', 30)
        self._run_realtime = app_config.get('run_realtime', True)
        self._port = '9002'
        print(self._port )
        self._weather_df = None
        # self._weather_df = query_weather.query_weather(self._start_time,self._end_time)

        self.resFolder = ""

        MainDir=""
        circuit_name = ""
        self.resFolder = os.path.join(MainDir, 'adms_result_' + circuit_name + '_' + str(self._start_time))
        self._result_csv = ResultCSV()
        self._result_csv.create_result_folder(self.resFolder)
        self._result_csv.create_result_file(self.resFolder, 'result.csv', 'second,epoch time,GHI,forecast time,Forecast GHI')

        # self.create_result_file(self.resFolder)
        # self._results, self._res_csvfile, self._results_writer = self.create_result_file(self.resFolder, 'second,epoch time,GHI,forecast time,Forecast GHI')

        ctx = zmq.Context()
        self._skt = ctx.socket(zmq.PUB)
        if running_on_host():
            self._skt.bind('tcp://127.0.0.1:{}'.format(self._port))
        else:
            self._skt.bind('tcp://*:{}'.format(self._port))

        time.sleep(.3)
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

        obj = dict(first=dict(first=1, time=1))
        jobj = json.dumps(obj).encode('utf8')
        zobj = zlib.compress(jobj)
        print('zipped pickle is %i bytes' % len(zobj))
        self._skt.send(zobj)

    def signal_handler(self, signal, frame):
        print('You pressed Ctrl+C! Saving output')
        # self._res_csvfile.close()
        self._result_csv.close()
        self._skt.close()
        self.save_plots()
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

    def _send_pause(self):
        command = {
            "command": "pause"
        }
        command = json.dumps(command)
        _log.info("{}\n\n".format(command))
        self._gapps.send(self._publish_to_topic, command)

    def _send_resume(self):
        command = {
            "command": "resume"
        }
        command = json.dumps(command)
        _log.info("{}\n\n".format(command))
        self._gapps.send(self._publish_to_topic, command)

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
        _log.info(message[:200])
        self._send_simulation_status('STARTED', "Rec message " + message[:200], 'INFO')
        if message == '{}':
            return
        if headers['destination'].startswith('/topic/goss.gridappsd.simulation.log.'+str(self.simulation_id)):
            message = json.loads(message)
            if message['processStatus'] == "COMPLETE":
                print(message)
                self.signal_handler(None, None)
            return
        # print(message)
        print(message[:100])

        message = json.loads(message)

        if not message['message']['measurements']:
            print("Measurements is empty")
            return
        self.timestamp_ = message['message']['timestamp']

        if (self.timestamp_ - 2) % self._run_freq != 0:
            print('Time on the time check. ' + str(self.timestamp_) + ' ' + datetime.
                  fromtimestamp(self.timestamp_, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
            return
        else:
            if not self._run_realtime:
                self._send_pause()
                _log.info("Pausing to work")

        if 'measurement' in message:
            current_date, epoch_time, final_, forecast_date, ghi_obs = self.measurement_message(message)
        else:
            # self.timestamp_ = message['message']['timestamp']
            current_date, epoch_time, final_, forecast_date, ghi_obs = self.simulation_message(message)

        print(epoch_time, current_date, ghi_obs, forecast_date, str(final_))
        _log.info("REC {} {} {}".format(epoch_time, current_date, ghi_obs))
        _log.info("FORECAST {} {}".format(forecast_date, str(final_)))

        self._result_csv.write({'second': self._message_count,
                                'epoch time': epoch_time,
                                'GHI': ghi_obs,
                                'forecast time': epoch_time + (60 * 30),
                                'Forecast GHI': final_})

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

        if not self._run_realtime:
            self._send_resume()
            _log.info("Resuming to work")

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

    def measurement_message(self, message):
        # sim_time = message["message"]["timestamp"]
        # print(sim_time)
        # self._gapps.send(ghi_forecast_topic, json.dumps({'ghi':42.0}))
        timestamp = int(message['time'])
        epoch_time = timestamp - (7 * 60 * 60)
        epoch_time = timestamp
        # epoch_time = int(message['time']) - (3600 * 14)
        # epoch_time = int(message['time']) - 40000 + 3600 * 2
        # {'type': 'AtmosphericAnalogKind.irradanceGlobalHorizontal', 'time': '1357088820', 'measurement': '30.7117181'}
        # temp       = 1357088820
        # epoch_time = 1357048800 + 3600 * 2
        ghi_obs = float(message['measurement']) * 10.7639
        ghi_obs = float(message['measurement'])
        # epoch_time = int(time.time()) + 3600
        epoch_time = int(datetime.utcfromtimestamp(epoch_time).replace(tzinfo=tz.gettz('US/Mountain')).timestamp())
        current_date = pd.to_datetime(epoch_time, unit='s').round('min')
        forecast_date = pd.to_datetime(epoch_time + (60 * 30), unit='s').round('min')
        ghi_forecast_final = solar_forecasting.the_forecast(ghi_obs, current_date)
        final_ = ghi_forecast_final
        if type(ghi_forecast_final) != int:
            final_ = float(ghi_forecast_final[0])
        return current_date, epoch_time, final_, forecast_date, ghi_obs

    def simulation_message(self, message):

        timestamp = message['message']['timestamp']
        epoch_time = timestamp

        current_sim_time = epoch_time
        start_time = epoch_time
        end_time = start_time + (24 * 60 * 60)
        GHI_type = 'AtmosphericAnalogKind.irradanceGlobalHorizontal'
        print(start_time, end_time)

        if self._weather_df is None:
            self._weather_df = query_weather.query_weather(start_time * 1000000, end_time * 1000000)
        current_sim_time = start_time
        # ghi = weather_df.iloc[weather_df.index.get_loc(current_sim_time, method='nearest')]['GlobalCM22']
        # print(ghi)

        temp_df = self._weather_df.iloc[self._weather_df.index.get_loc(current_sim_time, method='nearest')]
        ghi = temp_df['GlobalCM22']
        weather_time = temp_df['time']
        temp_dict = {'type': GHI_type,
                     'time': weather_time,
                     'measurement': ghi}

        ghi_obs = float(temp_dict['measurement'])

        epoch_time = int(datetime.utcfromtimestamp(epoch_time).replace(tzinfo=tz.gettz('US/Mountain')).timestamp())
        current_date = pd.to_datetime(epoch_time, unit='s').round('min')
        forecast_date = pd.to_datetime(epoch_time + (60 * 30), unit='s').round('min')
        print(current_date)
        # print(pd.to_datetime(self.timestamp_, unit='s').round('min'))
        ghi_forecast_final = solar_forecasting.the_forecast(ghi_obs, current_date)
        final_ = ghi_forecast_final
        if type(ghi_forecast_final) != int:
            final_ = float(ghi_forecast_final[0])
        return current_date, epoch_time, final_, forecast_date, ghi_obs

    def save_plots(self):
        """
        Save plots for comparison. Need to have OPF off run first and the OPF on for comparison.
        :return:
        """
        results0 = pd.read_csv(os.path.join(self.resFolder, "result.csv"), index_col='epoch time')
        results0.index = pd.to_datetime(results0.index, unit='s')
        results1 = pd.read_csv(os.path.join(self.resFolder, "result.csv"), index_col='forecast time')
        results1.index = pd.to_datetime(results1.index, unit='s')
        size = (10, 10)
        fig, ax = plt.subplots(figsize=size)
        plt.grid(True)
        ax.plot(results0[['GHI']])
        ax.plot(results1[['Forecast GHI']])
        ax.legend(['GHI', 'Forecast GHI'])
        fig.savefig(os.path.join(self.resFolder, "GHI Forecast"), bbox_inches='tight')

def _main_local():
    simulation_id =1543123248
    listening_to_topic = simulation_output_topic(simulation_id)

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
    _log.info(str(sys.argv))
    _log.info("Args ")
    for arg in sys.argv[1:]:
        _log.info(type(arg))
        _log.info(arg)

    # _log.info("Run local only -JEFF")
    # exit(0)
    parser = argparse.ArgumentParser()
    parser.add_argument("simulation_id",
                        help="Simulation id to use for responses on the message bus.")
    parser.add_argument("request",
                        help="Simulation Request")
    parser.add_argument("opt",
                        help="opt")
    # parser.add_argument("opt", help="opt")
    opts = parser.parse_args()
    _log.info(opts)
    listening_to_topic = simulation_output_topic(opts.simulation_id)
    print(opts.request)
    sim_request = json.loads(opts.request.replace("\'", ""))
    model_mrid = sim_request['power_system_config']['Line_name']
    start_time = sim_request['simulation_config']['start_time']
    app_config = sim_request["application_config"]["applications"]
    print(app_config)

    # app_config = [json.loads(app['config_string']) for app in app_config if app['name'] == 'solar_forecasting_app'][0]
    app = [app for app in app_config if app['name'] == 'solar_forecasting_app'][0]
    print(app)
    if app['config_string']:
        # app_config = json.loads(app['config_string'])
        app_config = json.loads(app['config_string'].replace(u'\u0027', '"'))
    else:
        app_config = {'run_freq': 60, 'run_on_host': False}

    ## Run the docker container. WOOT!
    if 'run_on_host' in app_config and app_config['run_on_host']:
        exit(0)


    _log.info("Model mrid is: {}".format(model_mrid))
    gapps = GridAPPSD(opts.simulation_id, address=utils.get_gridappsd_address(),
                      username=utils.get_gridappsd_user(), password=utils.get_gridappsd_pass())

    # gapps = GridAPPSD(opts.simulation_id)
    solar_forecast = Solar_Forecast(opts.simulation_id, gapps, model_mrid, start_time, app_config)
    gapps.subscribe(listening_to_topic, solar_forecast)
    # gapps.subscribe(ghi_weather_topic, solar_forecast)


    # ls_output = subprocess.Popen(["python solar_forecasting/util/post_goss_ghi.py","1357048800","720" , "10"], stdout=subprocess.PIPE)

    while True:
        time.sleep(0.1)

def running_on_host():
    __GRIDAPPSD_URI__ = os.environ.get("GRIDAPPSD_URI", "localhost:61613")
    if __GRIDAPPSD_URI__ == 'localhost:61613':
        return True
    return False

if __name__ == '__main__':
    _log.info("Main")
    if running_on_host():

        # post_output = subprocess.Popen(["python", "/Users/jsimpson/git/adms/Solar-Forecasting/solar_forecasting/util/post_goss_ghi.py", "--start_time", "1357140600 ", "720", "10"],
        #                              stdout=subprocess.PIPE)

        _main_local()
    else:
        # p = subprocess.Popen(
        #     ["python", "/usr/src/gridappsd-solar-forecasting/solar_forecasting/util/post_goss_ghi.py",
        #      "--start_time", "1357140600 ", "--duration", "28800", "--interval", "60", "-t", ".1"],
        #     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
        # (child_stdout, child_stdin) = (p.stdout, p.stdin)
        # print(child_stdout.readlines())
        _main()
# python /usr/src/gridappsd-solar-forecasting/solar_forecasting/app/main.py 498829576 '{"power_system_config":{"SubGeographicalRegion_name":"_1CD7D2EE-3C91-3248-5662-A43EFEFAC224","GeographicalRegion_name":"_73C512BD-7249-4F50-50DA-D93849B89C43","Line_name":"_C1C3E687-6FFD-C753-582B-632A27E28507"},"simulation_config":{"power_flow_solver_method":"NR","duration":120,"simulation_name":"ieee123","simulator":"GridLAB-D","start_time":1564691354,"run_realtime":true,"simulation_output":{},"model_creation_config":{"load_scaling_factor":1.0,"triplex":"y","encoding":"u","system_frequency":60,"voltage_multiplier":1.0,"power_unit_conversion":1.0,"unique_names":"y","schedule_name":"ieeezipload","z_fraction":0.0,"i_fraction":1.0,"p_fraction":0.0,"randomize_zipload_fractions":false,"use_houses":false},"simulation_broker_port":54776,"simulation_broker_location":"127.0.0.1"},"application_config":{"applications":[{"name":"solar_forecasting_app","config_string":""}]},"test_config":{"events":[],"appId":"solar_forecasting_app"},"simulation_request_type":"NEW"}'


 # python /usr/src/gridappsd-solar-forecasting/solar_forecasting/app/main.py 1601065288 {"power_system_config":{"SubGeographicalRegion_name":"_1CD7D2EE-3C91-3248-5662-A43EFEFAC224","GeographicalRegion_name":"_73C512BD-7249-4F50-50DA-D93849B89C43","Line_name":"_C1C3E687-6FFD-C753-582B-632A27E28507"},"simulation_config":{"power_flow_solver_method":"NR","duration":120,"simulation_name":"ieee123","simulator":"GridLAB-D","start_time":1564697025,"run_realtime":true,"simulation_output":{},"model_creation_config":{"load_scaling_factor":1.0,"triplex":"y","encoding":"u","system_frequency":60,"voltage_multiplier":1.0,"power_unit_conversion":1.0,"unique_names":"y","schedule_name":"ieeezipload","z_fraction":0.0,"i_fraction":1.0,"p_fraction":0.0,"randomize_zipload_fractions":false,"use_houses":false},"simulation_broker_port":50501,"simulation_broker_location":"127.0.0.1"},"application_config":{"applications":[{"name":"solar_forecasting_app","config_string":" {\u0027run_freq\u0027: 60, \u0027run_on_host\u0027: false}"}]},"test_config":{"events":[],"appId":"solar_forecasting_app"},"simulation_request_type":"NEW"} {run_freq:60,run_on_host:false}

# python /usr/src/gridappsd-solar-forecasting/solar_forecasting/app/main.py 1601065288 '{"power_system_config":{"SubGeographicalRegion_name":"_1CD7D2EE-3C91-3248-5662-A43EFEFAC224","GeographicalRegion_name":"_73C512BD-7249-4F50-50DA-D93849B89C43","Line_name":"_C1C3E687-6FFD-C753-582B-632A27E28507"},"simulation_config":{"power_flow_solver_method":"NR","duration":120,"simulation_name":"ieee123","simulator":"GridLAB-D","start_time":1564697025,"run_realtime":true,"simulation_output":{},"model_creation_config":{"load_scaling_factor":1.0,"triplex":"y","encoding":"u","system_frequency":60,"voltage_multiplier":1.0,"power_unit_conversion":1.0,"unique_names":"y","schedule_name":"ieeezipload","z_fraction":0.0,"i_fraction":1.0,"p_fraction":0.0,"randomize_zipload_fractions":false,"use_houses":false},"simulation_broker_port":50501,"simulation_broker_location":"127.0.0.1"},"application_config":{"applications":[{"name":"solar_forecasting_app","config_string":" {\u0027run_freq\u0027: 60, \u0027run_on_host\u0027: false}"}]},"test_config":{"events":[],"appId":"solar_forecasting_app"},"simulation_request_type":"NEW"}' '{run_freq:60,run_on_host:false}'

