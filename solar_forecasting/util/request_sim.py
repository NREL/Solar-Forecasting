import json
import time
import argparse
from gridappsd import GOSS
from gridappsd import GridAPPSD
from gridappsd.topics import simulation_output_topic
from solar_forecasting.app.main import Solar_Forecast
from time import strptime, strftime, mktime, gmtime
from calendar import timegm

goss_sim = "goss.gridappsd.process.request.simulation"
test_topic = 'goss.gridappsd.test'
responseQueueTopic = '/temp-queue/response-queue'
goss_simulation_status_topic = '/topic/goss.gridappsd/simulation/status/'


def _startTest(username,password,gossServer='localhost',stompPort='61613', simulationID=1234, rulePort=5000, topic="input"):
   req_template = {"power_system_config": {"SubGeographicalRegion_name": "_1CD7D2EE-3C91-3248-5662-A43EFEFAC224",
                                            "GeographicalRegion_name": "_24809814-4EC6-29D2-B509-7F8BFB646437",
                                            "Line_name": "_C1C3E687-6FFD-C753-582B-632A27E28507"},
                    "simulation_config": {"power_flow_solver_method": "NR",
                                          "duration": 120,
                                          "simulation_name": "ieee123",
                                          "simulator": "GridLAB-D",
                                          "start_time": 1248156000,
                                          "run_realtime": True,
                                          "timestep_frequency": "1000",
                                          "timestep_increment": "1000",
                                          "model_creation_config": {"load_scaling_factor": 1.0, "triplex": "y",
                                                                    "encoding": "u", "system_frequency": 60,
                                                                    "voltage_multiplier": 1.0,
                                                                    "power_unit_conversion": 1.0, "unique_names": "y",
                                                                    "schedule_name": "ieeezipload", "z_fraction": 0.0,
                                                                    "i_fraction": 1.0, "p_fraction": 0.0,
                                                                    "randomize_zipload_fractions": False,
                                                                    "use_houses": False},
                                          "simulation_broker_port": 52798, "simulation_broker_location": "127.0.0.1"},
                    "application_config": {"applications": [{"name": "der_dispatch_app", "config_string": "{}"}]},
                    "simulation_request_type": "NEW"}

   req_template['simulation_config']['model_creation_config']['load_scaling_factor'] = 1
   req_template['simulation_config']['run_realtime'] = True
   # req_template['simulation_config']['duration'] = 60 * 60 * 4
   req_template['simulation_config']['duration'] = 60 * 15
   req_template['simulation_config']['start_time'] = 1374510600
   req_template['power_system_config']['Line_name'] =  '_C1C3E687-6FFD-C753-582B-632A27E28507'
   # req_template['power_system_config']['Line_name'] = '_E407CBB6-8C8D-9BC9-589C-AB83FBF0826D'  # Mine 123pv
   # req_template['power_system_config']['Line_name'] = '_EBDB5A4A-543C-9025-243E-8CAD24307380'  # 123 with reg
   # # req_template['power_system_config']['Line_name'] = '_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62'  # 13
   # req_template['power_system_config']['Line_name'] = '_AAE94E4A-2465-6F5E-37B1-3E72183A4E44'  # New 8500
   req_template["application_config"]["applications"][0]['name'] = 'solar_forecasting_app'
   req_template['simulation_config']['start_time'] = timegm(strptime('2013-07-22 08:00:00 GMT', '%Y-%m-%d %H:%M:%S %Z'))

   app_config = {'run_freq': 60, 'run_on_host': False}
   app_config['run_realtime'] = req_template['simulation_config']['run_realtime']
   print(app_config)

   req_template["application_config"]["applications"] = [
       {"name": "solar_forecasting_app", "config_string": json.dumps(app_config)}]

   print(req_template)
   # req_template['power_system_config']['Line_name'] = '_67AB291F-DCCD-31B7-B499-338206B9828F' # J1

   simCfg13pv = json.dumps(req_template)
   print (simCfg13pv)
   goss = GOSS()
   goss.connect()

   simulation_id = goss.get_response(goss_sim, simCfg13pv, timeout=10)
   simulation_id = int(simulation_id['simulationId'])
   print (simulation_id)
   print('sent simulation request')
   time.sleep(1)

   if app_config['run_on_host']:
       listening_to_topic = simulation_output_topic(simulation_id)
       model_mrid = req_template['power_system_config']['Line_name']
       gapps = GridAPPSD(simulation_id)
       solar_forecast = Solar_Forecast(simulation_id, gapps, model_mrid, 1538484951, app_config)
       gapps.subscribe(listening_to_topic, solar_forecast)

       while True:
           time.sleep(0.1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t","--topic", type=str, help="topic, the default is input", default="input", required=False)
    parser.add_argument("-p","--port", type=int, help="port number, the default is 5000", default=5000, required=False)
    parser.add_argument("-i", "--id", type=int, help="simulation id", required=False)
    # parser.add_argument("--start_date", type=str, help="Simulation start date", default="2017-07-21 12:00:00", required=False)
    # parser.add_argument("--end_date", type=str, help="Simulation end date" , default="2017-07-22 12:00:00", required=False)
    # parser.add_argument('-o', '--options', type=str, default='{}')
    args = parser.parse_args()

    _startTest('system','manager',gossServer='127.0.0.1',stompPort='61613', simulationID=args.id, rulePort=args.port, topic=args.topic)
