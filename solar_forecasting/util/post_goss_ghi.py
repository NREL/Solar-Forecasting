from datetime import datetime
import json
import time
import os
from gridappsd import GOSS
from gridappsd import GridAPPSD
import argparse
import solar_forecasting.app.query_weather as query_weather

__GRIDAPPSD_URI__ = os.environ.get("GRIDAPPSD_URI", "localhost:61613")
print(__GRIDAPPSD_URI__)
if __GRIDAPPSD_URI__ == 'localhost:61613':
    # gridappsd_obj = GridAPPSD(1234)
    gridappsd_obj = GridAPPSD(1234)
    # goss = GOSS()
else:
    from gridappsd import utils
    # goss = GOSS(stomp_address=utils.get_gridappsd_address(),
    #                   username=utils.get_gridappsd_user(), password=utils.get_gridappsd_pass())
    gridappsd_obj = GridAPPSD(1069573052, address=utils.get_gridappsd_address(),
                      username=utils.get_gridappsd_user(), password=utils.get_gridappsd_pass())

import sys
sys.path.append('/usr/src/gridappsd-solar-forecasting')
# goss.connect()
ghi_weather_topic = '/topic/goss.gridappsd.weather.ghi'
GHI_type = 'AtmosphericAnalogKind.irradanceGlobalHorizontal'

def post_from_db(start_time=1357140600 , duration=3600*8, interval=60, time_sleep=.1):
    # start_time+=(7 * 60 * 60)
    end_time = start_time + 3600 * 24

    weather_df = query_weather.query_weather(start_time * 1000000, end_time * 1000000)
    current_sim_time = start_time
    # ghi = weather_df.iloc[weather_df.index.get_loc(current_sim_time, method='nearest')]['GlobalCM22']
    # print(ghi)
    while current_sim_time < start_time + duration :
        temp_df = weather_df.iloc[weather_df.index.get_loc(current_sim_time, method='nearest')]
        ghi = temp_df['GlobalCM22']
        weather_time = temp_df['time']
        temp_dict ={'type': GHI_type,
           'time':weather_time,
           'measurement':ghi}
        print(temp_dict)
        # print (current_sim_time)
        ghi_json = json.dumps(temp_dict)
        # goss.send(ghi_weather_topic, ghi_json)
        gridappsd_obj.send(ghi_weather_topic, ghi_json)
        current_sim_time += interval
        time.sleep(time_sleep)
    # print('Done')

def _main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_time", "-s", type=int, default=1357140600,
                        help="Integer epoch start time.")
    parser.add_argument("--duration", "-d", type=int, default=720,
                        help="How long the ghi should be posted")
    parser.add_argument("--interval", "-i", type=int, default=60,
                        help="Number of seconds to post")
    parser.add_argument("--time_sleep", "-t", type=float, default=60,
                        help="Number of seconds to post")
    opts = parser.parse_args()
    post_from_db(opts.start_time, opts.duration, opts.interval, opts.time_sleep)

if __name__ == '__main__':
    # post_from_db()
    _main()