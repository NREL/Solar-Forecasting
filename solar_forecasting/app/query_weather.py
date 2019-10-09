
import os
import sys
import pandas as pd
sys.path.append('/usr/src/gridappsd-python')
from gridappsd import GridAPPSD


__GRIDAPPSD_URI__ = os.environ.get("GRIDAPPSD_URI", "localhost:61613")
print(__GRIDAPPSD_URI__)
if __GRIDAPPSD_URI__ == 'localhost:61613':
    gridappsd_obj = GridAPPSD(1234)
else:
    from gridappsd import utils
    # gridappsd_obj = GridAPPSD(simulation_id=1234, address=__GRIDAPPSD_URI__)
    print(utils.get_gridappsd_address())
    gridappsd_obj = GridAPPSD(1069573052, address=utils.get_gridappsd_address(),
                      username=utils.get_gridappsd_user(), password=utils.get_gridappsd_pass())
goss_sim = "goss.gridappsd.process.request.simulation"
weather_channel = 'goss.gridappsd.process.request.data.timeseries'

def query_weather(start_time, end_time):
    query = {"queryMeasurement":"weather",
            "queryFilter":{"startTime":"1357048800000000",
                            "endTime":"1357058860000000"},
                            "responseFormat":"JSON"}
    query['queryFilter']['startTime'] = start_time
    query['queryFilter']['endTime'] = end_time

    weather_results = gridappsd_obj.get_response(weather_channel, query, timeout=120)

    if 'error' in weather_results and len(weather_results['error']['message']) > 1:
        return None
    time_dict = {int(row['time']): row for row in weather_results['data']}
    # time_dict = {}
    # for row in weather_results['data']:
    #     time_dict[int(row['time'])] = row

    weather_df = pd.DataFrame(time_dict).T
    return weather_df

if __name__ == '__main__':
    # query_weather(1357048800000000, 1357058860000000)

    start_time = 1357140720 + (7 * 60 * 60)
    end_time = start_time + 3600 * 24

    weather_df = query_weather(start_time * 1000000, end_time * 1000000)
    current_sim_time = start_time
    temp_df = weather_df.iloc[weather_df.index.get_loc(current_sim_time, method='nearest')]
    ghi = temp_df['GlobalCM22']
    weather_time = temp_df['time']
    temp_dict = {'type': 'X',
                 'time': weather_time,
                 'measurement': ghi}
    print(temp_dict)
    # query_weather(1357048800000000, 1357058860000000)