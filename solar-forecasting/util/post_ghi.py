from datetime import datetime
import json
import time
from gridappsd import GOSS

goss = GOSS()
goss.connect()
ghi_topic = '/topic/goss.gridappsd.meas.ghi'
ghi_queue = '/queue/goss.gridappsd.meas.ghi'
GHI_type = 'AtmosphericAnalogKind.irradanceGlobalHorizontal'

from influxdb import DataFrameClient
client = DataFrameClient(host='localhost', port=8086)

def post_from_db():
    database_name = u'measurements'
    for i in range(0,180):
        # current_time = datetime.utcnow().strftime('%2013-%m-%d %H:%M:00.00')
        current_time = datetime.now().strftime('%2013-%m-%d %H:%M:00.00')
        query_str = 'select "AtmosphericAnalogKind.irradanceGlobalHorizontal" from "weather"."autogen"."measurements" where time = \'{}\' '.format(current_time)
        res = client.query(query_str)
        # res = client.query('select "AtmosphericAnalogKind.irradanceGlobalHorizontal" from "weather"."autogen"."measurements" where time = \'2013-08-31 18:13:00.00\' ')
        if database_name in res:
            print res[database_name]
            #Post to GOSS
            temp_dict ={'type': GHI_type,
               'time':res[database_name].index[-1].strftime('%Y-%m-%d %H:%M:%S'),
               'measurement':res[database_name][GHI_type][-1]}
            ghi_json = json.dumps(temp_dict)
            goss.send(ghi_queue, ghi_json)
        else:
            print("No GHI data. Maybe it is night?")
        time.sleep(60)

if __name__ == '__main__':
    post_from_db()
