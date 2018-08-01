import pandas as pd

from influxdb import DataFrameClient


def load():
    df = pd.read_csv('../data/GHI_DHI_Temp_Wind_20130101.csv.gz', compression='gzip',skiprows=1)
    df.index = pd.to_datetime(df['DATE (MM/DD/YYYY)'] + ' ' + df['MST'], format='%m/%d/%Y %H:%M')
    df.columns = [u'DATE (MM/DD/YYYY)', u'MST', u'AtmosphericAnalogKind.irradanceGlobalHorizontal',
                  u'AtmosphericAnalogKind.irradanceDiffuseHorizontal',
                  u'AtmosphericAnalogKind.ambientTemperature', u'AtmosphericAnalogKind.humidity',
                  u'AtmosphericAnalogKind.speed', u'AtmosphericAnalogKind.bearing']
    dbname = 'weather'

    protocol = 'json'

    client = DataFrameClient(host='localhost', port=8086)

    print("Delete database: " + dbname)
    client.drop_database(dbname)

    print("Create pandas DataFrame")

    print("Create database: " + dbname)
    client.drop_database(dbname)
    client.create_database(dbname)

    client.switch_database(dbname)

    # print("Write DataFrame")
    client.write_points(df.loc['2013-7-1':'2013-7-31'], 'measurements', protocol=protocol)
    client.write_points(df.loc['2013-8-1':'2013-8-31'], 'measurements', protocol=protocol)
    client.write_points(df.loc['2013-9-1':'2013-9-30'], 'measurements', protocol=protocol)

    print("Write DataFrame with Tags")
    # client.write_points(df, 'demo',
    #                     {'k1': 'v1', 'k2': 'v2'}, protocol=protocol)

    print("Read DataFrame")
    # client.query("select * from weather")

if __name__ == '__main__':
    load()