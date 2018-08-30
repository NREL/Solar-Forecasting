# SRRL Data


Data has been downloaded from https://midcdmz.nrel.gov/srrl_bms/ and placed in the file GHI_DHI_Temp_Wind_20130101.csv


These are the options selected on the site :

- [x] 2013-January-1
- [x] 2013-December-31
- [x] Global CM22 (vent/corr.)
- [x] Dry Bulb Temp (Tower)
- [x] Relative Humidity (Tower)
- [x] Wind Speed (42')
- [x] Wind Direction (42')
- [x] Direct CH1
- [x] Diffuse CM22 (vent/corr.)
- [x] Selected 1-Min Data (ZIP Compressed)

To add the data to the time-series database you will need docker and to clone the gridappsd-docker repo.
 
git clone https://github.com/GRIDAPPSD/gridappsd-docker
cd gridappsd-docker 
./run.sh

Run the util/load_ghi.py script to load the data into influxdb