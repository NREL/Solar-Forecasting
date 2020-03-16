# Solar-Forecast
Solar-Forecast GridAPPS-D Application
## Purpose

The application to forecast GHI data

## Requirements

1. Docker ce version 17.12 or better.  You can install this via the docker_install_ubuntu.sh script.  (note for mint you will need to modify the file to work with xenial rather than ubuntu generically)

## Quick Start

1. Please clone the repository <https://github.com/GRIDAPPSD/gridappsd-docker> (refered to as gridappsd-docker repository) next to this repository (they should both have the same parent folder)

    ```console
    git clone https://github.com/GRIDAPPSD/gridappsd-docker
    git clone https://github.nrel.gov/PSEC/Solar-Forecasting
    
    ls -l
    
    drwxrwxr-x  7 osboxes osboxes 4096 Sep  4 14:56 gridappsd-docker
    drwxrwxr-x  5 osboxes osboxes 4096 Sep  4 19:06 Solar-Forecasting

    ```

## Creating the sample-app application container

1.  From the command line execute the following commands to build the sample-app container

    ```console
    osboxes@osboxes> cd Solar-Forecasting
    osboxes@osboxes> docker build --network=host -t solar-forecasting-app .
    ```

1.  Add the following to the gridappsd-docker/docker-compose.yml file

    ```` yaml
      solar_forecast:
        image: solar-forecast-app
        ports:
          - 9002:9002
        environment:
          GRIDAPPSD_URI: tcp://gridappsd:61613
        depends_on:
          - gridappsd
    ````

1.  Run the docker application 

    ```` console
    osboxes@osboxes> cd gridappsd-docker
    osboxes@osboxes> ./run.sh -t develop
    
    # you will now be inside the container, the following starts gridappsd
    
    gridappsd@f4ede7dacb7d:/gridappsd$ ./run-gridappsd.sh
    
    ````
1. Run bokeh on port 9002

```bash
python -m bokeh serve --show bokeh_two_plot/ --allow-websocket-origin=*:5006 --args --port 9002
```

Next to start the application through the viz follow the directions here: https://gridappsd.readthedocs.io/en/latest/using_gridappsd/index.html#start-gridapps-d-platform

If you want to run the application WITHOUT the viz, open another terminal window and move to the solar-forecasting GitHub directory. Perform the following commands:

```` console
user@local> export PYTHONPATH=/Users/Solar-Forecasting/
user@local> python solar_forecasting/util/post_goss_ghi.py --start_time 1357140600 --duration 720 --interval 10 -t .1
user@local> python solar_forecasting/util/post_goss_ghi.py --start_time 1370260800 --duration 43200 --interval 60 -t .1

# The above options can be changed to a desired output

````

Docker

Two notes to use inside a docker container:
1. Add 9002 to the ports in the gridappsd-docker/docker-compose.yml like this:

```yaml

  solar_forecasting:
    image: solar-forecasting-app
    ports:
      - 9002:9002
    environment:
      GRIDAPPSD_URI: tcp://gridappsd:61613
    depends_on:
      - gridappsd
```

```bash
>python /usr/src/gridappsd-solar-forecasting/solar_forecasting/app/main.py 1112306683 '{"power_system_config": {"SubGeographicalRegion_name": "_1CD7D2EE-3C91-3248-5662-A43EFEFAC224", "GeographicalRegion_name": "_24809814-4EC6-29D2-B509-7F8BFB646437", "Line_name": "_EBDB5A4A-543C-9025-243E-8CAD24307380"}, "simulation_config": {"power_flow_solver_method": "NR", "duration": 48, "simulation_name": "ieee123", "simulator": "GridLAB-D", "start_time": 1538484951, "run_realtime": true, "simulation_output": {}, "model_creation_config": {"load_scaling_factor": 1.0, "triplex": "y", "encoding": "u", "system_frequency": 60, "voltage_multiplier": 1.0, "power_unit_conversion": 1.0, "unique_names": "y", "schedule_name": "ieeezipload", "z_fraction": 0.0, "i_fraction": 1.0, "p_fraction": 0.0, "randomize_zipload_fractions": false, "use_houses": false}, "simulation_broker_port": 52798, "simulation_broker_location": "127.0.0.1"}, "application_config": {"applications": [{"name": "der_dispatch_app", "config_string": "{\"OPF\": 1, \"run_freq\": 60, \"run_on_host\": false, \"run_realtime\": true, \"stepsize_xp\": 0.2, \"stepsize_xq\": 2, \"coeff_p\": 0.1, \"coeff_q\": 5e-05, \"stepsize_mu\": 500}"}]}, "simulation_request_type": "NEW"}'

```