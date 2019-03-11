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
    osboxes@osboxes> docker build --network=host -t solar-forecast-app .
    ```

1.  Add the following to the gridappsd-docker/docker-compose.yml file

    ```` yaml
      solar_forecast:
        image: solar-forecast-app
        ports:
          - 9000:9000
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

Next to start the application through the viz follow the directions here: https://gridappsd.readthedocs.io/en/latest/using_gridappsd/index.html#start-gridapps-d-platform

If you want to run the application WITHOUT the viz, open another terminal window and move to the solar-forecasting GitHub directory. Perform the following commands:

```` console
user@local> export PYTHONPATH=/Users/Solar-Forecasting/
user@local> python solar_forecasting/util/post_goss_ghi.py --start_time 1357140600 --duration 720 --interval 10 -t .1

# The above options can be changed to a desired output

````

Docker

Two notes to use inside a docker container:
1. Add 9000 to the ports in the gridappsd-docker/docker-compose.yml like this:
  sample_app:
    image: sample-app
    ports:
      - 9000:9000
    environment:
      GRIDAPPSD_URI: tcp://gridappsd:61613
  #    GRIDAPPSD_USER: system
  #    GRIDAPPSD_PASS: manager
    depends_on:
      - gridappsd

2. Change  skt.bind('tcp://127.0.0.1:9000') to skt.bind('tcp://*:9000')
