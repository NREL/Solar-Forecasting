#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  7 11:36:00 2018

@author: akumler

This is the second version of the solar forecasting application for GridAPPS-D.
Making some improvements to the code to handle special cases.
"""

# Import packages
import pandas as pd
import numpy as np
from math import *
from datetime import datetime
import math
from pvlib.solarposition import *
from pvlib.atmosphere import *
from pvlib.clearsky import *
from pvlib.irradiance import *
from sklearn.metrics import *
from scipy import stats, integrate
from pspi_simple_v2 import *
import time

"""
Code used to listen and get GHI observation goes here. It will listen for a
date and time, along with a GHI observation. If one desires to use more input,
that can easily be coded in.

Time is in epoch. Timezone is eventually needed, and can be changed in the code.
"""

# How to plan for a 'None' in the middle of the day?
# JSON input goes somewhere here.
ghi_obs = 200
# date_time = pd.datetime(2013, 8, 17, 5, 22)
# df['time'] = pd.to_datetime(df['time'],unit='m')
epoch_time = int(time.time())
current_date = pd.to_datetime(epoch_time, unit='s').round('min')


# current_time = pd.Timestamp.now(tz='US/Mountain').round('min')
# date_time = pd.DatetimeIndex()

def the_forecast(ghi_obs, current_date):
    """ Atmospheric constants (can be replaced by observed values) that are mostly
    pertaining to the SRRL site in Golden, Colorado for the year 2013. If another
    site is desired, seperate analysis should be done for that site to find the
    appropriate constant.
    ghi_obs float ghi
    date
    """
    # Atmospheric Ozone concentraiton
    Ozone_cm = 0.3
    # Precipitable water
    H20_cm = 1.07
    # Aerosol optical depth @ 500nm
    AOD500nm = 0.0823
    # Aerosol optical depth @ 380nm
    AOD380nm = 0.1
    # Approximate broadband aerosol optical depth
    Taua = 0.05
    # Asymmetry factor
    Ba = 0.86
    # Surface albedo
    a_s = 0.2
    # Phase function?
    b = 0.5 - (0.5 * 0.86)
    # Site pressure (not sea-level corrected, pascals)
    pressure = 82000
    # Altitude (meters)
    altitude = 1829.0
    # Latitude of site
    lat = 39.742
    # Longitude of site
    lon = -105.18
    # Time Zone
    tz = 'US/Mountain'
    # Clear-sky transmittance. This value is according to the value used in
    # Kumler et al. 2018. The code to compute it exists in 'pspi_module.py', and
    # can be changed, but it is currently commented out.
    clear_transmit = 0.78904

    """
    First, we much check sunrise and sunset. If set, produced a 0 GHI forecast.
    In addition, we have to check for special cases where a forecast can't be 
    made, or it problems with the platform occur that the application can handle
    that and continue forecasting if necessary.
    """

    global previous_obs
    try:
        ghi_obs
    except IndexError:
        print('Got a non-numerical GHI observation. Assuming persistent observation')
        ghi_obs = previous_obs

    # Get a valid time, whether or not we are given one
    valid_time = valid_datetime(current_date)

    rise_set = get_sun_rise_set_transit(valid_time, lat, lon)
    rise_set.reset_index(inplace=True, drop=True)
    sunrise = rise_set['sunrise']
    sunset = rise_set['sunset']

    to_forecast = time_to_forecast(sunrise, sunset, valid_time, timezone=tz)
    if (to_forecast == True):
        print('Making forecast')
    elif (to_forecast == False):
        final_ghi_forecast = 0
        print('Sun is not up. No forecast needed')
        return final_ghi_forecast
    # Get the last valid GHI value
    global latest_ghi
    try:
        latest_ghi
    except NameError:
        print('Not yet defined. This is probably the first run of the day.')
        # Could just return an empty 'final_ghi_forecast', or just have it have
        # one value, which is None or 0.
        latest_ghi = None
    else:
        lastest_ghi = last_valid_ghi(ghi_obs=ghi_obs)
    # Get a valid ghi observation, whether we are given one or not
    obs = pd.Series(ghi_obs)
    valid_obs = valid_ghi(ghi_obs=obs, latest_ghi=latest_ghi)

    # In the original solar forecasting code, the last 15 minutes were removed
    # due to possible shading of the pyranometer. This was at the SRRL site, so
    # this may not happen everywhere. Because of this, it will not be included
    # in this first version of the PSPI application.

    # Now that we have a valid date time and ghi observation, we can begin to
    # construct the forecast. First we obtain the solar zenith angle (SZA) and
    # extraterrestrial radiation.
    valid_time_tz = valid_time.tz_localize(tz='MST')
    sza_data = spa_python(valid_time, lat, lon, altitude)
    sza_valid = valid_sza_data(sza_data)
    doy_fraction = valid_time_tz.dayofyear

    if (math.isnan(sza_valid['elevation'].iloc[0]) == True):
        final_forecast = 0.0
        final_forecast = pd.Series(final_forecast, index=valid_time)
        return final_forecast
    else:
        # If the solar zenith angle is greater than 87 degrees, the forecast is not
        # computed due to sun proximity on horizon, and thus an erroneous forecast.
        apparent_zenith = sza_valid['apparent_zenith'].copy()
        zenith = sza_valid['zenith'].copy()
        apparent_elev = sza_valid['apparent_elevation'].copy()

    ext_data = get_extra_radiation(valid_time, epoch_year=valid_time.year, method='nrel', solar_constant=1366.1)

    # Run the DISC model to get direct normal irradiance (DNI)
    # Produces DNI, kt (clearness index), and an airmass value
    #    dni = disc(valid_obs[0], zenith[0], valid_time, pressure)
    dni = erbs(valid_obs[0], zenith[0], doy_fraction)
    actual_dni = dni['dni']

    # We now have the information to setup the clear-sky GHI model. This is the
    # final step before getting the actual forecast.

    # Calculate relative and absolute airmass
    ghi_r_airmass = get_relative_airmass(apparent_zenith, model='kasten1966')
    ghi_a_airmass = get_absolute_airmass(ghi_r_airmass, pressure=pressure)

    # Alternate way to calculate Linke turbidity
    bird_aod = bird_hulstrom80_aod_bb(aod380=AOD380nm, aod500=AOD500nm)
    kasten_linke2 = kasten96_lt(ghi_a_airmass, precipitable_water=1.07, aod_bb=bird_aod)

    # Ineichen-Perez clear-sky GHI model
    cs_ineichen_perez = ineichen(apparent_zenith, airmass_absolute=ghi_a_airmass, linke_turbidity=kasten_linke2,
                                 altitude=altitude, dni_extra=ext_data)
    #cs_ineichen_perez['direct_horizontal'] = cs_ineichen_perez['dni'] * np.cos(np.radians(apparent_zenith))

    clearsky_ghi = cs_ineichen_perez['ghi']
    clearsky_dni = cs_ineichen_perez['dni']

    # Calculate future solar zenith angle
    # Need to calculate a future SZA.
    future_df = future_data(valid_time, apparent_zenith=apparent_zenith, lat=lat,
                            lon=lon, altitude=altitude, aod380=AOD380nm,
                            aod500=AOD380nm, precipitable_water=H20_cm,
                            ozone=Ozone_cm, pressure=pressure, asymmetry=Ba,
                            albedo=a_s)

    """
    Just about time to make the forecast.
    """

    final_ghi_forecast = ghi_forecast(valid_time, ghi_obs=valid_obs, cs_transmit=clear_transmit,
                                      clearsky_ghi=clearsky_ghi, clearsky_dni=clearsky_dni, dni=actual_dni,
                                      zenith=apparent_zenith, future_zenith=future_df['Future_Apparent_SZA'],
                                      future_cs_ghi=future_df['Future_Clearsky_GHI'],
                                      future_time=future_df['Future_Time'],
                                      albedo=a_s)
    return final_ghi_forecast


# ghi_forecast_final = the_forecast(ghi_obs, time=current_date)
#
# print(str(ghi_forecast_final[0]))

if __name__ == '__main__':
    ghi_obs = 200
    epoch_time = 1357140600
    current_date = pd.to_datetime(epoch_time, unit='s').round('min')
    print(current_date)
    ghi_forecast_final = the_forecast(ghi_obs, current_date)
    print(str(ghi_forecast_final[0]))


    ghi_obs = 200
    # date_time = pd.datetime(2013, 8, 17, 5, 22)
    # df['time'] = pd.to_datetime(df['time'],unit='m')
    epoch_time = int(time.time())
    current_date = pd.to_datetime(epoch_time, unit='s').round('min')
    ghi_forecast_final = the_forecast(ghi_obs, current_date)
    print(str(ghi_forecast_final[0]))


    ghi_obs = 200
    # date_time = pd.datetime(2013, 8, 17, 5, 22)
    # df['time'] = pd.to_datetime(df['time'],unit='m')
    epoch_time = int(time.time()) + 60
    print(epoch_time)
    current_date = pd.to_datetime(epoch_time, unit='s').round('min')
    print(current_date)
    ghi_forecast_final = the_forecast(ghi_obs, current_date)
    print(str(ghi_forecast_final[0]))

    ghi_obs = 200
    # date_time = pd.datetime(2013, 8, 17, 5, 22)
    # df['time'] = pd.to_datetime(df['time'],unit='m')
    epoch_time = int(time.time()) + 3600
    # 1543859977
    epoch_time = 1357048800 + 3600 * 2
    epoch_time = 1357115400
    current_date = pd.to_datetime(epoch_time, unit='s').round('min')
    print(current_date)
    ghi_forecast_final = the_forecast(ghi_obs, current_date)
    print(str(ghi_forecast_final[0]))

