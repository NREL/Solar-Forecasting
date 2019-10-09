#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  7 11:42:15 2018

@author: akumler

This script contains all the hard code that produces the solar forecast. The
solar forecation application imports this module to get desired data.

This is the second version, removing the requirement that a previous observation
is needed in order to make the forecast (reconstruction). Other improvements
are added.
"""

import pandas as pd
import numpy as np
from math import *
from datetime import datetime
import math
from pvlib.solarposition import *
from pvlib.atmosphere import *
from pvlib.clearsky import *
from pvlib.irradiance import *
# from bird_clear_sky_model import *
from sklearn.metrics import *
# import seaborn as sns;
#
# sns.set()
# import skill_metrics as sm
from scipy import stats
from datetime import datetime
import time
from time import strptime, strftime, mktime, gmtime
from calendar import timegm


def valid_datetime(date_time):
    """
    Checks to make sure the datetime received from the platform is valid.
    Parameters
    ----------
    date_time: 'Pandas DatetimeIndex'
        Current time. Usually a contains a year, month, day, hour, and minute.

    Returns
    -------
    valid_time: 'datetimeindex'
        Current time. If 'valid_datetime' receives an invalid input, one is
        assumed from the previous valid time given.
    """
    if (date_time is None):
        valid_time = np.array([pd.Timestamp.now()])
        valid_time = pd.DatetimeIndex(valid_time).round('min')
        return valid_time
    elif (isinstance(date_time, datetime) == True):
        valid_time = np.array([pd.to_datetime(date_time)])
        valid_time = pd.DatetimeIndex(valid_time)
        return valid_time
    elif (isinstance(date_time, pd.DatetimeIndex) == True):
        return date_time


def get_cs_transmit(zenith, airmass_relative, aod380, aod500, precipitable_water,
                    ozone=0.3, pressure=101325., dni_extra=1364., asymmetry=0.85,
                    albedo=0.2):
    """
    Calculats clear-sky transmittance to be used in the application.
    Parameters
    ----------
    zenith: 'Numpy array'
        A fake SZA array to calaculate transmittance.
    airmass_relative: 'Numpy array'
        A fake airmass to calculate transmittance.
    aod380: 'Float'
        Aerosol optical depth @ 380 nm.
    aod500: 'Float'
        Aerosol optical depth @ 500 nm.
    precipitable_water: 'Float'
        Annual average precipitable water for SRRL. Units: cm.
    ozone: 'Float'
        Annual average ozone concentration for SRRL. Units: cm.
    pressure: 'Float'
        Avearage sea-level pressure. Units: Pa.
    dni_extra: 'Float'
        Aveage extraterrestrial @ TOA. Units: W/m^2
    asymmetry: 'Float'
        Asymmetry parameter
    albedo: 'Float'
        Surface albedo.

    Returns
    -------
    irrads: 'Ordered Dictionary'
        Contains clear-sky GHI, DNI, DHI, and transmittance. Really only the
        transmittance is used.
    """
    etr = dni_extra  # extraradiation
    ze_rad = np.deg2rad(zenith)  # zenith in radians
    airmass = airmass_relative
    # Bird clear sky model
    am_press = atmosphere.absoluteairmass(airmass, pressure)
    t_rayleigh = (
        np.exp(-0.0903 * am_press ** 0.84 * (
                1.0 + am_press - am_press ** 1.01
        ))
    )
    am_o3 = ozone * airmass
    t_ozone = (
            1.0 - 0.1611 * am_o3 * (1.0 + 139.48 * am_o3) ** -0.3034 -
            0.002730 * am_o3 / (1.0 + 0.044 * am_o3 + 0.0003 * am_o3 ** 2.0)
    )
    t_gases = np.exp(-0.0127 * am_press ** 0.26)
    am_h2o = airmass * precipitable_water
    t_water = (
            1.0 - 2.4959 * am_h2o / (
            (1.0 + 79.034 * am_h2o) ** 0.6828 + 6.385 * am_h2o
    )
    )
    bird_huldstrom = atmosphere.bird_hulstrom80_aod_bb(aod380, aod500)
    t_aerosol = np.exp(
        -(bird_huldstrom ** 0.873) *
        (1.0 + bird_huldstrom - bird_huldstrom ** 0.7088) * airmass ** 0.9108
    )
    taa = 1.0 - 0.1 * (1.0 - airmass + airmass ** 1.06) * (1.0 - t_aerosol)
    rs = 0.0685 + (1.0 - asymmetry) * (1.0 - t_aerosol / taa)
    id_ = 0.9662 * etr * t_aerosol * t_water * t_gases * t_ozone * t_rayleigh
    ze_cos = np.where(zenith < 90, np.cos(ze_rad), 0.0)
    id_nh = id_ * ze_cos
    ias = (
            etr * ze_cos * 0.79 * t_ozone * t_gases * t_water * taa *
            (0.5 * (1.0 - t_rayleigh) + asymmetry * (1.0 - (t_aerosol / taa))) / (
                    1.0 - airmass + airmass ** 1.02
            )
    )
    gh = (id_nh + ias) / (1.0 - albedo * rs)
    diffuse_horiz = gh - id_nh
    transmit = t_aerosol * t_water * t_gases * t_ozone * t_rayleigh
    # TODO: be DRY, use decorator to wrap methods that need to return either
    # OrderedDict or DataFrame instead of repeating this boilerplate code
    irrads = OrderedDict()
    irrads['direct_horizontal'] = id_nh
    irrads['ghi'] = gh
    irrads['dni'] = id_
    irrads['dhi'] = diffuse_horiz
    irrads['clear_transmit'] = transmit
    if isinstance(irrads['dni'], pd.Series):
        irrads = pd.DataFrame.from_dict(irrads)
    return irrads


def time_to_forecast(sunrise, sunset, valid_time, timezone):
    """
    Checks to see if the sun is up so that a GHI forecast can be made.
    Parameters
    ----------
    sunrise: 'Pandas DatetimeIndex'
        Sunrise for this particular day, created using the 'get_sun_rise_set_transit'
        module in PVlib.
    sunset: 'Pandas DatetimeIndex'
        Sunset for this particular day, created using the 'get_sun_rise_set_transit'
        module in PVlib.
    valid_time: 'datetimeindex'
        Current time.
    timezone: 'tz'
        Timezone of current location.
    Returns
    -------
    to_forecast: 'Boolean'
        Boolean True or False. True means a forecast can be made. False means
        the sun is still set, and that a forecast should not be made.
    """
    # Is it DST?
    dst = time.localtime().tm_isdst
    #    current_tz = datetime.strftime("%z", gmtime())
    # Adjust for timezone.
    if (dst == 0):
        adj_sunrise = pd.DatetimeIndex(sunrise).tz_localize(timezone) - pd.Timedelta(hours=1)
        adj_sunset = pd.DatetimeIndex(sunset).tz_localize(timezone) - pd.Timedelta(hours=1)
        #        adj_time = pd.DatetimeIndex(valid_time).tz_localize(strftime("%z", gmtime()))
        adj_time = valid_time.tz_localize(timezone) - pd.Timedelta(hours=1)
    else:
        adj_sunrise = pd.DatetimeIndex(sunrise).tz_localize(timezone)
        adj_sunset = pd.DatetimeIndex(sunset).tz_localize(timezone)
        adj_time = valid_time.tz_localize(timezone)
    print(adj_sunrise,adj_time, adj_sunset)
    if (adj_sunrise <= adj_time < adj_sunset):
        to_forecast = True
    else:
        to_forecast = False

    return to_forecast


def valid_ghi(ghi_obs, latest_ghi):
    """
    Checks to make sure the GHI observation received from the platfrom is
    valid.
    Parameters
    ----------
    ghi_obs: 'Pandas Series object'
        Current GHI observation. Units: W/m^2

    Returns
    -------
    valid_ghi: 'Pandas series object'
        Current GHI observation. If 'valid_ghi' receives and invalid input,
        one is calculated using the persistence model and a valid datetimeindex.
    """
    if (ghi_obs is None):
        # Assume the persistence mdoel
        ghi_obs = persistence_model(latest_ghi)
    elif (isinstance(ghi_obs, pd.Series) == True):
        return ghi_obs


def persistence_model(latest_ghi):
    """
    Creates a persistence forecast when the software fails to receive a valid
    GHI observation. Can also be used if one simply desires a persistence
    forecast.
    Parameters
    ----------
    latest_ghi: 'Pandas Series object'
        Current time. Usually contains a year, month, day, hour, and minute.

    Returns
    -------
    persist_ghi: 'Pandas Series object'
        Persistence forecast for inputed date time.
    """
    persist_ghi = latest_ghi.copy()
    return persist_ghi


def last_valid_ghi(ghi_obs):
    """
    Saves the last valid GHI observation. Uses can vary, but the important one
    is that it can be used in case the software fails to receive a valid GHI
    observation.
    Parameters
    ----------
    ghi_obs: 'Pandas Series object'
        Current GHI observation. Units: W/m^2

    Returns
    -------
    latest_ghi: 'Pandas Series object'
        Latest GHI observation saved. Unites: W/m^2
    """
    latest_ghi = ghi_obs
    return latest_ghi


def valid_sza_data(sza_data):
    """
    Checkes to see if the SPA data is valid. This mainly concerns the SZA, and
    if it is greater than 87 degress, return np.nan, and thus no forecast.
    Parameters
    ----------
    sza_data: 'Pandas DataFrame object'

    Returns
    -------
    sza_valid: 'Pandas DataFrame object'
        Valid SPA data. If the solar zenith angle is greater than 87 degrees,
        then a np.nan is returned, and no forecast is generated.
    """
    sza_valid = []
    if (sza_data['elevation'].iloc[0] < 7):
        sza_data['elevation'].iloc[0] = np.nan
        sza_valid = sza_data.copy()
    else:
        sza_valid = sza_data.copy()
    
    return sza_valid


def future_data(valid_time, apparent_zenith, lat, lon, altitude, aod380,
                aod500, precipitable_water, ozone, pressure, asymmetry, albedo):
    """
    Calculates the necessary variables for the future time period, so that a
    GHI forecast can be made.
    Parameters
    ----------
    valid_time: 'Pandas DatetimeIndex'
        Current time.
    apparent_zenith: 'Pandas Series object'
        Apparent solar zenith angle generated by PVlib. Units: degrees
    lat: 'float'
        Latitude of site
    lon: 'float
        Longitude of site
    altitude: 'float'
        Altitude of site. Units: m

    Returns
    -------
    future_apparent_sza: 'Pandas Series object'
        Apparent solar zenith angle in the future time period: Units: degrees
    future_clearsky_ghi: 'Pandas Series object'
        Future clear-sky GHI. Unites: W/m^2
    """
    # Calculate future solar zenith angle
    # Need to calculate a future SZA.
    future_time = valid_time + pd.DateOffset(minutes=30)
    sza_data_future = spa_python(future_time, lat, lon, altitude)
    future_apparent_sza = valid_sza_data(sza_data_future)
    future_apparent_sza = sza_data_future['apparent_zenith']

    # Future DNI
    future_ext = get_extra_radiation(future_time, epoch_year=future_time.year, method='nrel', solar_constant=1366.1)
    future_ext = pd.Series(future_ext)

    # Calculate relative and absolute airmass
    future_airmass = get_relative_airmass(future_apparent_sza, model='kasten1966')
    ghi_a_airmass = get_absolute_airmass(future_airmass, pressure=pressure)
    # Alternate way to calculate Linke turbidity
    bird_aod = bird_hulstrom80_aod_bb(aod380=aod380, aod500=aod500)
    kasten_linke2 = kasten96_lt(ghi_a_airmass, precipitable_water=precipitable_water, aod_bb=bird_aod)

    # Calculate future clear-sky GHI
    # Bird Clear-sky GHI model
    cs_ineichen_perez = ineichen(future_apparent_sza, airmass_absolute=ghi_a_airmass, linke_turbidity=kasten_linke2,
                                 altitude=altitude, dni_extra=future_ext)
    #cs_ineichen_perez['direct_horizontal'] = cs_ineichen_perez['dni'] * np.cos(np.radians(future_apparent_sza))

    future_clearsky_ghi = cs_ineichen_perez['ghi']

    # Convert the time variables into Pandas Series objects
    future_time = pd.Series(future_time)
    future_time.index = future_clearsky_ghi.index

    # Gather all the data into one dataframe. May have to play with data formats
    # a bit to get everything to work.
    future_df = pd.concat([future_apparent_sza, future_clearsky_ghi, future_time], axis=1)
    future_df.columns = ['Future_Apparent_SZA', 'Future_Clearsky_GHI', 'Future_Time']

    return future_df


def ghi_forecast(valid_time, ghi_obs, cs_transmit, clearsky_ghi, clearsky_dni,
                 dni, zenith, future_zenith, future_cs_ghi, future_time,
                 albedo):
    """
    Calculates a GHI forecast based on Xie and Liu 2013.
    Parameters
    ----------
    valid_time: 'Pandas DatetimeIndex'
        Current time.
    valid_ghi: 'Pandas Series object'
        Current GHI observation. Units: W/m^2
    clear_transmit: 'Pandas Series object'
        Clear-sky transmittance.
    clearsky_ghi: 'Pandas Series object'
        Clear-sky GHI generated by the Bird model. Units: W/m^2
    clearsky_dni: 'Pandas Series object'
        Clear-sky DNI generated by the Bird model. Units: W/m^2
    dni: 'Pandas Series object'
        DNI generated by the DISC model. Units: W/m^2
    apparent_zenith: 'Pandas Series object'
        Apparent solar zenith angle generated by PVlib. Units: degrees
    future_zenith: 'Pandas Series object'
        Future apparent solar zenith angle generated by PVlib. Units: degrees
    albedo: 'float'
        Surface albedo
    """
    # Try some data stuff
    ghi_obs = np.array(ghi_obs)
    cs_transmit = np.array(cs_transmit)
    clearsky_ghi = np.array(clearsky_ghi)
    clearsky_dni = np.array(clearsky_dni)
    dni = np.array(dni)
    zenith = np.array(zenith)
    future_zenith = np.array(future_zenith)
    future_cs_ghi = np.array(future_cs_ghi)
    # Finish transmittance calculation
    transmit = cs_transmit ** 2

    # Upwelling shortwave radiation
    ghi_up = albedo * ghi_obs

    # It is now possible to calculate B1 and B2
    B1 = (clearsky_ghi - ghi_obs) / (clearsky_ghi - ghi_up * transmit)

    B2 = (clearsky_dni - dni) / clearsky_dni

    b_final = B1 / B2

    # In order to continue, and calculate cloud fraction and GHI, we have to
    # compute cloud albedo. These values change depending on the values of B1
    # and B2. Thus, an if else statement if appropriate.

    # Initial GHI is no longer computed, as it isn't needed anymore for the forecast.
    # It was initially used for the reconstructed ratios.
    if (0 <= abs(b_final) <= 0.07):
        cloud_albedo = 0
        cloud_fraction = 0
    elif (0.07 < b_final < 0.07872):
        cloud_albedo = 0
        cloud_fraction = 0
    elif (0.07872 <= b_final <= 0.11442):
        cloud_albedo = 1 - 31.1648 * (b_final) + np.sqrt(((31.1648 * (b_final)) ** 2 - 49.6255 * (b_final)))
        cloud_fraction = B1 / cloud_albedo
    elif (0.114422 < b_final <= 0.185):
        cloud_albedo = ((2.61224 * B1 - B2 + np.sqrt((24.2004 * B1 ** 2) - (9.0098 * B1 * B2) + B2 ** 2)) /
                        (18.3622 * B1 - 4 * B2))
        cloud_fraction = B1 / cloud_albedo
    elif (0.185 < b_final <= 0.23792):
        cloud_albedo = 0.89412 * (b_final) + 0.02519
        cloud_fraction = B1 / cloud_albedo
    elif (0.23792 < b_final <= 1.0):
        cloud_albedo = b_final
        cloud_fraction = B1 / cloud_albedo
    else:
        cloud_albedo = b_final
        cloud_fraction = B1 / cloud_albedo

    # Now we can calculate cloud optical thickness for the next 30 min
    cloud_fraction_persist = cloud_fraction
    sza_thick = np.cos(np.radians(zenith))
    cloud_thick = (2 * cloud_albedo * sza_thick / ((1 - cloud_albedo) * (1 - 0.86)))
    b = 0.5 - (0.5 * 0.86)

    # Need to calculate a future SZA.
    sza_valid_future = future_zenith
    sza_thick_future = np.cos(np.radians(sza_valid_future))
    cloud_albedo_future = (((b * cloud_thick) / sza_thick_future) / (1 + (b * cloud_thick) / sza_thick_future))
    
    # Set some reasonable limits for cloud albedo and cloud fraction.
    cloud_albedo_future = np.array([cloud_albedo_future])
    cloud_albedo_future[cloud_albedo_future < 0] = 0
    cloud_albedo_future[cloud_albedo_future > 1] = 1
    
    cloud_fraction_persist = np.array([cloud_fraction_persist])
    cloud_fraction_persist[cloud_fraction_persist < 0] = 0
    cloud_fraction_persist[cloud_fraction_persist > 1] = 1

    # Calculate GHI cloud for next time step
    future_clearsky_ghi = future_cs_ghi
    future_cloudysky_ghi = (1 - cloud_albedo_future) * future_clearsky_ghi
    F1f = (cloud_fraction_persist * future_cloudysky_ghi) + ((1 - cloud_fraction_persist) * future_clearsky_ghi)
    ghi_forecast_v3 = F1f * (1 - albedo * cloud_albedo_future * cloud_fraction_persist * transmit) ** -1

    # Return the final forecast
    final_ghi_forecast = ghi_forecast_v3
    final_ghi_forecast = pd.Series(final_ghi_forecast[0], index=future_time)
    
    # If the forecasted GHI is higher than clear-sky GHI, set it equal to clear-sky GHI.
    final_ghi_forecast[final_ghi_forecast > future_clearsky_ghi] = future_clearsky_ghi

    return final_ghi_forecast












