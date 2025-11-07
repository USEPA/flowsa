# literature_values.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Values from the literature used for data allocation are
specified here and can be called on using functions.
"""

import pandas as pd
import numpy as np
from flowsa.settings import datapath


def get_Canadian_to_USD_exchange_rate(year):
    """
    Return exchange rate (Canadian $/USD)
    From https://www.federalreserve.gov/releases/h10/current/ on 10/28/2025
    :param year: str, year of exchange rate to return
    :return: number, value of exchange rate for year
    """
    er = ({2000: 1.4855,
           2001: 1.5487,
           2002: 1.5704,
           2003: 1.4008,
           2004: 1.3017,
           2005: 1.2115,
           2006: 1.134,
           2007: 1.0734,
           2008: 1.066,
           2009: 1.1412,
           2010: 1.0298,
           2011: 0.9887,
           2012: 0.9995,
           2013: 1.03,
           2014: 1.1043,
           2015: 1.2791,
           2016: 1.3243,
           2017: 1.2984,
           2018: 1.2957,
           2019: 1.3269,
           2020: 1.3422,
           2021: 1.2533,
           2022: 1.3014,
           2023: 1.3494,
           2024: 1.3699
           })

    exchange_rate = er.get(year, np.nan)
    return exchange_rate


def get_area_of_urban_land_occupied_by_houses_2013():
    """
    Reported area of urban land occupied by houses in 2013 from the USDA
    ERS Major Land Uses Report

    :return: number, the area of land occupied by houses in square meters
    """

    acres_to_sq_m_conversion = 4046.86
    # value originally reported in million acres
    area_urban_residence = 32.8

    # convert to square meters
    area_urban_residence = \
        area_urban_residence * (10 ** 6) * acres_to_sq_m_conversion

    return area_urban_residence


def get_area_of_rural_land_occupied_by_houses_2013():
    """
    Reported area of urban land occupied by houses in 2013 from the
    USDA ERS Major Land Uses Report

    :return: number, the area of land occupied by rural houses in square meters
    """

    acres_to_sq_m_conversion = 4046.86
    # value originally reported in million acres
    area_rural_residence = 106.3
    # convert to square meters
    area_rural_residence = \
        area_rural_residence * 1000000 * acres_to_sq_m_conversion

    return area_rural_residence


def get_commercial_and_manufacturing_floorspace_to_land_area_ratio():
    """
    The additional land area associated with commercial and
    manufacturing buildings (parking, sinage, landscaping)

    Based on original USEEIO assumption
    :return: number, ratio of land area to total floorspace assumption
    """

    floor_space_to_land_area_ratio = 0.25

    return floor_space_to_land_area_ratio


def get_open_space_fraction_of_urban_area():
    """
    Assumption on the fraction of urban areas that is open space

    Based on Lin Zeng's 2020 paper
    :return: number, fraction of open space in urban areas
    """

    value = 0.1

    return value


def get_urban_land_use_for_airports():
    """
    Based on Lin Zeng's 2020 paper
    :return: number, fraction of land used for airports
    """

    value = 0.05

    return value


def get_urban_land_use_for_railroads():
    """
    Based on Lin Zeng's 2020 paper
    :return: number, fraction of land used for railroads
    """

    value = 0.05

    return value


def get_fraction_of_urban_local_road_area_for_parking():
    """
    Based on Lin Zeng's 2020 paper
    :return: number, fraction of road area used for parking
    """

    value = 0.25

    return value


def get_transportation_sectors_based_on_FHA_fees():
    """
    Values from https://www.fhwa.dot.gov/policy/hcas/addendum.cfm
    Website accessed 11/02/2020
    Data from 1997

    :return: dictionary, fraction of fha fees by transportation type
    """
    fha_dict = ({'Truck transportation':
                     {'NAICS_2012_Code': '484', 'ShareOfFees': 0.329},
                 'Transit and ground passenger transportation':
                     {'NAICS_2012_Code': '485', 'ShareOfFees': 0.001},
                 'State and local government passenger transit': {
                     'NAICS_2012_Code': 'S00201',
                     'ShareOfFees': 0.001},
                 'Personal consumption expenditures': {
                     'NAICS_2012_Code': 'F01000',
                     'ShareOfFees': 0.669}
                 })

    return fha_dict
