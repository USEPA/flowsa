# values_from_literature.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Values from the literature used for data allocation are specified here and can be called on using functions.
"""

import pandas as pd
import numpy as np
from flowsa.common import datapath

def get_US_urban_green_space_and_public_parks_ratio():
    """
    calculates weighted average of urban green space and public parks in national total urban areas
    Based on weighted average of 44 cities based on city population.

    weighted average value = 12.35%

    Larson LR, Jennings V, Cloutier SA (2016) Public Parks and Wellbeing in Urban Areas of the United States.
    PLoS ONE 11(4): e0153211. https://doi.org/10.1371/journal.pone.0153211
    """

    # load Larson's saved SI data
    df = pd.read_csv(datapath + "Larson_UrbanPublicParks_SI.csv")
    # drop columns
    # df = df.drop(columns=['PopChange2010-12', 'LogIncome', 'SinglePercent', 'WorkFulltime', 'CollegeDegree',
    #                       'NaturalAmentiesIndex', 'ParkAccessibility-WalkableAccess2014', 'WBI-Total', 'WBI-Purpose',
    #                       'WBI-Social', 'WBI-Financial', 'WBI-Physical', 'WBI-Community'])

    # calculate a weighted value for ratio of urban land that belongs to parks based on city populations
    # weighted average function
    try:
        wm = lambda x: np.ma.average(x, weights=df.loc[x.index, "CityPop2010"])
    except ZeroDivisionError:
        wm = 0

    # column to weight
    agg_funx = {"ParkPercent-2014": wm}

    # weighted averages as value
    value_series = df.agg(agg_funx)
    value = value_series[0]

    return value


def get_Canadian_to_USD_exchange_rate(year):
    """
    Return exchange rate (Canadian $/USD)
    From https://www.federalreserve.gov/releases/h10/current/ on 09/07/2020
    :param year:
    :return:
    """
    er = ({'2000': '1.4855',
           '2001': '1.5487',
           '2002': '1.5704',
           '2003': '1.4008',
           '2004': '1.3017',
           '2005': '1.2115',
           '2006': '1.134',
           '2007': '1.0734',
           '2008': '1.066',
           '2009': '1.1412',
           '2010': '1.0298',
           '2011': '0.9887',
           '2012': '0.9995',
           '2013': '1.03',
           '2014': '1.1043',
           '2015': '1.2791',
           '2016': '1.3243',
           '2017': '1.2984',
           '2018': '1.2957',
           '2019': '1.3269'
           })

    exchange_rate = er.get(year)
    return exchange_rate


def get_commercial_and_manufacturing_floorspace_to_land_area_ratio():
    """
    The additional land area associated with commercial and manufacturing buildings (parking, sinage, landscaping)

    Based on original USEEIO assumption
    :return: ratio of land area to total floorspace assumption
    """

    floor_space_to_land_area_ratio = 0.25

    return floor_space_to_land_area_ratio


def get_open_space_fraction_of_urban_area():
    """
    Assumption on the fraction of urban areas that is open space

    Based on Lin Zeng's 2020 paper
    :return: fraction of open space in urban areas
    """

    value = 0.1

    return value


def get_urban_land_use_for_airports():
    """
    Based on Lin Zeng's 2020 paper
    :return:
    """

    value = 0.05

    return value


def get_urban_land_use_for_railroads():
    """
    Based on Lin Zeng's 2020 paper
    :return:
    """

    value = 0.05

    return value


def get_fraction_of_urban_local_road_area_for_parking():
    """
    Based on Lin Zeng's 2020 paper
    :return:
    """

    value = 0.25

    return value


def get_transportation_sectors_based_on_FHA_fees():
    """
    Values from https://www.fhwa.dot.gov/policy/hcas/addendum.cfm
    Website accessed 11/02/2020
    Data from 1997

    :return:
    """

    FHA_dict = ({'Truck': 0.329,
                 'Transit and ground passenger transportation': 0.001,
                 'State/local government passenger transit': 0.001,
                 'Personal consumption expenditures': 0.669})
    return FHA_dict
