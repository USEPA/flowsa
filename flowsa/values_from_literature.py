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

