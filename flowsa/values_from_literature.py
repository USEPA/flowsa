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


