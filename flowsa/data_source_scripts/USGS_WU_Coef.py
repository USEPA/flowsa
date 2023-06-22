# USGS_WU_Coef.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Animal Water Use coefficients data obtained from: USGS Publication
(Lovelace, 2005)
https://pubs.er.usgs.gov/publication/sir20095041

Data output manually saved as csv, "data/external_data/USGS_WU_Coef_Raw.csv"
"""

import pandas as pd
from flowsa.location import US_FIPS
from flowsa.settings import externaldatapath
from flowsa.flowbyfunctions import assign_fips_location_system


def usgs_coef_parse(*, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # Read directly into a pandas df
    df_raw = pd.read_csv(externaldatapath / "USGS_WU_Coef_Raw.csv")

    # rename columns to match flowbyactivity format
    df = df_raw.rename(columns={"Animal Type": "ActivityConsumedBy",
                                "WUC_Median": "FlowAmount",
                                "WUC_Minimum": "Min",
                                "WUC_Maximum": "Max"
                                })

    # drop columns
    df = df.drop(columns=["WUC_25th_Percentile", "WUC_75th_Percentile"])

    # hardcode data
    df["Class"] = "Water"
    df["SourceName"] = "USGS_WU_Coef"
    df["Location"] = US_FIPS
    df['Year'] = year
    df = assign_fips_location_system(df, '2005')
    df["Unit"] = "gallons/animal/day"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df
