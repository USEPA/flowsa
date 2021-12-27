# NOAA_FisheryLandings.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
NOAA fisheries data obtained from: https://foss.nmfs.noaa.gov/apexfoss/f?p=215:200
                               on: April 28, 2020

Parameters used to select data:
Landings
Data set = Commercial
Year = 2012 --2018
Region Type = States
State Landed = Select All
Species = "ALL SPECIES"

Report Type: "7. Landings by States"

Data output saved as csv, retaining assigned file name "foss_landings.csv"
"""

import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.common import get_state_FIPS
from flowsa.settings import externaldatapath


def noaa_parse(dataframe_list, args):
    """
    Combine, parse, and format the provided dataframes
    :param dataframe_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """
    # Read directly into a pandas df
    df_raw = pd.read_csv(externaldatapath + "foss_landings.csv")

    # read state fips from common.py
    df_state = get_state_FIPS().reset_index(drop=True)
    df_state['State'] = df_state["State"].str.lower()

    # modify fish state names to match those from common
    df = df_raw.drop('Sum Pounds', axis=1)
    df['State'] = df["State"].str.lower()

    # filter by year
    df = df[df['Year'] == int(args['year'])]
    # noaa differentiates between florida east and west, which is not necessary for our purposes
    df['State'] = df['State'].str.replace(r'-east', '')
    df['State'] = df['State'].str.replace(r'-west', '')

    # sum florida data after casting rows as numeric
    df['Sum Dollars'] = df['Sum Dollars'].str.replace(r',', '')
    df["Sum Dollars"] = df["Sum Dollars"].apply(pd.to_numeric)
    df2 = df.groupby(['Year', 'State'], as_index=False).agg({"Sum Dollars": sum})

    # new column includes state fips
    df3 = df2.merge(df_state[["State", "FIPS"]], how="left", left_on="State", right_on="State")

    # data includes "process at sea", which is not associated with any fips, assign value of '99'
    # if fips is nan, add the state name to description and drop state name
    df3['Description'] = None
    df3.loc[df3['State'] == 'process at sea', 'Description'] = df3['State']
    df3.loc[df3['State'] == 'process at sea', 'FIPS'] = 99
    df4 = df3.drop('State', axis=1)

    # rename columns to match flowbyactivity format
    df4 = df4.rename(columns={"Sum Dollars": "FlowAmount",
                              "FIPS": "Location"})

    # hardcode data
    df4["Class"] = "Money"
    df4["SourceName"] = "NOAA_Landings"
    df4["FlowName"] = None
    df4 = assign_fips_location_system(df4, args['year'])
    df4["Unit"] = "$"
    df4["ActivityProducedBy"] = "All Species"
    df4['DataReliability'] = 5  # tmp
    df4['DataCollection'] = 5  #tmp

    return df4
