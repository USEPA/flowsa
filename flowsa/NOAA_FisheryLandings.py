# write_NOAA_fisheries_from_csv.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""
This script is run on it's own, not through flowbyactivity.py, as data pulled from csv in flowsa.

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

from flowsa.common import *
import pandas as pd
from flowsa.flowbyactivity import store_flowbyactivity
from flowsa.flowbyfunctions import add_missing_flow_by_fields


# 2012--2018 fisheries data at state level
csv_load = datapath + "foss_landings.csv"


if __name__ == '__main__':
    # Read directly into a pandas df
    df_raw = pd.read_csv(csv_load)

    # read state fips from common.py
    df_state = get_state_FIPS()
    df_state['State'] = df_state["State"].str.lower()

    # modify fish state names to match those from common
    df = df_raw.drop('Sum Pounds', axis=1)
    df['State'] = df["State"].str.lower()

    # noaa differentiates between florida east and west, which is not necessary for our purposes
    df['State'] = df['State'].str.replace(r'-east', '')
    df['State'] = df['State'].str.replace(r'-west', '')

    # sum florida data after casting rows as numeric
    df['Sum Dollars'] = df['Sum Dollars'].str.replace(r',', '')
    df["Sum Dollars"] = df["Sum Dollars"].apply(pd.to_numeric)
    df2 = df.groupby(['Year', 'State'], as_index=False)[["Sum Dollars"]].agg("sum")

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
    df['LocationSystem'] = "FIPS_2018" # state FIPS codes have not changed over last decade
    df4["Unit"] = "$"
    df4["ActivityProducedBy"] = "All Species"

    # add missing dataframe fields (also converts columns to desired datatype)
    flow_df = add_missing_flow_by_fields(df4, flow_by_activity_fields)
    parquet_name = 'NOAA_FisheryLandings_2012-2018'
    store_flowbyactivity(flow_df, parquet_name)
