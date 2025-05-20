# NOAA_FisheriesLandings.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
NOAA fisheries data obtained from:
https://foss.nmfs.noaa.gov/apexfoss/f?p=215:200
on: August 10, 2022

Parameters used to select data:
Landings
Data set = Commercial
Year = 2012 - 2021
Region Type = States
State Landed = Select All
Species = "ALL SPECIES"

Report Type: "TOTAL BY YEAR/STATE"

Data output saved as csv, retaining assigned file name
"NOAA_FisheriesLandings.csv"
"""

import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.location import get_state_FIPS
from flowsa.settings import externaldatapath


def noaa_parse(*, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # Read directly into a pandas df
    df_raw = pd.read_csv(externaldatapath / "NOAA_FisheriesLandings.csv")

    # read state fips from common.py
    df_state = get_state_FIPS().reset_index(drop=True)
    df_state['State'] = df_state["State"].str.lower()

    # modify fish state names to match those from common
    df = df_raw.drop('Pounds', axis=1)
    df['State'] = df["State"].str.lower()

    # filter by year
    df = df[df['Year'] == int(year)]
    # noaa differentiates between florida east and west,
    # which is not necessary for our purposes
    df['State'] = df['State'].str.replace(r'-east', '')
    df['State'] = df['State'].str.replace(r'-west', '')

    # sum florida data after casting rows as numeric
    df['Dollars'] = df['Dollars'].str.replace(r',', '')
    df["Dollars"] = df["Dollars"].apply(pd.to_numeric)
    df2 = df.groupby(['Year', 'State'], as_index=False).agg({"Dollars": sum})

    # new column includes state fips
    df3 = df2.merge(df_state[["State", "FIPS"]], how="left",
                    left_on="State", right_on="State")

    # data includes "process at sea", which is not associated with any
    # fips, assign value of '99' if fips is nan, add the state name to
    # description and drop state name
    df3.loc[df3['State'] == 'process at sea', 'Description'] = df3['State']
    df3.loc[df3['State'] == 'process at sea', 'FIPS'] = 99
    df4 = df3.drop('State', axis=1)

    # rename columns to match flowbyactivity format
    df4 = df4.rename(columns={"Dollars": "FlowAmount",
                              "FIPS": "Location"})

    # hardcode data
    df4["Class"] = "Money"
    df4["SourceName"] = "NOAA_FisheriesLandings"
    df4["FlowName"] = "Commercial"
    df4 = assign_fips_location_system(df4, year)
    df4["Unit"] = "USD"
    df4["ActivityProducedBy"] = "All Species"
    df4['DataReliability'] = 5  # tmp
    df4['DataCollection'] = 5  # tmp

    return df4

if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(year='2012-2023', source='NOAA_FisheriesLandings')
    fba = flowsa.getFlowByActivity('NOAA_FisheriesLandings', year=2023)
