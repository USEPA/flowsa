# Census_VIP.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
U.S. Census Value of Construction Put in Place
https://www.census.gov/construction/c30/c30index.html
"""
import pandas as pd
import numpy as np
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.location import US_FIPS


def census_vip_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe,
    begin parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    # Convert response to dataframe
    df = pd.read_excel(resp.content,
                       sheet_name='Total',
                       header=3).dropna().reset_index(drop=True)

    df.loc[df['Type of Construction:'].str.startswith("  "),
           'Type of Construction:'] = 'Nonresidential - ' + \
                                      df['Type of Construction:'].str.strip()
    df = df[df['Type of Construction:'] != 'Nonresidential']
    index_1 = np.where(df['Type of Construction:'].str.startswith(
        "Total Private Construction"))[0][0]
    index_2 = np.where(df['Type of Construction:'].str.startswith(
        "Total Public Construction"))[0][0]

    df_private = df.iloc[index_1+1:index_2, :].reset_index(drop=True)
    df_public = df.iloc[index_2+1:, :].reset_index(drop=True)

    df_public['Type of Construction:'] = 'Public, ' + \
                                         df_public['Type of Construction:']
    df_private['Type of Construction:'] = 'Private, ' + \
                                          df_private['Type of Construction:']

    df2 = pd.concat([df_public, df_private], ignore_index=True)

    df2 = df2.melt(id_vars=['Type of Construction:'],
                   var_name='Year',
                   value_name='FlowAmount')

    return df2


def census_vip_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    df = pd.concat(df_list, sort=False)
    df['Year'] = df['Year'].astype(str)
    df = df[df['Year'] == year].reset_index(drop=True)
    df = df.rename(columns={'Type of Construction:': 'ActivityProducedBy'})

    df['Class'] = 'Money'
    df['SourceName'] = 'Census_VIP'
    df['FlowName'] = 'Construction spending'
    # millions of dollars
    df['FlowAmount'] = df['FlowAmount'] * 1000000
    df['Unit'] = 'USD'
    df['FlowType'] = "ELEMENTARY_FLOW"
    df['Compartment'] = None
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, year)
    df['Year'] = year
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5

    return df
