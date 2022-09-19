# -*- coding: utf-8 -*-
"""
EPA REI
"""
import pandas as pd
from flowsa.location import US_FIPS
import re


def primary_factors_call(*, url, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df = pd.read_csv(url)

    return df


def primary_factors_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year of FBS
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat list of dataframes
    df = pd.concat(df_list, sort=False)
    df.iloc[0, 0] = 'Description'
    df.iloc[0, 1] = 'Activity'
    df = df.rename(columns=df.iloc[0]).drop(df.index[0]).reset_index(drop=True)

    # use "melt" fxn to convert columns into rows
    df = df.melt(id_vars=["Description", "Activity"],
                 var_name="FlowName",
                 value_name="FlowAmount")

    # hardcode info
    df['Location'] = US_FIPS
    df['SourceName'] = 'EPA_REI_PF'
    df["Class"] = "Money"
    df.loc[df['FlowName'] == 'Employment', 'Class'] = 'Employment'
    df['Unit'] = 'Thousand USD'
    df.loc[df['FlowName'] == 'Employment', 'Unit'] = 'p'
    df["Year"] = year
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df
