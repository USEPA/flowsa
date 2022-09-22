# -*- coding: utf-8 -*-
"""
EPA REI
"""
import pandas as pd
from flowsa.location import US_FIPS


def rei_call(*, url, **_):
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
    # df for primary factors
    df = pd.concat(df_list, sort=False)
    df.iloc[0, 0] = 'Description'
    df.iloc[0, 1] = 'ActivityProducedBy'
    df = df.rename(columns=df.iloc[0]).drop(df.index[0]).reset_index(drop=True)

    # use "melt" fxn to convert columns into rows
    df = df.melt(id_vars=["Description", "ActivityProducedBy"],
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
    df['DataReliability'] = 5
    df['DataCollection'] = 5

    return df


def waste_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year of FBS
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # df for primary factors
    df = pd.concat(df_list, sort=False)
    df.iloc[0, 0] = 'Description'
    df.iloc[0, 1] = 'FlowName'
    df = df.rename(columns=df.iloc[0]).drop(df.index[0]).reset_index(drop=True)

    df = (df
          .drop(columns=df.columns[[2]])
          .drop([0, 1])  # Drop blanks
          .melt(id_vars=['Description', 'FlowName'],
                var_name='ActivityConsumedBy',
                value_name='FlowAmount')
          )
    df = df[~df['FlowAmount'].str.contains('-')].reset_index(
        drop=True)
    df['FlowAmount'] = (df['FlowAmount'].str.replace(
        ',', '').astype('float'))
    df['Unit'] = 'USD'
    # Where recyclable code >= 9 (Gleaned product), change units to MT
    df.loc[df['Description'].str[-2:].astype('int') >= 9,
           'Unit'] = 'MT'
    df['FlowName'] = df['FlowName'].apply(
        lambda x: x.split('(', 1)[0])

    # hardcode info
    df['Location'] = US_FIPS
    df['SourceName'] = 'EPA_REI_W'
    df["Class"] = "Other"
    df['FlowType'] = 'WASTE_FLOW'
    df["Year"] = year
    df['DataReliability'] = 5
    df['DataCollection'] = 5

    return df
