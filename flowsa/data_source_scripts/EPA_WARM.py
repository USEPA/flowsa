# -*- coding: utf-8 -*-
"""
EPA WARM
"""
import pandas as pd
from flowsa.location import US_FIPS


def warm_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df = pd.read_csv('https://raw.githubusercontent.com/USEPA/WARMer/main'
                     '/warmer/data/flowsa_inputs/WARMv15_env.csv')

    return df


def warm_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year of FBS
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat list of dataframes (info on each page)
    df = pd.concat(df_list, sort=False)
    # rename columns and reset data to FBA format
    df = df.rename(columns={'ProcessName': 'ActivityConsumedBy',
                            'Flowable': 'FlowName',
                            'Context': 'Compartment'}
                   ).drop(columns=['ProcessID', 'FlowUUID', 'ProcessCategory'])
    df['Compartment'] = df['Compartment'].fillna('')
    df['Location'] = df['Location'].replace('US', US_FIPS)

    # add new column info
    df['SourceName'] = 'EPA_WARMer'
    df["Class"] = "Other"
    df['FlowType'] = "WASTE_FLOW"
    df["Year"] = year
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df
