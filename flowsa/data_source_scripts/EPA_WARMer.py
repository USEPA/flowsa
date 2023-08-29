# -*- coding: utf-8 -*-
"""
EPA WARMer
"""
import pandas as pd
from flowsa.location import US_FIPS
import re


def warmer_call(*, url, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df = pd.read_csv(url)

    return df


def warmer_parse(*, df_list, year, **_):
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
    df = df.rename(columns={'ProcessName': 'ActivityProducedBy',
                            'Flowable': 'FlowName',
                            'Context': 'Compartment',
                            'Amount': 'FlowAmount'}
                   ).drop(columns=['ProcessID', 'ProcessCategory'])
    df['Compartment'] = df['Compartment'].fillna('')
    df['Location'] = df['Location'].replace('US', US_FIPS)

    # Add description of materials - set material to the values in between the
    # characters 'of ' and ';' of the Activity Name - if ';' exists in string
    df['Description'] = df['ActivityProducedBy'].apply(
        lambda x: re.search('of (.*)', x).group(1))
    df['Description'] = df['Description'].apply(
        lambda x: x.split(';', 1)[0])

    # add new column info
    df['SourceName'] = 'EPA_WARMer'
    df["Class"] = "Chemicals"

    df.loc[df['FlowName'] == 'Other means of transport (no truck, train or ship)','Class'] = 'Other'
    df.loc[df['FlowName'] == 'Jobs', 'Class'] = 'Employment'
    df.loc[df['FlowName'].str.contains('Wages|Taxes'),
           'Class'] = 'Money'
    df.loc[df['FlowName'].str.contains('Energy'), 'Class'] = 'Energy'
    df.loc[df['Unit'].str.contains('kg|Item|MJ|USD|MT'), 'FlowType'] = \
        "ELEMENTARY_FLOW"
    df.loc[df['Unit'].str.contains('t*km'), 'FlowType'] = "TECHNOSPHERE_FLOW"
    df["Year"] = year
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df
