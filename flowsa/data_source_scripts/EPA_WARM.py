# -*- coding: utf-8 -*-
"""
EPA WARM
"""
import pandas as pd
import flowsa
from flowsa.sectormapping import get_activitytosector_mapping
import re


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


def warm_parse(*, df_list, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat list of dataframes (info on each page)
    df = pd.concat(df_list, sort=False)
    # rename columns
    df = df.rename(columns={'ProcessName': 'Activity'}).drop(
        columns=['ProcessID'])
    df['Context'] = df['Context'].fillna('')

    ### Subset WARM data
    # pathway = 'Landfilling'  # pass as function parameter?
    # df = df.query(
    #     'Context.str.startswith("emission").values &' \
    #     'ProcessCategory.str.startswith(@pathway).values')

    return df
