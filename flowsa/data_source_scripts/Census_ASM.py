# Census_ASM.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls Census Annual Survey of Manufacturers
--year = 'year' e.g. 2015
"""
import json
import pandas as pd
import numpy as np
from flowsa.location import US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system


def asm_URL_helper(*, build_url, year, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data
    is obtained.
    :param build_url: string, base url
    :param year: year
    :return: list, urls to call, concat, parse, format into
        Flow-By-Activity format
    """
    urls_census = []
    # This section gets the census data by county instead of by state.
    # This is only for years 2010 and 2011. This is done because the State
    # query that gets all counties returns too many results and errors out.

    url = build_url
    # url = url.replace("%3A%2A", ":*")
    urls_census.append(url)

    return urls_census


def asm_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin
        parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    cbp_json = json.loads(resp.text)
    # convert response to dataframe
    df_census = pd.DataFrame(
        data=cbp_json[1:len(cbp_json)], columns=cbp_json[0])
    return df_census


def asm_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)

    df = df.rename(columns={'NAICS2017':'ActivityProducedBy',
                            'RCPTOT': 'FlowAmount',
                            'YEAR': 'Year'})

    df['Location'] = US_FIPS
    df['FlowName'] = 'Sales'
    df['Unit'] = 'Thousand USD'
    df['Class'] ='Money'

    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # hard code data
    df['SourceName'] = 'Census_ASM'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    return df
