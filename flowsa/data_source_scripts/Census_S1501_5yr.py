# Census_S1501_5yr.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls American Community Survey 5yr Subject Tables (S1501): Educational Attainment
--year = 'year' e.g. 2015
"""
import json
import pandas as pd
import numpy as np
from flowsa.location import US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system


def S1501_5yr_URL_helper(*, build_url, year, **_):
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
    urls_S1501 = []

    

    url = build_url
    urls_S1501.append(url)

    return urls_S1501


def S1501_5yr_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin
        parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    S1501_json = json.loads(resp.text)
    # convert response to dataframe
    df_S1501 = pd.DataFrame(
        data=S1501_json[1:len(S1501_json)], columns=S1501_json[0])
    return df_S1501


def S1501_5yr_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)

    # remove first string of GEO_ID to access the FIPS code
    df['GEO_ID'] = df.GEO_ID.str.replace('0500000US' , '')

    if year in ['2010','2011','2012','2013','2014']:
        df = df.rename(columns={'S1501_C01_014E':'High School Degree',
                                'S1501_C01_015E':'Bachelors Degree',
                                'NAME': 'County Name',
                                'GEO_ID':'Location'})
    else:
        df = df.rename(columns={'S1501_C02_014E':'High School Degree',
                                'S1501_C02_015E':'Bachelors Degree',
                                'NAME': 'County Name',
                                'GEO_ID':'Location'})
    
    # melt economic columns into one FlowAmount column
    df = df.melt(id_vars= ['Location'], 
                 value_vars=['High School Degree', 'Bachelors Degree'],
                 var_name='FlowName',
                 value_name='FlowAmount')
    
    # assign units based on the FlowName values
    df.loc[df.FlowName == 'High School Degree', 'Unit'] = '% of people'
    df.loc[df.FlowName == 'Bachelors Degree', 'Unit'] = '% of people'
    
    # hard code data for flowsa format
    df['LocationSystem'] = 'FIPS'
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['Class'] ='Other'
    df['Year'] = year
    df['ActivityProducedBy'] = 'Households'
    df['SourceName'] = 'Census_S1501_5yr'
    df['Description'] = 'Educational Attainment data from 5yr S1501'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None

    return df