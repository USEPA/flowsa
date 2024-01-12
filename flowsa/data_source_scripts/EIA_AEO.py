# EIA_AEO.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
ANNUAL ENERGY OUTLOOK (AEO)
https://www.eia.gov/outlooks/aeo/
"""

import json
import pandas as pd
import numpy as np
import re
import math
from flowsa.settings import externaldatapath
from flowsa.common import load_env_file_key
from flowsa.flowbyfunctions import assign_fips_location_system


def eia_aeo_url_helper(*, build_url, year, config, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    :param build_url: string, base url
    :param year: year
    :param config: dictionary, items in FBA method yaml
    :return: list, urls to call, concat, parse, format into
        Flow-By-Activity format
    """
    # maximum number of series IDs that can be called at once
    max_num_series = 100
    
    list_seriesIDs = config['series_id']
    
    # reshape list of series IDs into 2D array, padded with ''
    rows = max_num_series
    cols = math.ceil(len(list_seriesIDs) / max_num_series)   
    list_seriesIDs = np.pad(list_seriesIDs, (0, rows*cols - len(list_seriesIDs)),
                            mode='constant')
    array_seriesIDs = list_seriesIDs.reshape(cols, rows).T

    urls = []
    # for each batch of series IDs...
    for col in range(array_seriesIDs.shape[1]):
        # concatenate series IDs into a list separated by semicolons
        series_list = "&facets[seriesId][]=".join(array_seriesIDs[:,col])
        # remove any empty seriesid
        series_list = series_list.replace('&facets[seriesId][]=0', '')
        
        # create url from build url
        url = build_url
        userAPIKey = load_env_file_key('API_Key', config['api_name'])
        url = (url.replace("__API_KEY__", userAPIKey)
               .replace("__YEAR__", year)
               .replace("__SERIES_ID__", series_list)
               )
        urls.append(url)
    return urls


def eia_aeo_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin
        parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    df_json = json.loads(resp.text)
    # convert response to dataframe
    series = df_json["response"]["data"]
    df_load = pd.DataFrame(data=series[1:len(series)], columns=series[0])
    # begin subsettting/renaming
    df = (df_load
          .query("period==2020")
          .rename(columns={"seriesName": "Description",
                           "value": "FlowAmount",
                           "unit": "Unit"})
          .drop(columns=['history', 'scenario', 'scenarioDescription',
                         'tableId', 'tableName', 'seriesId'])
          )

    return df


def eia_aeo_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False, ignore_index=True)
    df['Year'] = year
    df['Location'] = np.where(df['regionId'].isin(['0-0', '1-0']),
                                 '00000', None)
    df = df.rename(columns={year: "FlowAmount"})

    for index, row in df.iterrows():
        # split the string based on :
        name_array = row["Description"].split(":")
        name_array = [n.strip() for n in name_array]
        if len(name_array) == 4:
            # Except when 3rd value is 'Natural Gas'
            if name_array[2] == 'Natural Gas':
                apb_string = "-".join(name_array[0:2])
                name_string = ", ".join(name_array[2:4])
            else:
                apb_string = "-".join(name_array[0:3])
                name_string = name_array[3]
        elif len(name_array) == 3:
            # Except when first value is Residential or Commercial
            if name_array[0] in ['Residential', 'Commercial',
                                 'Transportation']:
                apb_string = "-".join([name_array[0], name_array[2]])
                name_string = name_array[1]
            else:
                apb_string = "-".join(name_array[0:2])
                name_string = name_array[2]
        elif len(name_array) == 2:
            apb_string = name_array[0]
            name_string = name_array[1]
        else:
            print(name_array)
        df.loc[index, 'FlowName'] = clean_string(name_string, 'flow')
        df.loc[index, 'ActivityConsumedBy'] = clean_string(apb_string, 'activity')
    df = assign_fips_location_system(df, year)
    # hard code data
    df['SourceName'] = 'EIA_AEO'
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    df['Class'] = 'Energy'
    return df


def clean_string(s, string_type):
    # Adjustments to flow and activity strings
    s = re.sub(', United States(.*)', '', s)
    s = re.sub(', Reference(.*)', '', s)
    if string_type == 'flow':
        s = s.replace(',','')
        s = s.replace(' Use by End Use', '')
        if s.startswith('Total') | (s=='Energy Use by Mode'):
            s = 'Total'
    if string_type == 'activity':
        if s.startswith('Energy Use-'):
            s = s.replace('Energy Use-','')
        s = s.replace('-Energy Use','')
    return s
