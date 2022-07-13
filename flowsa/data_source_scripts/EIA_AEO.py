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
import math
from flowsa.settings import externaldatapath
from flowsa.common import load_api_key
from flowsa.flowbyfunctions import assign_fips_location_system


def eia_aeo_url_helper(*, build_url, year, config, **_):
    """
    This helper function uses the "build_url" input from flowbyactivity.py,
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
    
    # initialize url list
    urls = []
    
    # maximum number of series IDs that can be called at once
    max_num_series = 100
    
    # load crosswalk of series IDs
    filepath = externaldatapath + 'AEOseriesIDs.csv'
    df_seriesIDs = pd.read_csv(filepath)
    
    # add year into series IDs


    if year == '2012':
        aeo_year = '2014'
    elif year == '2013':
        aeo_year = '2015'
    elif year == '2014':
        aeo_year = '2016'
    elif year == '2015':
        aeo_year = '2017'
    elif year == '2016':
        aeo_year = '2018'
    elif year == '2017':
        aeo_year = '2019'
    elif year == '2018':
        aeo_year = '2019'
    elif year == '2019':
        aeo_year = '2020'
    elif year == '2020':
        aeo_year = '2021'
    elif year == '2021':
        aeo_year = '2022'
    else:
        aeo_year = '2022'

    df_seriesIDs['series_id'] = df_seriesIDs['series_id'].str.replace('__year__', aeo_year)
    list_seriesIDs = df_seriesIDs['series_id'].to_list()
    
    # reshape list of series IDs into 2D array, padded with ''
    rows = max_num_series
    cols = math.ceil(len(list_seriesIDs) / max_num_series)   
    list_seriesIDs = np.pad(list_seriesIDs, (0, rows*cols - len(list_seriesIDs)), 
                            mode='constant', constant_values='')
    array_seriesIDs = list_seriesIDs.reshape(cols, rows).T
    
    # for each batch of series IDs...
    for col in range(array_seriesIDs.shape[1]):
        
        # concatenate series IDs into a list separated by semicolons
        series_list = ";".join(array_seriesIDs[:,col])
        # remove any trailing semicolons
        series_list = series_list.rstrip(";")
        
        # create url from build url
        url = build_url
        userAPIKey = load_api_key(config['api_name'])
        print(userAPIKey)
        url = url.replace("__API_KEY__", userAPIKey)
        url = url.replace("__SERIES_ID__", series_list)
        print(url)
        urls.append(url)
    
  #  print(urls)

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
    series = df_json["series"]
    df_series = pd.DataFrame(
        data=series[1:len(series)], columns=series[0])
    data = ["data"]
    d = [pd.DataFrame(df_series[col].tolist()).add_prefix(col) for col in data]
    df_series = pd.concat([df_series, d[0]], axis=1)
    df_series = df_series.drop('data', axis=1)
    row_1=df_series.iloc[0]
    for col in df_series.columns:
        if "data" in col:
            # change the column name from the current data to the row 1 year value.
            data = row_1[col]
            year_str = data[0]
            split_data = pd.DataFrame(df_series[col].to_list(), columns=[col + "_split", year_str])
            df_series = pd.concat([df_series, split_data], axis=1)
            df_series = df_series.drop(col, axis=1)
            df_series = df_series.drop(col + "_split", axis=1)
            if year_str != year:
                df_series = df_series.drop(year_str, axis=1)
    df_series = df_series.drop(columns=["updated", "end", "start", "f", "lastHistoricalPeriod", "description"])
    df_series = df_series.rename(columns={"series_id": "Description"})
    df_series = df_series.rename(columns={"units": "Unit"})
    return df_series


def eia_aeo_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)
    # Add year
    df['Year'] = year
    df['Location'] = '00000'
    df = df.rename(columns={year: "FlowAmount"})


    for index, row in df.iterrows():
        string_name = row["name"]
        #split the string based on :
        name_array = string_name.split(":")
        if len(name_array) == 4:
            name_string = name_array[0] + " " + name_array[1] + " " + name_array[2]
            apb_stting = name_array[3]
            df.loc[index, 'FlowName'] = name_string
            df.loc[index, 'ActivityProducedBy'] = apb_stting
        elif len(name_array) == 3:
            name_string = name_array[0] + " " + name_array[1]
            apb_stting = name_array[2]
            df.loc[index, 'FlowName'] = name_string
            df.loc[index, 'ActivityProducedBy'] = apb_stting
        elif len(name_array) == 2:
            name_string = name_array[0]
            apb_stting = name_array[1]
            df.loc[index, 'FlowName'] = name_string
            df.loc[index, 'ActivityProducedBy'] = apb_stting
        else:
            print(name_array)
    df = df.drop('name', axis=1)
    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # hard code data
    df['SourceName'] = 'EIA_AEO'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    df['Class'] = 'Energy'
    return df
