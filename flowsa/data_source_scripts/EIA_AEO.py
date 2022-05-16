# EIA_AEO.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
ANNUAL ENERGY OUTLOOK (AEO)
https://www.eia.gov/outlooks/aeo/
"""

import pandas as pd
import numpy as np
import math
from flowsa.settings import externaldatapath
from flowsa.common import load_api_key


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
    df_seriesIDs['series_id'] = df_seriesIDs['series_id'].str.replace('__year__', year)
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
        urls.append(url)
    
    print(urls)

    return urls