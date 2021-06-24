# USDA_ACUP.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import json
import numpy as np
import pandas as pd
from flowsa.common import *
from flowsa.flowbyfunctions import assign_fips_location_system, collapse_activity_fields, allocate_by_sector


def acup_url_helper(**kwargs):
    """
    This helper function uses the "build_url" input from flowbyactivity.py, which
    is a base url for data imports that requires parts of the url text string
    to be replaced with info specific to the data year.
    This function does not parse the data, only modifies the urls from which data is obtained.
    :param kwargs: potential arguments include:
                   build_url: string, base url
                   config: dictionary, items in FBA method yaml
                   args: dictionary, arguments specified when running flowbyactivity.py
                   flowbyactivity.py ('year' and 'source')
    :return: list, urls to call, concat, parse, format into Flow-By-Activity format
    """

    # load the arguments necessary for function
    build_url = kwargs['build_url']
    config = kwargs['config']
    # initiate url list for coa cropland data
    urls = []

    # call on state acronyms from common.py (and remove entry for DC)
    state_abbrevs = abbrev_us_state
    state_abbrevs = {k: v for (k, v) in state_abbrevs.items() if k != "DC"}

    for x in config['domain_levels']:
        for y in state_abbrevs:
            url = build_url
            url = url.replace("__domainLevel__", x)
            url = url.replace("__stateAlpha__", y)
            url = url.replace(" ", "%20")
            urls.append(url)

    return urls


def acup_call(**kwargs):
    """
    Convert response for calling url to pandas dataframe, begin parsing df into FBA format
    :param kwargs: potential arguments include:
                   url: string, url
                   response_load: df, response from url call
                   args: dictionary, arguments specified when running
                   flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    # load arguments necessary for function
    response_load = kwargs['r']

    response_json = json.loads(response_load.text)
    # not all states have data, so return empty df if does not exist
    try:
        df = pd.DataFrame(data=response_json["data"])
    except KeyError:
        # log.info('No data exists for state')
        df = []

    return df


def pest_parse(**kwargs):
    """
    Combine, parse, and format the provided dataframes
    :param kwargs: potential arguments include:
                   dataframe_list: list of dataframes to concat and format
                   args: dictionary, used to run flowbyactivity.py ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """
    # load arguments necessary for function
    dataframe_list = kwargs['dataframe_list']
    args = kwargs['args']

    df = pd.concat(dataframe_list, sort=False)

    # add location system based on year of data
    df = assign_fips_location_system(df, args['year'])
    # Add hardcoded data
    df['SourceName'] = "USDA_ACUP_Pestice"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df
