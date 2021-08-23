# Census_PEP_Population.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls Population data from US Census Bureau
Inclues helper functions for calling and parsing data
"""

import json
import pandas as pd
from flowsa.common import US_FIPS, load_api_key, get_all_state_FIPS_2
from flowsa.flowbyfunctions import assign_fips_location_system


def Census_pop_URL_helper(**kwargs):
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
    args = kwargs['args']

    urls = []

    # get date code for july 1 population numbers
    for k, v in config['datecodes'].items():
        if str(args['year']) == str(k):
            dc = str(v)

    # the url for 2010 and earlier is different
    url2000 = 'https://api.census.gov/data/2000/pep/int_population?get=' \
              'POP,DATE_DESC&for=__aggLevel__:*&DATE_=12&key=__apiKey__'

    # state fips required for county level 13-14
    FIPS_2 = get_all_state_FIPS_2()['FIPS_2']
    # drop puerto rico
    FIPS_2 = FIPS_2[FIPS_2 != '72']

    for c in config['agg_levels']:
        # this timeframe requires state fips at the county level in the url
        if '2010' < args['year'] < '2015' and c == 'county':
            for b in FIPS_2:
                url = build_url
                url = url.replace("__aggLevel__", c)
                url = url.replace("__DateCode__", dc)
                url = url.replace("population?&", 'cty?')
                url = url.replace("DATE_CODE", 'DATE_')
                url = url.replace("&key", "&in=state:" + b + "&key")
                urls.append(url)
        else:
            if args['year'] > '2010':
                url = build_url
                url = url.replace("__aggLevel__", c)
                url = url.replace("__DateCode__", dc)
                # url date variable different pre 2018
                if args['year'] < '2018':
                    url = url.replace("DATE_CODE", 'DATE_')
                # url for 2011 - 2014 slightly modified
                if args['year'] < '2015' and c != 'county':
                    url = url.replace("population?&", 'natstprc?')
                urls.append(url)
            elif args['year'] == '2010':
                url = url2000
                url = url.replace("__aggLevel__", c)
                url = url.replace("&in=state:__stateFIPS__", '')
                if c == "us":
                    url = url.replace("*", "1")
                userAPIKey = load_api_key(config['api_name'])  # (common.py fxn)
                url = url.replace("__apiKey__", userAPIKey)
                urls.append(url)
    return urls


def census_pop_call(**kwargs):
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

    json_load = json.loads(response_load.text)
    # convert response to dataframe
    df = pd.DataFrame(data=json_load[1:len(json_load)], columns=json_load[0])
    return df


def census_pop_parse(**kwargs):
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

    # concat dataframes
    df = pd.concat(dataframe_list, sort=False)
    # Add year
    df['Year'] = args["year"]
    # drop puerto rico
    df = df[df['state'] != '72']
    # replace null county cells with '000'
    df['county'] = df['county'].fillna('000')
    # Make FIPS as a combo of state and county codes
    df['Location'] = df['state'] + df['county']
    # replace the null value representing the US with US fips
    df.loc[df['us'] == '1', 'Location'] = US_FIPS
    # drop columns
    df = df.drop(columns=['state', 'county', 'us'])
    # rename columns
    df = df.rename(columns={"POP": "FlowAmount"})
    # add location system based on year of data
    df = assign_fips_location_system(df, args['year'])
    # hardcode dta
    df['Class'] = 'Other'
    df['SourceName'] = 'Census_PEP_Population'
    df['FlowName'] = 'Population'
    df['Unit'] = 'p'
    df['ActivityConsumedBy'] = 'All'
    # temporary data quality scores
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp
    # sort df
    df = df.sort_values(['Location'])
    # reset index
    df.reset_index(drop=True, inplace=True)
    return df
