# Census_PEP_Population.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls Population data from US Census Bureau
Inclues helper functions for calling and parsing data
"""

import json
import pandas as pd
from flowsa.location import US_FIPS, get_all_state_FIPS_2
from flowsa.common import load_env_file_key
from flowsa.flowbyfunctions import assign_fips_location_system


def Census_pop_URL_helper(*, build_url, year, config, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url
    text string to be replaced with info specific to the data year. This
    function does not parse the data, only modifies the urls from which
    data is obtained.
    :param build_url: string, base url
    :param year: year
    :param config: dictionary, items in FBA method yaml
    :return: list, urls to call, concat, parse, format into
        Flow-By-Activity format
    """
    urls = []

    # get date code for july 1 population numbers
    for k, v in config['datecodes'].items():
        if str(year) == str(k):
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
        if '2010' < year < '2015' and c == 'county':
            for b in FIPS_2:
                url = build_url
                url = url.replace("population?", "cty?")
                url = url.replace("__aggLevel__", c)
                url = url.replace("__DateCode__", dc)
                url = url.replace("population?&", 'cty?')
                url = url.replace("DATE_CODE", 'DATE_')
                url = url.replace("&key", "&in=state:" + b + "&key")
                urls.append(url)
        else:
            if year > '2010':
                url = build_url
                url = url.replace("__aggLevel__", c)
                if year == '2020':
                    url = url.replace("POP", 'POP_2020')
                    url = url.replace("data/2020/pep/", 'data/2021/pep/')
                    url = url.replace("for=county%3A%2A", "for=state:*")
                    url = url.replace("&DATE_CODE=__DateCode__", "")
                    urls.append(url)
                elif year == '2021':
                    url = url.replace("POP", 'POP_2021')
                    url = url.replace("for=county%3A%2A&DATE_CODE=__DateCode__", "for=state:*")
                    url = url.replace("for=county%3A%2A", "for=state:*")
                    url = url.replace("&DATE_CODE=__DateCode__", "")
                    urls.append(url)
                else:
                    url = url.replace("__DateCode__", dc)
                    # url date variable different pre 2018
                    if year == '2013' or year == '2014':
                        url = url.replace("DATE_CODE", 'DATE_')
                        url = url.replace("population", 'natstprc')
                    elif year =='2020' or year =='2021':
                        url = url.replace("&DATE_CODE=__DateCode__", '')
                    elif year < '2018':
                        url = url.replace("DATE_CODE", 'DATE_')
                    # url for 2011 - 2014 slightly modified
                    if year < '2015' and c != 'county':
                        url = url.replace("population?&", 'natstprc?')
                    urls.append(url)
            elif year == '2010':
                url = url2000
                url = url.replace("__aggLevel__", c)
                url = url.replace("&in=state:__stateFIPS__", '')
                if c == "us":
                    url = url.replace("*", "1")
                userAPIKey = load_env_file_key('API_Key', config['api_name'])
                url = url.replace("__apiKey__", userAPIKey)
                urls.append(url)
    return urls


def census_pop_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    json_load = json.loads(resp.text)
    # convert response to dataframe
    df = pd.DataFrame(data=json_load[1:len(json_load)], columns=json_load[0])
    return df


def census_pop_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)
    # Add year
    df['Year'] = year
    # drop puerto rico
    df = df[df['state'] != '72']
    if "county" in df.columns:
        # replace null county cells with '000'
        df['county'] = df['county'].fillna('000')
        # Make FIPS as a combo of state and county codes
        df['Location'] = df['state'] + df['county']
        # replace the null value representing the US with US fips
        if "us" in df.columns:
            df.loc[df['us'] == '1', 'Location'] = US_FIPS
            # drop columns
            df = df.drop(columns=['state', 'county', 'us'])
        else:
            df = df.drop(columns=['state', 'county'])
    else:
        df['Location'] = df['state'] + '000'
        if "us" in df.columns:
            df.loc[df['us'] == '1', 'Location'] = US_FIPS
            # drop columns
            df = df.drop(columns=['state', 'us'])
        else:
            df = df.drop(columns=['state'])
    # rename columns
    if year == "2020":
        df = df.rename(columns={"POP_2020": "FlowAmount"})
    elif year == "2021":
        df = df.rename(columns={"POP_2021": "FlowAmount"})
    else:
        df = df.rename(columns={"POP": "FlowAmount"})
    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # hardcode dta
    df['Class'] = 'Other'
    df['SourceName'] = 'Census_PEP_Population'
    df['FlowName'] = 'Population'
    df['Unit'] = 'p'
    df['ActivityProducedBy'] = 'F01000'  # attribute to household
    # temporary data quality scores
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp
    # sort df
    df = df.sort_values(['Location'])
    # reset index
    df.reset_index(drop=True, inplace=True)
    return df
