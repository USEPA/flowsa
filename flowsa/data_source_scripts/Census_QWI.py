# BLS_QCEW.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls Quarterly Census of Employment and Wages data in NAICS from Bureau
of Labor Statistics. Writes out to various FlowBySector class files for
these data items
EMP = Number of employees, Class = Employment
PAYANN = Annual payroll ($1,000), Class = Money
ESTAB = Number of establishments, Class = Other
This script is designed to run with a configuration parameter
--year = 'year' e.g. 2015
"""

import json
import pandas as pd
from flowsa.location import get_county_FIPS
from flowsa.common import load_env_file_key
from flowsa.flowbyfunctions import assign_fips_location_system


def census_qwi_url_helper(*, build_url, year, config, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :param args: dictionary, arguments specified when running generateflowbyactivity.py
        generateflowbyactivity.py ('year' and 'source')
    :return: list, urls to call, concat, parse, format into Flow-By-Activity
        format
    """
    quarters = [1, 2, 3, 4]
    urls = []
    if int(year) >= 2015:
        fips_year = str(2015)
    elif int(year) >= 2013:
        fips_year = str(2013)
    else:
        fips_year = str(2010)
    county_fips_df = get_county_FIPS(fips_year)
    county_fips = county_fips_df.FIPS
    for q in quarters:
        for d in county_fips:
            url = build_url
            url = url.replace('__year__', str(year))
            userAPIKey = load_env_file_key('API_Key', config['api_name'])
            url = url.replace("__apiKey__", userAPIKey)
            state_digit = str(d[0]) + str(d[1])
            county_digit = str(d[2]) + str(d[3]) + str(d[4])
            url = url.replace("__state__", state_digit)
            url = url.replace("__county__", county_digit)
            url = url.replace("__quarter__", str(q))
            urls.append(url)

    return urls


def census_qwi_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe,
    begin parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    try:
        json_load = json.loads(resp.text)
        # convert response to dataframe
        df = pd.DataFrame(data=json_load[1:len(json_load)],
                          columns=json_load[0])
    except:
        df = pd.DataFrame()
    finally:
        return df


def census_qwi_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # Concat dataframes
    df = pd.concat(df_list, ignore_index=True)
    # drop rows don't need
    # get rid of None values in EmpTotal
    df = df[df.EmpTotal.notnull()]
    df.loc[df['ownercode'] == 'A00', 'Owner'] = 'State and local government ' \
                                                'plus private ownership'
    df.loc[df['ownercode'] == 'A01', 'Owner'] = 'Federal government'
    df.loc[df['ownercode'] == 'A05', 'Owner'] = 'All Private'
    df = df.reindex()

    # Combine the State and County into the location.
    df['Location'] = df['state'] + df['county']

    # industry needs to be renamed Activity Produced by.
    # add the Quarter and ownership codes to flowname.
    df['FlowName'] = df.apply(
        lambda x: f'Number of employees, {x["Owner"]}, Quarter {x["quarter"]}',
        axis=1)
    df = df.rename(columns={'EmpTotal': 'FlowAmount',
                            'year': 'Year',
                            'industry': "ActivityProducedBy"})

    df = df.drop(columns=['state', 'county', 'Owner', 'ownercode'])
    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # add hard code data
    df['SourceName'] = 'Census_QWI'
    df['FlowType'] = "ELEMENTARY_FLOW"
    df['Class'] = "Employment"
    df['Unit'] = "p"
    return df
