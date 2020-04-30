# Census_CBP.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls County Business Patterns data in NAICS from the Census Bureau
Writes out to various FlowBySector class files for these data items
EMP = Number of employees, Class = Employment
PAYANN = Annual payroll ($1,000), Class = Money
ESTAB = Number of establishments, Class = Other
This script is designed to run with a configuration parameter
--year = 'year' e.g. 2015
"""

import pandas as pd
import json
from flowsa.common import US_FIPS


def Census_pop_URL_helper(build_url, config, args):
    urls = []  # todo modify url helper to create correct urls for 2010 and earlier
    for c in config['agg_levels']:
        url = build_url
        url = url.replace("__aggLevel__", c)
        urls.append(url)
    return urls


def census_pop_call(url, response_load, args):
    json_load = json.loads(response_load.text)
    # convert response to dataframe
    df = pd.DataFrame(data=json_load[1:len(json_load)], columns=json_load[0])
    return df


def census_pop_parse(dataframe_list, args):
    # concat dataframes
    df = pd.concat(dataframe_list, sort=False)
    # Add year
    df['Year'] = args["year"]
    # replace null county cells with '000'
    df['county'] = df['county'].fillna('000')
    # Make FIPS as a combo of state and county codes
    df['FIPS'] = df['state'] + df['county']
    # replace the null value representing the US with US fips
    df.loc[df['us'] == '1', 'FIPS'] = US_FIPS
    # drop columns
    df = df.drop(columns=['state', 'county', 'us'])
    # rename columns
    df = df.rename(columns={"POP": "FlowAmount"})
    # hardcode dta
    df['Class'] = 'Other'
    df['SourceName'] = 'US_Census'
    df['FlowName'] = 'Population'
    df['Unit'] = 'p'
    # temporary data quality scores
    df['DataReliability'] = None
    df['DataCollection'] = None
    return df

