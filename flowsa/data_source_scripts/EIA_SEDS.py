# EIA_MER.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
EIA Energy Monthly Data, summed to yearly
https://www.eia.gov/totalenergy/data/monthly/
2010 - 2020
Last updated: September 8, 2020
"""

import io
import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.location import getFIPS, get_all_state_FIPS_2, us_state_abbrev, US_FIPS


def eia_seds_url_helper(*, build_url, config, **_):
    """
    This helper function uses the "build_url" input from flowbyactivity.py,
    which is a base url for data imports that requires parts of the url
    text string to be replaced with info specific to the data year. This
    function does not parse the data, only modifies the urls from which
    data is obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :return: list, urls to call, concat, parse, format into
        Flow-By-Activity format
    """
    urls = []
    url = build_url
    csvs = ['use_all_phy.csv', 'use_US.csv']
    for csv in csvs:
        urls.append(url + csv)
    return urls


def eia_seds_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin
    parsing df into FBA format
    :param resp: response, response from url call
    :return: pandas dataframe of original source data
    """
    with io.StringIO(resp.text) as fp:
        df = pd.read_csv(fp, encoding="ISO-8859-1")
    print(year)
    columns = ['Data_Status', 'State', 'MSN']
    columns.append(year)
    print(columns)
    for col in df.columns:
        if col not in columns:
            df = df.drop(col, axis=1)
    return df

def eia_seds_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    df = pd.concat(df_list, sort=False)

    df = df.rename(columns={year: "FlowAmount", "MSN": "ActivityConsumedBy", "Data_Status": "Description"})
    df['FlowName'] = "All consumption estimates"
    fips = get_all_state_FIPS_2().reset_index(drop=True)
    # ensure capitalization of state names
    fips['State'] = fips['State'].apply(lambda x: x.title())
    fips['StateAbbrev'] = fips['State'].map(us_state_abbrev)
    # pad zeroes
    fips['FIPS_2'] = fips['FIPS_2'].apply(lambda x: x.ljust(3 + len(x), '0'))
    df = pd.merge(
        df, fips, how='left', left_on='State', right_on='StateAbbrev')
    # set us location code
    df.loc[df['State_x'] == 'US', 'FIPS_2'] = US_FIPS

    df = df.rename(columns={'FIPS_2': "Location"})
    assign_fips_location_system(df, year)
    df = df.drop('StateAbbrev', axis=1)
    df = df.drop('State_x', axis=1)
    df = df.drop('State_y', axis=1)
    # hard code data
    df['Class'] = 'Energy'
    df['SourceName'] = 'EIA_SEDS'
    df['ActivityProducedBy'] = 'None'
    df['Year'] = year
    # Fill in the rest of the Flow by fields so they show
    # "None" instead of nan.
    df['Compartment'] = 'None'
    df['MeasureofSpread'] = 'None'
    df['DistributionType'] = 'None'
    # Add DQ scores
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df
