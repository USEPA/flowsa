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
    urls.append(url)
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

    df = df.rename(columns={year: "FlowAmount", "MSN": "ActivityConsumedBy", "Data_Status": "Description", "State": "Location"})
    df['FlowName'] = "All consumption estimates"
    df.loc[df['Location'] == 'US', 'Location'] = '00000'
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
