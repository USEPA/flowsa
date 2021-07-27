# EIA_CBECS_Water.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
'''
Pulls EIA CBECS water use data for large buildings from 2012
'''

import io
import pandas as pd
from flowsa.common import US_FIPS, WITHDRAWN_KEYWORD
from flowsa.flowbyfunctions import assign_fips_location_system


def eia_cbecs_water_call(**kwargs):
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

    # Convert response to dataframe
    df_raw = pd.io.excel.read_excel(io.BytesIO(response_load.content), sheet_name='data').dropna()
    # skip rows and remove extra rows at end of dataframe
    df = pd.DataFrame(df_raw.loc[10:25]).reindex()
    # set column headers
    df.columns = ["PBA", "Number of Buildings", "Total Floor Space", "Total Consumption",
                  "Consumption per Building", "Consumption per square foot",
                  "Consumption per worker", "Distribution of building 25th",
                  "Distribution of building Median", "Distribution of building 75th"]
    return df


def eia_cbecs_water_parse(**kwargs):
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
    df = pd.concat(dataframe_list, sort=False).dropna()
    # drop columns
    df = df.drop(columns=["Distribution of building 25th", "Distribution of building Median",
                          "Distribution of building 75th"])
    # use "melt" fxn to convert colummns into rows
    df = df.melt(id_vars=["PBA"],
                 var_name="FlowName",
                 value_name="FlowAmount")
    # rename column(s)
    df = df.rename(columns={'PBA': 'ActivityConsumedBy'})
    # replace withdrawn code
    df.loc[df['FlowAmount'] == "Q", 'FlowAmount'] = WITHDRAWN_KEYWORD
    # add unit based on flowname
    df.loc[df['FlowName'] == 'Number of Buildings', 'Unit'] = 'p'
    df.loc[df['FlowName'] == "Total Floor Space", 'Unit'] = 'million square feet'
    df.loc[df['FlowName'] == "Total Consumption", 'Unit'] = 'billion gallons'
    df.loc[df['FlowName'] == "Consumption per Building", 'Unit'] = 'thousand gallons'
    df.loc[df['FlowName'] == "Consumption per square foot", 'Unit'] = 'gallons'
    df.loc[df['FlowName'] == "Consumption per worker", 'Unit'] = 'thousand gallons'
    # class type based on flowname/unit
    df["Class"] = 'Water'
    df.loc[df['FlowName'] == 'Number of Buildings', 'Class'] = 'Other'
    df.loc[df['FlowName'] == "Total Floor Space", 'Class'] = 'Other'
    # add location system based on year of data
    df = assign_fips_location_system(df, args['year'])
    # hardcode columns
    df["SourceName"] = 'EIA_CBECS_Water'
    df['Year'] = args["year"]
    df['Location'] = US_FIPS
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  #tmp

    return df
