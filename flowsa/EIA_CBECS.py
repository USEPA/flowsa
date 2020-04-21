# EIA_CBECS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
'''
Pulls EIA CBECS water use data for large buildings from 2012
'''

import pandas as pd
import io
from flowsa.common import US_FIPS, withdrawn_keyword

def eia_cbecs_call(url, response_load):
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


def eia_cbecs_parse(dataframe_list, args):
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
    df.loc[df['FlowAmount'] == "Q", 'FlowAmount'] = withdrawn_keyword
    # add unit based on flowname
    df.loc[df['FlowName'] == 'Number of Buildings', 'Unit'] = 'p'
    df.loc[df['FlowName'] == "Total Floor Space", 'Unit'] = 'million square feet'
    df.loc[df['FlowName'] == "Total Consumption", 'Unit'] = 'billion gallons'
    df.loc[df['FlowName'] == "Consumption per Building", 'Unit'] = 'thousand gallons'
    df.loc[df['FlowName'] == "Consumption per square foot", 'Unit'] = 'gallons'
    df.loc[df['FlowName'] == "Consumption per worker", 'Unit'] = 'thousand gallons'
    # hardcode columns
    df["Class"] = 'Water'
    df["SourceName"] = 'EIA_CBECS'
    df['Year'] = args["year"]
    df['FIPS'] = US_FIPS
    return df

