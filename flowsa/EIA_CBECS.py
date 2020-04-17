# EIA_CBECS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
'''
Pulls EIA CBECS water use data for large buildings from 2012
'''

import pandas as pd
import io
import sys
from flowsa.common import log, get_all_state_FIPS_2

def eia_cbecs_call(url, eia_response):
    # Convert response to dataframe
    df_eia_raw = pd.io.excel.read_excel(io.BytesIO(eia_response.content), sheet_name='data').dropna()
    # skip rows and remove extra rows at end of dataframe
    df_eia = pd.DataFrame(df_eia_raw.loc[10:25]).reindex()
    # set column headers
    df_eia.columns = ["PBA", "Number of Buildings", "Total Floor Space", "Total Consumption",
                      "Consumption per Building", "Consumption per square foot",
                      "Consumption per worker", "Distribution of building 25th",
                      "Distribution of building Median", "Distribution of building 75th"]
    #df_eia.columns = df_eia_raw.loc[3, ]
    return df_eia



def eia_cbecs_parse(dataframe_list, args): # TODO: determine best column of water data to use for industrial allocation
    # concat dataframes
    df = pd.concat(dataframe_list, sort=False).dropna()
    # drop all columns except consumption per building
    df = df[df["PBA", "Consumption per Building"]]
    df = df.rename(columns={'PBA': 'ActivityConsumedBy',
                            'Consumption per Building': 'FlowAmount'})
    # replace withdrawn code
    df.loc[df['FlowAmount'] == "Q", 'FlowAmount'] = withdrawn_keyword
    # hardcode columns
    df['Year'] = args["year"]
    df["Unit"] = "thousand gallons per day"

    return df

