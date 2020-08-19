# EIA_CBECS_Land.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import pandas as pd
import numpy as np
import io
from flowsa.common import *

"""
2012 Commercial Buildings Energy Consumption Survey (CBECS)
https://www.eia.gov/consumption/commercial/reports/2012/energyusage/index.php 
Last updated: Monday, August 17, 2020
"""

def eia_cbecs_call(url, cbesc_response, args):
    # Convert response to dataframe
    df_raw = pd.io.excel.read_excel(io.BytesIO(cbesc_response.content), sheet_name='data').dropna()
    # skip rows and remove extra rows at end of dataframe
    df = pd.DataFrame(df_raw.loc[10:25]).reindex()
    # set column headers
  #  df.columns = ["PBA", "Number of Buildings", "Total Floor Space", "Total Consumption",
 #                 "Consumption per Building", "Consumption per square foot",
 #                 "Consumption per worker", "Distribution of building 25th",
 #                 "Distribution of building Median", "Distribution of building 75th"]
 #   return df
    df.columns = ["Name", "All buildings", "New England", "Middle Atlantic", "East North Central",
                      "West North Central", "South Atlantic",
                      "East South Central", "West South Central",
                      "Mountain", "Pacific"]
    return df

def eia_cbecs_parse(dataframe_list, args):
    # concat dataframes
    df = pd.concat(dataframe_list, sort=False).dropna()
    # drop columns
    df = df.melt(id_vars=["Name"],
                 var_name="FlowName",
                 value_name="FlowAmount")
    # rename column(s)
    df = df.rename(columns={'Name': 'ActivityConsumedBy'})
    # replace withdrawn code
    df.loc[df['FlowAmount'] == "Q", 'FlowAmount'] = withdrawn_keyword
    df["Class"] = 'Land'
    df["SourceName"] = 'EIA_CBECS_Land'
    df['Year'] = args["year"]
    df['LocationSystem'] = "Census_Division"
    test = get_region_and_division_codes()
    print(test)
    return df

