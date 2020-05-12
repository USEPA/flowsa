# USDA_ERS_FIWS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
USDA Economic Research Service (ERS) Farm Income and Wealth Statistics (FIWS)
https://www.ers.usda.gov/data-products/farm-income-and-wealth-statistics/

Downloads the February 5, 2020 update
"""

import pandas as pd
import numpy as np
import zipfile
import io
from flowsa.common import *


def fiws_call(url, fiws_response, args):
    # extract data from zip file (only one csv)
    with zipfile.ZipFile(io.BytesIO(fiws_response.content), "r") as f:
        # read in file names
        for name in f.namelist():
            data = f.open(name)
            df = pd.read_csv(data, encoding="ISO-8859-1")
        return df

def fiws_parse(dataframe_list, args):
    # concat dataframes
    df = pd.concat(dataframe_list, sort=False)
    # select data for chosen year, cast year as string to match argument
    df['Year'] = df['Year'].astype(str)
    df = df[df['Year'] == args['year']]
    # add state fips codes, reading in datasets from common.py
    fips = get_all_state_FIPS_2()
    fips['StateAbbrev'] = fips['State'].map(us_state_abbrev)
    df = pd.merge(df, fips, how='left', left_on='State', right_on='StateAbbrev')
    # set us location code
    df.loc[df['State_x'] == 'US', 'FIPS_2'] = '00'
    # drop columns
    df = df.drop(columns=['artificialKey', 'PublicationDate', 'Source', 'ChainType_GDP_Deflator',
                          'VariableDescriptionPart1', 'VariableDescriptionPart2',
                          'State_x', 'State_y', 'StateAbbrev'])
    # rename columns
    df = df.rename(columns={"VariableDescriptionTotal": "Description",
                            "Amount": "FlowAmount",
                            "unit_desc": "Unit",
                            "FIPS_2": "Location"})
    # split description column into 2 columns, based on comma placement
    df['FlowName'], df['ActivityProducedBy'] = df['Description'].str.split(', ', 1).str
    # remove ', all" from compartment
    df['ActivityProducedBy'] = df['ActivityProducedBy'].str.replace(", all", "", regex=True)
    df['ActivityProducedBy'] = df['ActivityProducedBy'].str.strip()  # trim whitespace
    # add location system based on year of data
    df['Year'] = df['Year'].astype(int)
    df.loc[df['Year'] >= 2019, 'LocationSystem'] = 'FIPS_2019'
    df.loc[df['Year'].between(2015, 2018), 'LocationSystem'] = 'FIPS_2015'
    df.loc[df['Year'].between(2013, 2014), 'LocationSystem'] = 'FIPS_2013'
    df.loc[df['Year'].between(2010, 2012), 'LocationSystem'] = 'FIPS_2010'
    # drop unnecessary rows
    df = df[df['FlowName'].str.contains("Cash receipts|Value of production")]
    df = df[df['ActivityProducedBy'].isin(['all commodities', 'crops', 'livestock and products', 'forest products',
                                           'agricultural sector', 'services and forestry'])]
    # hard code data
    df['Class'] = 'Money'
    df['SourceName'] = 'USDA_ERS_FIWS'
    # Add tmp DQ scores
    # df['DataReliability'] = 5
    # df['DataCollection'] = 5
    # sort df
    df = df.sort_values(['Location', 'FlowName'])
    # reset index
    df.reset_index(drop=True, inplace=True)
    return df


