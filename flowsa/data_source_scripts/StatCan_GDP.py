# Stat_Canada.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
How to cite: Statistics Canada. Table 36-10-0401-01 Gross domestic product
(GDP) at basic prices, by industry (x 1,000,000)
DOI: https://doi.org/10.25318/3610040101-eng

"""

import io
import zipfile
import pandas as pd
from flowsa.common import call_country_code


def sc_gdp_call(**kwargs):
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
    # read all files in the stat canada zip
    with zipfile.ZipFile(io.BytesIO(response_load.content), "r") as f:
        # read in file names
        for name in f.namelist():
            # if filename does not contain "MetaData", then create dataframe
            if "MetaData" not in name:
                data = f.open(name)
                df = pd.read_csv(data, header=0)
    return df


def sc_gdp_parse(**kwargs):
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
    df = pd.concat(dataframe_list, sort=False)
    # drop columns
    df = df.drop(columns=['COORDINATE', 'DECIMALS', 'DGUID', 'SYMBOL',
                          'TERMINATED', 'UOM_ID', 'SCALAR_ID', 'VECTOR',
                          'SCALAR_FACTOR'])
    # rename columns
    df = df.rename(columns={'GEO': 'Location',
                            'North American Industry Classification System (NAICS)': 'Description',
                            'REF_DATE': 'Year',
                            'STATUS': 'Spread',
                            'VALUE': "FlowAmount",
                            "UOM": 'Unit'})
    # extract NAICS as activity column. rename activity based on flowname
    df['ActivityProducedBy'] = df['Description'].str.extract('.*\[(.*)\].*')
    # hard code data
    df['Class'] = 'Money'
    df.loc[:, 'FlowName'] = 'GDP'
    df.loc[:, 'Unit'] = 'Canadian Dollar'
    df.loc[:, 'FlowAmount'] = \
        df['FlowAmount'].astype(float) * 1000000  # original unit million canadian dollars
    df['SourceName'] = 'StatCan_GDP'
    df.loc[:, 'Year'] = df['Year'].astype(str)
    # temp hardcode canada iso code
    df['Location'] = call_country_code('Canada')
    df['LocationSystem'] = "ISO"
    df["MeasureofSpread"] = 'RSD'
    df["DataReliability"] = 3
    df["DataCollection"] = 4

    # drop data
    df = df[df['Year'] == args['year']]
    df = df[~df['ActivityProducedBy'].apply(lambda x: x[0:1] == 'T')].reset_index(drop=True)
    return df
