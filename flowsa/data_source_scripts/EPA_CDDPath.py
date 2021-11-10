# EPA_CDDPath.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Construction and Demolition Debris 2014 Final Disposition Estimates Using the CDDPath Method v2
https://edg.epa.gov/metadata/catalog/search/resource/details.page?uuid=https://doi.org/10.23719/1503167
Last updated: 2018-11-07
"""

import io
import pandas as pd
from flowsa.common import US_FIPS
from flowsa.settings import externaldatapath
from flowsa.flowbyfunctions import assign_fips_location_system


# Read pdf into list of DataFrame
def epa_cddpath_call(url, response_load, args):
    """
    Convert response for calling url to pandas dataframe, begin parsing df into FBA format
    :param kwargs: url: string, url
    :param kwargs: response_load: df, response from url call
    :param kwargs: args: dictionary, arguments specified when running
        flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    # Convert response to dataframe
    df = (pd.io.excel.read_excel(io.BytesIO(response_load.content),
                                 sheet_name='Final Results',
                                 # exclude extraneous rows & cols
                                 header=2, nrows=30, usecols="A, B, E",
                                 # give columns tidy names
                                 names=["FlowName", "landfilled", "processed"],
                                 # specify data types
                                 dtype={'a': str, 'b': float, 'e': float})
          .dropna()  # drop NaN's produced by Excel cell merges
          .melt(id_vars=["FlowName"],
                var_name="Description",
                value_name="FlowAmount"))

    return df


def epa_cddpath_parse(dataframe_list, args):
    """
    Combine, parse, and format the provided dataframes
    :param dataframe_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """
    # concat list of dataframes (info on each page)
    df = pd.concat(dataframe_list, sort=False)
    
    # hardcode
    df['Class'] = 'Other'  # confirm this
    df['SourceName'] = 'EPA_CDDPath'  # confirm this
    df['Unit'] = 'short tons'
    df['FlowType'] = 'WASTE_FLOW'
    df.loc[df['ActivityProducedBy'].isna(), 'ActivityProducedBy'] = 'Buildings'
    # df['Compartment'] = 'waste'  # confirm this
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, args['year'])
    df['Year'] = args['year']
    # df['MeasureofSpread'] = "NA"  # none available
    df['DataReliability'] = 5  # confirm this
    df['DataCollection'] = 5  # confirm this

    return df


def write_cdd_path_from_csv():
    file = 'EPA_2016_Table5_CNHWCGenerationbySource_Extracted_UsingCNHWCPathNames.csv'
    df = pd.read_csv(externaldatapath + file, header=0,
                     names=['FlowName', 'ActivityProducedBy',
                            'FlowAmount'])
    return df


def combine_cdd_path(url, response_load, args):
    """Call function to generate combined dataframe from csv file and excel dataset,
    bringing only those flows from the excel file that are not in the csv file
    """
    df_csv = write_cdd_path_from_csv()
    df_excel = epa_cddpath_call(url, response_load, args)
    df_excel = df_excel[~df_excel['FlowName'].isin(df_csv['FlowName'])]
    
    df = pd.concat([df_csv, df_excel], ignore_index=True)
    return df


def assign_wood_to_engineering(df, **kwargs):
    """clean_fba_df_fxn that reclassifies Wood from 'Other' to
    'Other - Wood' so that its mapping can be adjusted to only use
    237990/Heavy engineering NAICS according to method in Meyer et al. 2020
    :param df: df, FBA of CDDPath
    :param kwargs: additional arguments can include 'attr', a
    dictionary of FBA method yaml parameters
    :return: df, CDDPath FBA with wood reassigned
    """
    
    # Update wood to a new activity for improved mapping
    df.loc[((df.FlowName == 'Wood') &
           (df.ActivityProducedBy == 'Other')),
           'ActivityProducedBy'] = 'Other - Wood'

    return df
