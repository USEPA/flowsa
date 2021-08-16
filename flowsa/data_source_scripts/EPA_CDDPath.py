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
#import numpy as np
#import flowsa
from flowsa.common import US_FIPS, externaldatapath
from flowsa.flowbyfunctions import assign_fips_location_system #, \
#     proportional_allocation_by_location_and_activity, filter_by_geoscale
# from flowsa.dataclean import harmonize_units, clean_df
# from flowsa.mapping import add_sectors_to_flowbyactivity
# from flowsa.data_source_scripts.BLS_QCEW import clean_bls_qcew_fba

# =============================================================================
# ## Import statements from EIA_CBECS_Land.py: ##
# from flowsa.common import US_FIPS, get_region_and_division_codes, withdrawn_keyword,\
#     clean_str_and_capitalize, fba_default_grouping_fields
# from flowsa.flowbyfunctions import assign_fips_location_system, aggregator
# from flowsa.values_from_literature import \
#     get_commercial_and_manufacturing_floorspace_to_land_area_ratio
# =============================================================================


# Read pdf into list of DataFrame
def epa_cddpath_call(**kwargs):
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


def epa_cddpath_parse(**kwargs):
    """
    Combine, parse, and format the provided dataframes
    :param kwargs: potential arguments include:
                   dataframe_list: list of dataframes to concat and format
                   args: dictionary, used to run flowbyactivity.py ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """
    # load arguments necessary for function
    args = kwargs['args']
    dataframe_list = kwargs['dataframe_list']

    # concat list of dataframes (info on each page)
    df = pd.concat(dataframe_list, sort=False)
    
    # hardcode
    df['Class'] = 'Other'  # confirm this
    df['SourceName'] = 'EPA_CDDPath'  # confirm this
    df['Unit'] = 'short tons'
    df['FlowType'] = 'WASTE_FLOW'
    # df['Compartment'] = 'waste'  # confirm this
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, args['year'])
    df['Year'] = args['year']
    # df['MeasureofSpread'] = "NA"  # none available
    df['DataReliability'] = 5  # confirm this
    df['DataCollection'] = 5  # confirm this

    return df


def write_cdd_path_from_csv(**kwargs):
    file = 'EPA_2016_Table5_CNHWCGenerationbySource_Extracted_UsingCNHWCPathNames.csv'
    df = pd.read_csv(externaldatapath + file, header = 0,
                     names = ['FlowName', 'ActivityProducedBy',
                              'FlowAmount'])
    return df

        
    