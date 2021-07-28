# BLS_QCEW.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls Quarterly Census of Employment and Wages data in NAICS from Bureau of Labor Statistics
Writes out to various FlowBySector class files for these data items
EMP = Number of employees, Class = Employment
PAYANN = Annual payroll ($1,000), Class = Money
ESTAB = Number of establishments, Class = Other
This script is designed to run with a configuration parameter
--year = 'year' e.g. 2015
"""

import zipfile
import io
import pandas as pd
import numpy as np
from flowsa.common import US_FIPS, fba_default_grouping_fields, flow_by_activity_wsec_mapped_fields
from flowsa.flowbyfunctions import assign_fips_location_system, \
    aggregator
from flowsa.dataclean import add_missing_flow_by_fields, replace_strings_with_NoneType


def BLS_QCEW_URL_helper(**kwargs):
    """
    This helper function uses the "build_url" input from flowbyactivity.py, which
    is a base url for data imports that requires parts of the url text string
    to be replaced with info specific to the data year.
    This function does not parse the data, only modifies the urls from which data is obtained.
    :param kwargs: potential arguments include:
                   build_url: string, base url
                   config: dictionary, items in FBA method yaml
                   args: dictionary, arguments specified when running flowbyactivity.py
                   flowbyactivity.py ('year' and 'source')
    :return: list, urls to call, concat, parse, format into Flow-By-Activity format
    """

    # load the arguments necessary for function
    build_url = kwargs['build_url']
    args = kwargs['args']

    urls = []

    url = build_url
    url = url.replace('__year__', str(args['year']))
    urls.append(url)

    return urls


def bls_qcew_call(**kwargs):
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

    # initiate dataframes list
    df_list = []
    # unzip folder that contains bls data in ~4000 csv files
    with zipfile.ZipFile(io.BytesIO(response_load.content), "r") as f:
        # read in file names
        for name in f.namelist():
            # Only want state info
            if "singlefile" in name:
                data = f.open(name)
                df_state = pd.read_csv(data, header=0, dtype=str)
                df_list.append(df_state)
                # concat data into single dataframe
                df = pd.concat(df_list, sort=False)
                df = df[['area_fips', 'own_code', 'industry_code', 'year',
                         'annual_avg_estabs', 'annual_avg_emplvl', 'total_annual_wages']]
        return df


def bls_qcew_parse(**kwargs):
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

    # Concat dataframes
    df = pd.concat(dataframe_list, sort=False)
    # drop rows don't need
    df = df[~df['area_fips'].str.contains('C|USCMS|USMSA|USNMS')].reset_index(drop=True)
    df.loc[df['area_fips'] == 'US000', 'area_fips'] = US_FIPS
    # set datatypes
    float_cols = [col for col in df.columns if col not in ['area_fips', 'industry_code', 'year']]
    for col in float_cols:
        df[col] = df[col].astype('float')
    # Keep owner_code = 1, 2, 3, 5
    df = df[df.own_code.isin([1, 2, 3, 5])]
    # Aggregate annual_avg_estabs and annual_avg_emplvl by area_fips, industry_code, year, flag
    df = df.groupby(['area_fips',
                     'industry_code',
                     'year'])[['annual_avg_estabs',
                               'annual_avg_emplvl',
                               'total_annual_wages']].sum().reset_index()
    # Rename fields
    df = df.rename(columns={'area_fips': 'Location',
                            'industry_code': 'ActivityProducedBy',
                            'year': 'Year',
                            'annual_avg_emplvl': 'Number of employees',
                            'annual_avg_estabs': 'Number of establishments',
                            'total_annual_wages': 'Annual payroll'})
    # Reformat FIPs to 5-digit
    df['Location'] = df['Location'].apply('{:0>5}'.format)
    # use "melt" fxn to convert colummns into rows
    df = df.melt(id_vars=["Location", "ActivityProducedBy", "Year"],
                 var_name="FlowName",
                 value_name="FlowAmount")
    # specify unit based on flowname
    df['Unit'] = np.where(df["FlowName"] == 'Annual payroll', "USD", "p")
    # specify class
    df.loc[df['FlowName'] == 'Number of employees', 'Class'] = 'Employment'
    df.loc[df['FlowName'] == 'Number of establishments', 'Class'] = 'Other'
    df.loc[df['FlowName'] == 'Annual payroll', 'Class'] = 'Money'
    # add location system based on year of data
    df = assign_fips_location_system(df, args['year'])
    # add hard code data
    df['SourceName'] = 'BLS_QCEW'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    df['FlowType'] = "ELEMENTARY_FLOW"

    return df


def clean_bls_qcew_fba_for_employment_sat_table(fba_df, **kwargs):
    """
    When creating the employment satellite table for use in useeior,
    modify the flow name to match prior methodology for mapping/impact factors

    :param fba_df: df, flowbyactivity
    :param kwargs: dictionary, can include attr, a dictionary of parameters in the FBA method yaml
    :return: df, flowbyactivity, with modified flow names
    """

    fba_df = clean_bls_qcew_fba(fba_df, **kwargs)

    # rename flowname value
    fba_df['FlowName'] = fba_df['FlowName'].replace({'Number of employees': 'Jobs'})

    return fba_df


def clean_bls_qcew_fba(fba_df, **kwargs):
    """
    Function to clean BLS QCEW data when FBA is not used for employment satellite table
    :param fba_df: df, FBA format
    :param kwargs: dictionary, can include attr, a dictionary of parameters in the FBA method yaml
    :return: df, modified BLS QCEW data
    """

    fba_df = fba_df.reset_index(drop=True)
    # aggregate data to NAICS 2 digits, if 2 digit value is missing
    fba_df = replace_missing_2_digit_sector_values(fba_df)
    # drop rows of data where sectors are provided in ranges
    fba_df = remove_2_digit_sector_ranges(fba_df)

    return fba_df


def replace_missing_2_digit_sector_values(df):
    """
    In the 2015 (and possibly other dfs, there are instances of values
    at the 3 digit NAICS level, while the 2 digit NAICS is reported as 0.
    The 0 values are replaced with summed 3 digit NAICS
    :param df: df, BLS QCEW data in FBA format
    :return: df, BLS QCEW data with 2-digit NAICS sector FlowAmounts
    """

    # check for 2 digit 0 values
    df_missing = df[(df['ActivityProducedBy'].apply(lambda x:
                                                    len(x) == 2)) & (df['FlowAmount'] == 0)]
    # create list of location/activityproduced by combos
    missing_sectors = df_missing[['Location',
                                  'ActivityProducedBy']].drop_duplicates().values.tolist()

    # subset the df to 3 naics where flow amount is not 0 and
    # that would sum to the missing 2 digit naics
    df_subset = df[df['ActivityProducedBy'].apply(lambda x: len(x) == 3) & (df['FlowAmount'] != 0)]
    new_sectors_list = []
    for q, r in missing_sectors:
        c1 = df_subset['Location'] == q
        c2 = df_subset['ActivityProducedBy'].apply(lambda x: x[0:2] == r)
        # subset data
        new_sectors_list.append(df_subset[c1 & c2])
    if len(new_sectors_list) != 0:
        new_sectors = pd.concat(new_sectors_list, sort=False, ignore_index=True)

        # drop last digit of naics and aggregate
        new_sectors.loc[:, 'ActivityProducedBy'] = \
            new_sectors['ActivityProducedBy'].apply(lambda x: x[0:2])
        new_sectors = aggregator(new_sectors, fba_default_grouping_fields)

        # drop the old location/activity columns in the bls df and add new sector values
        new_sectors_list = new_sectors[['Location',
                                        'ActivityProducedBy']].drop_duplicates().values.tolist()

        # rows to drop
        rows_list = []
        for q, r in new_sectors_list:
            c1 = df['Location'] == q
            c2 = df['ActivityProducedBy'].apply(lambda x: x == r)
            # subset data
            rows_list.append(df[(c1 & c2)])
        rows_to_drop = pd.concat(rows_list, ignore_index=True)
        # drop rows from df
        modified_df = pd.merge(df, rows_to_drop, indicator=True,
                               how='outer').query('_merge=="left_only"').drop(
            '_merge', axis=1)
        # add new rows
        modified_df = modified_df.append(new_sectors, sort=False)
    else:
        modified_df = df.copy()

    return modified_df


def remove_2_digit_sector_ranges(fba_df):
    """
    BLS publishes activity ranges of '31-33', 44-45', '48-49... drop these ranges.
    The individual 2 digit naics are summed later.
    :param fba_df: df, BLS QCEW in FBA format
    :return: df, no sector ranges
    """

    df = fba_df[~fba_df['ActivityProducedBy'].str.contains('-')].reset_index(drop=True)

    return df


def bls_clean_allocation_fba_w_sec(df_w_sec, **kwargs):
    """
    clean up bls df with sectors by estimating suppresed data
    :param df_w_sec: df, FBA format BLS QCEW data
    :param kwargs: additional arguments can include 'attr', a
    dictionary of FBA method yaml parameters
    :return: df, BLS QCEW FBA with estimated suppressed data
    """
    df_w_sec = df_w_sec.reset_index(drop=True)
    df2 = add_missing_flow_by_fields(df_w_sec,
                                     flow_by_activity_wsec_mapped_fields).reset_index(drop=True)
    df3 = replace_strings_with_NoneType(df2)

    return df3
