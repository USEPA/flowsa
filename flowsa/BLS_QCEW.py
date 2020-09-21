# BLS_QCEW.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
'''
Pulls Quarterly Census of Employment and Wages data in NAICS from Bureau of Labor Statistics
Writes out to various FlowBySector class files for these data items
EMP = Number of employees, Class = Employment
PAYANN = Annual payroll ($1,000), Class = Money
ESTAB = Number of establishments, Class = Other
This script is designed to run with a configuration parameter
--year = 'year' e.g. 2015
'''

import pandas as pd
import numpy as np
import io
import zipfile
from flowsa.common import log, get_all_state_FIPS_2, US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system


def BLS_QCEW_URL_helper(build_url, config, args):
    urls = []
    FIPS_2 = get_all_state_FIPS_2()['FIPS_2']
    us = pd.Series(['US'])
    FIPS_2 = FIPS_2.append(us, ignore_index=True)


    # the url for 2013 earlier is different than the base url (and is a zip file)
    if args["year"] < '2014':
        url = build_url
        url = url.replace('api', 'files')
        url = url.replace('a/area/__areaFIPS__.csv', 'csv/' + args["year"] + '_annual_by_area.zip')
        urls.append(url)
    else:
        for c in FIPS_2:
            url = build_url
            url = url.replace('__areaFIPS__', c + '000')
            urls.append(url)
    return urls

def bls_qcew_call(url, qcew_response, args):
    if args["year"] < '2014':
        # initiate dataframes list
        df_list = []
        # unzip folder that contains bls data in ~4000 csv files
        with zipfile.ZipFile(io.BytesIO(qcew_response.content), "r") as f:
            # read in file names
            for name in f.namelist():
                # Only want state info
                if "Statewide" in name or "US000" in name:
                    data = f.open(name)
                    df_state = pd.read_csv(data, header=0)
                    df_list.append(df_state)
                    # concat data into single dataframe
                    df = pd.concat(df_list, sort=False)
                    df = df[['area_fips', 'own_code', 'industry_code', 'year',
                             'annual_avg_estabs_count', 'annual_avg_emplvl', 'total_annual_wages']]
                    # change column name to match format for 2014+
                    df = df.rename(columns={'annual_avg_estabs_count': 'annual_avg_estabs'})
            return df
    else:
        df = pd.read_csv(io.StringIO(qcew_response.content.decode('utf-8')))
        df = df[['area_fips', 'own_code', 'industry_code', 'year',
                 'annual_avg_estabs', 'annual_avg_emplvl', 'total_annual_wages']]
        return df


def bls_qcew_parse(dataframe_list, args):
    # Concat dataframes
    df = pd.concat(dataframe_list, sort=False)
    # Keep owner_code = 1, 2, 3, 5
    df = df[df.own_code.isin([1, 2, 3, 5])]
    # Aggregate annual_avg_estabs and annual_avg_emplvl by area_fips, industry_code, year, flag
    df = df.groupby(['area_fips', 'industry_code', 'year'])[['annual_avg_estabs',
                                                             'annual_avg_emplvl',
                                                             'total_annual_wages']].sum().reset_index()
    # Rename fields
    df = df.rename(columns={'area_fips': 'Location',
                            'industry_code': 'ActivityProducedBy',
                            'year': 'Year',
                            'annual_avg_estabs': 'Number of establishments',
                            'annual_avg_emplvl': 'Number of employees',
                            'total_annual_wages': 'Annual payroll'})
    # Reformat FIPs to 5-digit
    df.loc[df['Location'] == 'US000', 'Location'] = US_FIPS
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
    return df


def clean_bls_qcew_fba(fba_df, attr):

    fba_df = replace_missing_2_digit_sector_values(fba_df)
    fba_df = remove_2_digit_sector_ranges(fba_df)

    return fba_df


def replace_missing_2_digit_sector_values(df):
    """
    In the 2015 (and possibly other dfs, there are instances of values at the 3 digit NAICS level, while
    the 2 digit NAICS is reported as 0. The 0 values are replaced with summed 3 digit NAICS
    :param df:
    :return:
    """
    from flowsa.flowbyfunctions import aggregator, fba_default_grouping_fields

    # check for 2 digit 0 values
    df_missing = df[(df['ActivityProducedBy'].apply(lambda x: len(x) == 2)) & (df['FlowAmount'] == 0)]
    # create list of location/activityproduced by combos
    missing_sectors = df_missing[['Location', 'ActivityProducedBy']].drop_duplicates().values.tolist()

    # subset the df to 3 naics where flow amount is not 0 and that would sum to the missing 2 digit naics
    df_subset = df[df['ActivityProducedBy'].apply(lambda x: len(x) == 3) & (df['FlowAmount'] != 0)]
    new_sectors_list = []
    for q, r in missing_sectors:
        c1 = df_subset['Location'] == q
        c2 = df_subset['ActivityProducedBy'].apply(lambda x: x[0:2] == r)
        # subset data
        new_sectors_list.append(df_subset[c1 & c2])
    new_sectors = pd.concat(new_sectors_list, sort=False, ignore_index=True)

    # drop last digit of naics and aggregate
    new_sectors.loc[:, 'ActivityProducedBy'] = new_sectors['ActivityProducedBy'].apply(lambda x: x[0:2])
    new_sectors = aggregator(new_sectors, fba_default_grouping_fields)

    # drop the old location/activity columns in the bls df and add new sector values
    new_sectors_list = new_sectors[['Location', 'ActivityProducedBy']].drop_duplicates().values.tolist()

    # rows to drop
    rows_list = []
    for q, r in new_sectors_list:
        c1 = df['Location'] == q
        c2 = df['ActivityProducedBy'].apply(lambda x: x == r)
        # subset data
        rows_list.append(df[(c1 & c2)])
    rows_to_drop = pd.concat(rows_list, ignore_index=True)

    # drop rows from df
    modified_df = pd.merge(df, rows_to_drop, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
    # add new rows
    modified_df = modified_df.append(new_sectors, sort=False)

    return modified_df


def remove_2_digit_sector_ranges(fba_df):
    """
    BLS publishes activity ranges of '31-33', 44-45', '48-49... drop these ranges.
    The individual 2 digit naics are summed later.
    :param df:
    :return:
    """

    df = fba_df[~fba_df['ActivityProducedBy'].str.contains('-')]

    return df



def bls_clean_allocation_fba_w_sec(df_w_sec, attr, method):
    """
    clean up bls df with sectors by estimating suppresed data
    :param df_w_sec:
    :param attr:
    :param method:
    :return:
    """

    df = estimate_suppressed_data(df_w_sec)

    return df


def estimate_suppressed_data(df):
    """
    Estimate data suppressions
    :param df:
    :return:
    """

    # exclude nonsectors
    df = df.replace({'nan': '',
                     'None': ''})
    df = df.replace({None: '',
                     np.nan: ''})

    # can be changed to expand range - takes a long time and at national level, only missing suppresed \
    # 6 digit for industrial
    estimate_range = [5]
    for i in estimate_range:

        # create df of i length
        df_x = df.loc[df['Sector'].apply(lambda x: len(x) == i)]

        # create df of i + 1 length
        df_y = df.loc[df['Sector'].apply(lambda x: len(x) == i + 1)]

        # create temp sector columns in df y, that are i digits in length
        df_y.loc[:, 's_tmp'] = df_y['Sector'].apply(lambda x: x[0:i])

        # create list of location and temp activity combos that contain a 0
        missing_sectors_df = df_y[df_y['FlowAmount'] == 0]
        missing_sectors_list = missing_sectors_df[['Location', 's_tmp']].drop_duplicates().values.tolist()
        # subset the y df
        if len(missing_sectors_list) != 0:
            # new df of sectors that start with missing sectors. drop last digit of the sector and sum flows
            # set conditions
            suppressed_list = []
            for q, r, in missing_sectors_list:
                c1 = df_y['Location'] == q
                c2 = df_y['s_tmp'] == r
                # subset data
                suppressed_list.append(df_y.loc[c1 & c2])
            suppressed_sectors = pd.concat(suppressed_list, sort=False)
            # add column of existing allocated data for length of i
            suppressed_sectors['alloc_flow'] = suppressed_sectors.groupby(['Location', 's_tmp'])['FlowAmount'].transform('sum')
            # subset further so only keep rows of 0 value
            suppressed_sectors_sub = suppressed_sectors[suppressed_sectors['FlowAmount'] == 0]
            # add count
            suppressed_sectors_sub['sector_count'] = suppressed_sectors_sub.groupby(['Location', 's_tmp'])['s_tmp'].transform('count')

            # merge suppressed sector subset with df x
            df_m = pd.merge(df_x,
                            suppressed_sectors_sub[['Class', 'Compartment', 'FlowType', 'FlowName', 'Location', 'LocationSystem', 'Unit',
                                                    'Year', 'Sector', 's_tmp', 'alloc_flow', 'sector_count']],
                            how='right',
                            left_on=['Class', 'Compartment', 'FlowType', 'FlowName', 'Location', 'LocationSystem', 'Unit',
                                     'Year', 'Sector'],
                            right_on=['Class', 'Compartment', 'FlowType', 'FlowName', 'Location', 'LocationSystem', 'Unit',
                                      'Year', 's_tmp'])
            # calculate estimated flows by subtracting the flow amount already allocated from total flow of \
            # sector one level up and divide by number of sectors with suppresed data
            df_m.loc[:, 'FlowAmount'] = (df_m['FlowAmount'] - df_m['alloc_flow']) / df_m['sector_count']
            # only keep the suppressed sector subset activity columns
            df_m2 = df_m.drop(columns = ['Sector_x', 's_tmp', 'alloc_flow', 'sector_count'])
            df_m2 = df_m2.rename(columns={'Sector_y': 'Sector'})

            # drop the existing rows with suppressed data and append the new estimates from fba df
            modified_df = pd.merge(df, suppressed_sectors_sub, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
            df = pd.concat([modified_df, df_m2], ignore_index=True, sort=True)

    df_w_estimated_data = df.replace({'': None})

    return df_w_estimated_data

