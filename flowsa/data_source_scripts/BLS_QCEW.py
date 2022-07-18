# BLS_QCEW.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls Quarterly Census of Employment and Wages data in NAICS from Bureau
of Labor Statistics. Writes out to various FlowBySector class files for
these data items
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
from flowsa.location import US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system, \
    aggregator, equally_allocate_suppressed_parent_to_child_naics


def BLS_QCEW_URL_helper(*, build_url, year, **_):
    """
    This helper function uses the "build_url" input from flowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :param args: dictionary, arguments specified when running flowbyactivity.py
        flowbyactivity.py ('year' and 'source')
    :return: list, urls to call, concat, parse, format into Flow-By-Activity
        format
    """
    urls = []

    url = build_url
    url = url.replace('__year__', str(year))
    urls.append(url)

    return urls


def bls_qcew_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe,
    begin parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    # initiate dataframes list
    df_list = []
    # unzip folder that contains bls data in ~4000 csv files
    with zipfile.ZipFile(io.BytesIO(resp.content), "r") as f:
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
                         'annual_avg_estabs', 'annual_avg_emplvl',
                         'total_annual_wages']]
        return df


def bls_qcew_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # Concat dataframes
    df = pd.concat(df_list, sort=False)
    # drop rows don't need
    df = df[~df['area_fips'].str.contains(
        'C|USCMS|USMSA|USNMS')].reset_index(drop=True)
    df.loc[df['area_fips'] == 'US000', 'area_fips'] = US_FIPS
    # set datatypes
    float_cols = [col for col in df.columns if col not in
                  ['area_fips', 'own_code', 'industry_code', 'year']]
    for col in float_cols:
        df[col] = df[col].astype('float')
    # Keep owner_code = 1, 2, 3, 5
    df = df[df.own_code.isin(['1', '2', '3', '5'])]
    # replace ownership code with text defined by bls
    # https://www.bls.gov/cew/classifications/ownerships/ownership-titles.htm
    replace_dict = {'1': 'Federal Government',
                    '2': 'State Government',
                    '3': 'Local Government',
                    '5': 'Private'}
    for key in replace_dict.keys():
        df['own_code'] = df['own_code'].replace(key, replace_dict[key])
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
    df2 = df.melt(id_vars=["Location", "ActivityProducedBy", "Year",
                          'own_code'],
                  var_name="FlowName",
                  value_name="FlowAmount")
    # specify unit based on flowname
    df2['Unit'] = np.where(df2["FlowName"] == 'Annual payroll', "USD", "p")
    # specify class
    df2.loc[df2['FlowName'] == 'Number of employees', 'Class'] = 'Employment'
    df2.loc[df2['FlowName'] == 'Number of establishments', 'Class'] = 'Other'
    df2.loc[df2['FlowName'] == 'Annual payroll', 'Class'] = 'Money'
    # update flow name
    df2['FlowName'] = df2['FlowName'] + ', ' + df2['own_code']
    df2 = df2.drop(columns='own_code')
    # add location system based on year of data
    df2 = assign_fips_location_system(df2, year)
    # add hard code data
    df2['SourceName'] = 'BLS_QCEW'
    # Add tmp DQ scores
    df2['DataReliability'] = 5
    df2['DataCollection'] = 5
    df2['Compartment'] = None
    df2['FlowType'] = "ELEMENTARY_FLOW"

    return df2


def clean_bls_qcew_fba_for_employment_sat_table(fba, **_):
    """
    When creating the employment satellite table for use in useeior,
    modify the flow name to match prior methodology for mapping/impact factors.
    clean_fba_df_fxn

    :param fba: df, flowbyactivity
    :return: df, flowbyactivity, with modified flow names
    """

    # rename flowname value
    for c in ['FlowName', 'Flowable']:
        fba[c] = fba[c].str.replace('Number of employees', 'Jobs')

    return fba


def bls_clean_allocation_fba_w_sec(df_w_sec, **kwargs):
    """
    clean up bls df with sectors by estimating suppresed data
    :param df_w_sec: df, FBA format BLS QCEW data
    :param kwargs: additional arguments can include 'attr', a
    dictionary of FBA method yaml parameters
    :return: df, BLS QCEW FBA with estimated suppressed data
    """
    groupcols = list(df_w_sec.select_dtypes(include=['object', 'int']).columns)
    # estimate supressed data
    df = equally_allocate_suppressed_parent_to_child_naics(
        df_w_sec, kwargs['method'], 'SectorProducedBy', groupcols)

    # for purposes of allocation, we do not need to differentiate between
    # federal government, state government, local government, or private
    # sectors. So after estimating the suppressed data (above), modify the
    # flow names and aggregate data
    col_list = [e for e in df_w_sec.columns if e in ['FlowName', 'Flowable']]
    for c in col_list:
        df[c] = df[c].str.split(',').str[0]
    df2 = aggregator(df, groupcols)

    return df2
