# Blackhurst_IO.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Scrapes data from Blackhurst paper 'Direct and Indirect Water Withdrawals
for US Industrial Sectors' (Supplemental info)
Includes supporting functions for Blackhurst paper data.
"""

import io
import tabula
import pandas as pd
import numpy as np
from flowsa.location import US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system, \
    load_fba_w_standardized_units
from flowsa.allocation import \
    proportional_allocation_by_location_and_activity
from flowsa.sectormapping import add_sectors_to_flowbyactivity
from flowsa.validation import compare_df_units


def bh_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    pages = range(5, 13)
    bh_df_list = []
    for x in pages:
        bh_df = tabula.read_pdf(io.BytesIO(resp.content),
                                pages=x, stream=True)[0]
        bh_df_list.append(bh_df)

    bh_df = pd.concat(bh_df_list, sort=False)

    return bh_df


def bh_parse(*, df_list, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat list of dataframes (info on each page)
    df = pd.concat(df_list, sort=False)
    df = df.rename(columns={"I-O code": "ActivityConsumedBy",
                            "I-O description": "Description",
                            "gal/$M": "FlowAmount",
                            })
    # hardcode
    # original data in gal/million usd
    df.loc[:, 'FlowAmount'] = df['FlowAmount'] / 1000000
    df['Unit'] = 'gal/USD'
    df['SourceName'] = 'Blackhurst_IO'
    df['Class'] = 'Water'
    df['FlowName'] = 'Water Withdrawals IO Vector'
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, '2002')
    df['Year'] = '2002'
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df


def convert_blackhurst_data_to_kg_per_year(df, **kwargs):
    """
    Load BEA Make After Redefinition data to convert Blackhurst IO
    dataframe units to gallon per year
    :param df: df, FBA format
    :param kwargs: kwargs includes "attr" - dictionary, attribute
    data from method yaml for activity set
    :return: transformed fba df
    """

    # load the bea make table
    bmt = load_fba_w_standardized_units(
        datasource='BEA_Make_AR',
        year=kwargs['attr']['allocation_source_year'],
        flowclass='Money',
        download_FBA_if_missing=kwargs['download_FBA_if_missing'])
    # drop rows with flowamount = 0
    bmt = bmt[bmt['FlowAmount'] != 0]

    # check on units of dfs before merge
    compare_df_units(df, bmt)
    bh_df_revised = pd.merge(
        df, bmt[['FlowAmount', 'ActivityProducedBy', 'Location']],
        left_on=['ActivityConsumedBy', 'Location'],
        right_on=['ActivityProducedBy', 'Location'])

    bh_df_revised.loc[:, 'FlowAmount'] = ((bh_df_revised['FlowAmount_x']) *
                                          (bh_df_revised['FlowAmount_y']))
    bh_df_revised.loc[:, 'Unit'] = 'kg'
    # drop columns
    bh_df_revised = bh_df_revised.drop(columns=["FlowAmount_x", "FlowAmount_y",
                                                'ActivityProducedBy_y'])
    bh_df_revised = bh_df_revised.rename(columns={"ActivityProducedBy_x":
                                                  "ActivityProducedBy"})

    return bh_df_revised


def convert_blackhurst_data_to_kg_per_employee(
        df_wsec, attr, method, **kwargs):
    """
    Load BLS employment data and use to transform original units to
    gallons per employee
    :param df_wsec: df, includes sector columns
    :param attr: dictionary, attribute data from method yaml for activity set
    :param method: dictionary, FBS method yaml
    :return: df, transformed fba dataframe with sector columns
    """

    # load 2002 employment data
    bls = load_fba_w_standardized_units(
        datasource='BLS_QCEW', year='2002',
        flowclass='Employment', geographic_level='national',
        download_FBA_if_missing=kwargs['download_FBA_if_missing'])

    # assign naics to allocation dataset
    bls_wsec = add_sectors_to_flowbyactivity(
        bls, sectorsourcename=method['target_sector_source'])
    # drop rows where sector = None ( does not occur with mining)
    bls_wsec = bls_wsec[~bls_wsec['SectorProducedBy'].isnull()]
    bls_wsec = bls_wsec.rename(columns={'SectorProducedBy': 'Sector',
                                        'FlowAmount': 'HelperFlow'})

    # check units before merge
    compare_df_units(df_wsec, bls_wsec)
    # merge the two dfs
    df = pd.merge(df_wsec,
                  bls_wsec[['Location', 'Sector', 'HelperFlow']],
                  how='left',
                  left_on=['Location', 'SectorConsumedBy'],
                  right_on=['Location', 'Sector'])
    # drop any rows where sector is None
    df = df[~df['Sector'].isnull()]
    # fill helperflow values with 0
    df['HelperFlow'] = df['HelperFlow'].fillna(0)

    # calculate proportional ratios
    df_wratio = proportional_allocation_by_location_and_activity(df, 'Sector')

    df_wratio = df_wratio.rename(columns={'FlowAmountRatio': 'EmployeeRatio',
                                          'HelperFlow': 'Employees'})

    # drop rows where helperflow = 0
    df_wratio = df_wratio[df_wratio['Employees'] != 0]

    # calculate gal/employee in 2002
    df_wratio.loc[:, 'FlowAmount'] = \
        (df_wratio['FlowAmount'] * df_wratio['EmployeeRatio']) / \
        df_wratio['Employees']
    df_wratio.loc[:, 'Unit'] = 'kg/p'

    # drop cols
    df_wratio = df_wratio.drop(
        columns=['Sector', 'Employees', 'EmployeeRatio'])

    return df_wratio


def scale_blackhurst_results_to_usgs_values(
        df_load, attr,  download_FBA_if_missing):
    """
    Scale the initial estimates for Blackhurst-based mining estimates to
    USGS values. Oil-based sectors are allocated a larger percentage of the
    difference between initial water withdrawal estimates and published USGS
    values.

    This method is based off the Water Satellite Table created by Yang and
    Ingwersen, 2017
    :param df_load: df, fba dataframe to be modified
    :param attr: dictionary, attribute data from method yaml for activity set
    :param download_FBA_if_missing: bool, indicate if missing FBAs should be
        downloaded from Data Commons
    :return: scaled fba results
    """
    # determine national level published withdrawal data for usgs mining
    # in FBS method year
    pv_load = load_fba_w_standardized_units(
        datasource="USGS_NWIS_WU", year=str(attr['helper_source_year']),
        flowclass='Water', download_FBA_if_missing=download_FBA_if_missing)

    pv_sub = pv_load[(pv_load['ActivityConsumedBy'] == 'Mining') &
                     (pv_load['Compartment'] == 'total') &
                     (pv_load['FlowName'] == 'total')].reset_index(drop=True)
    # rename the published value flow name and merge with Blackhurst data
    pv_sub = pv_sub.rename(columns={'FlowAmount': 'pv'})
    df = df_load.merge(pv_sub[['Location', 'pv']], how='left')
    # calculate the difference between published value and allocated value
    # for each naics length
    df = df.assign(nLen=df['SectorConsumedBy'].apply(lambda x: len(x)))
    # calculate initial FlowAmount accounted for
    df = df.assign(av=df.groupby('nLen')['FlowAmount'].transform('sum'))
    # calc difference
    df = df.assign(vd=df['pv'] - df['av'])

    # subset df to scale into oil and non-oil sectors
    df['sector_label'] = np.where(df['SectorConsumedBy'].apply(
        lambda x: x[0:5] == '21111'), 'oil', 'nonoil')
    df['ratio'] = np.where(df['sector_label'] == 'oil', 2 / 3, 1 / 3)
    df['label_sum'] = df.groupby(['Location', 'nLen', 'sector_label'])[
        'FlowAmount'].transform('sum')

    # calculate revised water withdrawal allocation
    df_scaled = df.copy()
    df_scaled.loc[:, 'FlowAmount'] = \
        df_scaled['FlowAmount'] + \
        (df_scaled['FlowAmount'] / df_scaled['label_sum']) * \
        (df_scaled['ratio'] * df_scaled['vd'])
    df_scaled = df_scaled.drop(columns=['sector_label', 'ratio', 'nLen',
                                        'label_sum', 'pv', 'av', 'vd'])

    return df_scaled
