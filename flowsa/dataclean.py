# dataclean.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Common functions to clean and harmonize dataframes
"""

import numpy as np
from flowsa.common import log


def clean_df(df, flowbyfields, fill_na_dict, drop_description=True):
    """
    Modify a dataframe to ensure all columns are present and column datatypes correct
    :param df: df, any format
    :param flowbyfields: list, flow_by_activity_fields or flow_by_sector_fields
    :param fill_na_dict: dict, fba_fill_na_dict or fbs_fill_na_dict
    :param drop_description: specify if want the Description column dropped, defaults to true
    :return: df, modified
    """
    df = df.reset_index(drop=True)
    # ensure correct data types
    df = add_missing_flow_by_fields(df, flowbyfields)
    # fill null values
    df = df.fillna(value=fill_na_dict)
    # drop description field, if exists
    if 'Description' in df.columns and drop_description is True:
        df = df.drop(columns='Description')
    if flowbyfields == 'flow_by_sector_fields':
        # harmonize units across dfs
        df = standardize_units(df)
    # if datatypes are strings, ensure that Null values remain NoneType
    df = replace_strings_with_NoneType(df)

    return df


def replace_strings_with_NoneType(df):
    """
    Ensure that cell values in columns with datatype = string remain NoneType
    :param df: df with columns where datatype = object
    :return: A df where values are NoneType if they are supposed to be
    """
    # if datatypes are strings, ensure that Null values remain NoneType
    for y in df.columns:
        if df[y].dtype == object:
            df.loc[df[y].isin(['nan', 'None', np.nan, '']), y] = None
    return df


def replace_NoneType_with_empty_cells(df):
    """
    Replace all NoneType in columns where datatype = string with empty cells
    :param df: df with columns where datatype = object
    :return: A df where values are '' when previously they were NoneType
    """
    # if datatypes are strings, change NoneType to empty cells
    for y in df.columns:
        if df[y].dtype == object:
            df.loc[df[y].isin(['nan', 'None', np.nan, None]), y] = ''
    return df


def add_missing_flow_by_fields(flowby_partial_df, flowbyfields):
    """
    Add in missing fields to have a complete and ordered df
    :param flowby_partial_df: Either flowbyactivity or flowbysector df
    :param flowbyfields: Either flow_by_activity_fields, flow_by_sector_fields,
           or flow_by_sector_collapsed_fields
    :return: df, with all required columns
    """
    for k in flowbyfields.keys():
        if k not in flowby_partial_df.columns:
            flowby_partial_df[k] = None
    # convert data types to match those defined in flow_by_activity_fields
    for k, v in flowbyfields.items():
        flowby_partial_df.loc[:, k] = flowby_partial_df[k].astype(v[0]['dtype'])
    # Resort it so order is correct
    flowby_partial_df = flowby_partial_df[flowbyfields.keys()]
    return flowby_partial_df


def standardize_units(df):
    """
    Convert unit to standard
    Timeframe is over one year
    :param df: df, Either flowbyactivity or flowbysector
    :return: df, with standarized units
    """

    days_in_year = 365
    sq_ft_to_sq_m_multiplier = 0.092903
    gallon_water_to_kg = 3.79  # rounded to match USGS_NWIS_WU mapping file on FEDEFL
    ac_ft_water_to_kg = 1233481.84
    acre_to_m2 = 4046.8564224
    acres_in_thousand_acres = 1000
    mj_in_btu = .0010550559
    ton_to_kg = 907.185
    lb_to_kg = 0.45359

    # class = employment, unit = 'p'
    # class = energy, unit = MJ
    # class = land, unit = m2
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'].isin(['ACRES', 'Acres']), df['FlowAmount'] * acre_to_m2,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'].isin(['ACRES', 'Acres']), 'm2', df['Unit'])

    df.loc[:, 'FlowAmount'] = np.where(df['Unit'].isin(['million sq ft', 'million square feet']),
                                       df['FlowAmount'] * sq_ft_to_sq_m_multiplier * 1000000,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'].isin(['million sq ft', 'million square feet']),
                                 'm2', df['Unit'])

    df.loc[:, 'FlowAmount'] = np.where(df['Unit'].isin(['square feet']),
                                       df['FlowAmount'] * sq_ft_to_sq_m_multiplier,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'].isin(['square feet']), 'm2', df['Unit'])

    # class = money, unit = USD

    # class = water, unit = kg
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'gallons/animal/day',
                                       (df['FlowAmount'] * gallon_water_to_kg) * days_in_year,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'gallons/animal/day', 'kg', df['Unit'])

    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'ACRE FEET / ACRE',
                                       (df['FlowAmount'] / acre_to_m2) * ac_ft_water_to_kg,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'ACRE FEET / ACRE', 'kg/m2', df['Unit'])

    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'Mgal',
                                       df['FlowAmount'] * 1000000 * gallon_water_to_kg,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Mgal', 'kg', df['Unit'])

    # class = other, unit varies




    # Convert Energy unit "Quadrillion Btu" to MJ
    # 1 Quad = .0010550559 x 10^15
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'Quadrillion Btu',
                                       df['FlowAmount'] * mj_in_btu * (10 ** 15),
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Quadrillion Btu', 'MJ', df['Unit'])

    # Convert Energy unit "Trillion Btu" to MJ
    # 1 Tril = .0010550559 x 10^14
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'Trillion Btu',
                                       df['FlowAmount'] * mj_in_btu * (10 ** 14),
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Trillion Btu', 'MJ', df['Unit'])

    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'million Cubic metres/year',
                                       df['FlowAmount'] * 264.172, df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'million Cubic metres/year', 'Mgal', df['Unit'])

    # Convert mass units (LB or TON) to kg
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'TON',
                                       df['FlowAmount'] * ton_to_kg,
                                       df['FlowAmount'])
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'LB',
                                       df['FlowAmount'] * lb_to_kg,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'].isin(['TON', 'LB']),
                                 'kg', df['Unit'])

    return df


def harmonize_FBS_columns(df):
    """
    For FBS use in USEEIOR, harmonize the values in the columns
    - LocationSystem: drop the year, so just 'FIPS'
    - MeasureofSpread: tmp set to NoneType as values currently misleading
    - Spread: tmp set to 0 as values currently misleading
    - DistributionType: tmp set to NoneType as values currently misleading
    - MetaSources: Combine strings for rows where class/context/flowtype/flowable/etc. are equal
    :param df: FBS dataframe with mixed values/strings in columns
    :return: FBS df with harmonized values/strings in columns
    """

    # harmonize LocationSystem column
    log.info('Drop year in LocationSystem')
    if df['LocationSystem'].str.contains('FIPS').all():
        df = df.assign(LocationSystem='FIPS')
    # harmonize MeasureofSpread
    log.info('Reset MeasureofSpread to NoneType')
    df = df.assign(MeasureofSpread=None)
    # reset spread, as current values are misleading
    log.info('Reset Spread to 0')
    df = df.assign(Spread=0)
    # harmonize Distributiontype
    log.info('Reset DistributionType to NoneType')
    df = df.assign(DistributionType=None)

    # harmonize metasources
    log.info('Harmonize MetaSources')
    df = replace_NoneType_with_empty_cells(df)

    # subset all string cols of the df and drop duplicates
    string_cols = ['Flowable', 'Class', 'SectorProducedBy', 'SectorConsumedBy',
                   'SectorSourceName', 'Context', 'Location', 'LocationSystem',
                   'Unit', 'FlowType', 'Year', 'MeasureofSpread', 'MetaSources']
    df_sub = df[string_cols].drop_duplicates().reset_index(drop=True)
    # sort df
    df_sub = df_sub.sort_values(['MetaSources', 'SectorProducedBy',
                                 'SectorConsumedBy']).reset_index(drop=True)

    # new group cols
    group_no_meta = [e for e in string_cols if e not in 'MetaSources']

    # combine/sum columns that share the same data other than Metasources,
    # combining MetaSources string in process
    df_sub = df_sub.groupby(group_no_meta)['MetaSources'].apply(', '.join).reset_index()
    # drop the MetaSources col in original df and replace with the MetaSources col in df_sub
    df = df.drop('MetaSources', 1)
    harmonized_df = df.merge(df_sub, how='left')
    harmonized_df = replace_strings_with_NoneType(harmonized_df)

    return harmonized_df


def reset_fbs_dq_scores(df):
    """
    Set all Data Quality Scores to None
    :param df: FBS dataframe with mixed values/strings in columns
    :return: FBS df with the DQ scores set to null
    """

    # reset spread, as current values are misleading
    log.info('Reset Spread to None')
    df = df.assign(Spread=None)
    # reset min, as current values are misleading
    log.info('Reset Min to None')
    df = df.assign(Min=None)
    # reset min, as current values are misleading
    log.info('Reset Max to None')
    df = df.assign(Max=None)
    # reset DR, as current values are misleading
    log.info('Reset DataReliability to None')
    df = df.assign(DataReliability=None)
    # reset TC, as current values are misleading
    log.info('Reset TemporalCorrelation to None')
    df = df.assign(TemporalCorrelation=None)
    # reset GC, as current values are misleading
    log.info('Reset GeographicalCorrelation to None')
    df = df.assign(GeographicalCorrelation=None)
    # reset TC, as current values are misleading
    log.info('Reset TechnologicalCorrelation to None')
    df = df.assign(TechnologicalCorrelation=None)
    # reset DC, as current values are misleading
    log.info('Reset DataCollection to None')
    df = df.assign(DataCollection=None)

    return df
