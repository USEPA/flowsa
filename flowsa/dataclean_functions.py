"""

"""
import logging
import logging as log
import sys

import numpy as np

import flowsa
from flowsa.common import flow_by_activity_fields
from flowsa.datachecks import check_if_data_exists_at_geoscale
from flowsa.flowbyfunctions import fba_fill_na_dict, subset_df_by_geoscale
from flowsa.mapping import add_sectors_to_flowbyactivity


# import specific functions
from flowsa.data_source_scripts.BEA import subset_BEA_Use
from flowsa.data_source_scripts.Blackhurst_IO import convert_blackhurst_data_to_gal_per_year, convert_blackhurst_data_to_gal_per_employee
from flowsa.data_source_scripts.BLS_QCEW import clean_bls_qcew_fba, clean_bls_qcew_fba_for_employment_sat_table, \
    bls_clean_allocation_fba_w_sec
from flowsa.data_source_scripts.EIA_CBECS_Land import cbecs_land_fba_cleanup
from flowsa.data_source_scripts.EIA_MECS import mecs_energy_fba_cleanup, eia_mecs_energy_clean_allocation_fba_w_sec, \
    mecs_land_fba_cleanup, mecs_land_fba_cleanup_for_land_2012_fbs, mecs_land_clean_allocation_mapped_fba_w_sec
from flowsa.data_source_scripts.EPA_NEI import clean_NEI_fba, clean_NEI_fba_no_pesticides
from flowsa.data_source_scripts.StatCan_IWS_MI import convert_statcan_data_to_US_water_use, disaggregate_statcan_to_naics_6
from flowsa.data_source_scripts.stewiFBS import stewicombo_to_sector, stewi_to_sector
from flowsa.data_source_scripts.USDA_CoA_Cropland import disaggregate_coa_cropland_to_6_digit_naics, coa_irrigated_cropland_fba_cleanup
from flowsa.data_source_scripts.USDA_ERS_MLU import allocate_usda_ers_mlu_land_in_urban_areas, allocate_usda_ers_mlu_other_land,\
    allocate_usda_ers_mlu_land_in_rural_transportation_areas
from flowsa.data_source_scripts.USDA_IWMS import disaggregate_iwms_to_6_digit_naics
from flowsa.data_source_scripts.USGS_NWIS_WU import usgs_fba_data_cleanup, usgs_fba_w_sectors_data_cleanup


def load_map_clean_fba(method, attr, fba_sourcename, df_year, flowclass, geoscale_from, geoscale_to, **kwargs):
    """
    Load, clean, and map a FlowByActivity df
    :param method:
    :param attr:
    :param fba_sourcename:
    :param df_year:
    :param flowclass:
    :param geoscale_from:
    :param geoscale_to:
    :param kwargs:
    :return:
    """

    log.info("Loading allocation flowbyactivity " + fba_sourcename + " for year " +
             str(df_year))
    fba = flowsa.getFlowByActivity(datasource=fba_sourcename, year=df_year, flowclass=flowclass)
    fba = clean_df(fba, flow_by_activity_fields, fba_fill_na_dict)
    fba = harmonize_units(fba)

    # check if allocation data exists at specified geoscale to use
    log.info("Checking if allocation data exists at the " + geoscale_from + " level")
    check_if_data_exists_at_geoscale(fba, geoscale_from)

    # aggregate geographically to the scale of the flowbyactivty source, if necessary
    fba = subset_df_by_geoscale(fba, geoscale_from, geoscale_to)

    # subset based on yaml settings
    if 'flowname_subset' in kwargs:
            if kwargs['flowname_subset'] != 'None':
                fba = fba.loc[fba['FlowName'].isin(kwargs['flowname_subset'])]
    if 'compartment_subset' in kwargs:
        if kwargs['compartment_subset'] != 'None':
            fba = fba.loc[fba['Compartment'].isin(kwargs['compartment_subset'])]

    # cleanup the fba allocation df, if necessary
    if 'clean_fba' in kwargs:
        log.info("Cleaning " + fba_sourcename)
        fba = getattr(sys.modules[__name__], kwargs["clean_fba"])(fba, attr=attr)
    # reset index
    fba = fba.reset_index(drop=True)

    # assign sector to allocation dataset
    log.info("Adding sectors to " + fba_sourcename)
    fba_wsec = add_sectors_to_flowbyactivity(fba, sectorsourcename=method['target_sector_source'])

    # call on fxn to further clean up/disaggregate the fba allocation data, if exists
    if 'clean_fba_w_sec' in kwargs:
        log.info("Further disaggregating sectors in " + fba_sourcename)
        fba_wsec = getattr(sys.modules[__name__], kwargs['clean_fba_w_sec'])(fba_wsec, attr=attr, method=method)

    return fba_wsec


def clean_df(df, flowbyfields, fill_na_dict, drop_description=True):
    """

    :param df:
    :param flowbyfields: flow_by_activity_fields or flow_by_sector_fields
    :param fill_na_dict: fba_fill_na_dict or fbs_fill_na_dict
    :param drop_description: specify if want the Description column dropped, defaults to true
    :return:
    """

    # ensure correct data types
    df = add_missing_flow_by_fields(df, flowbyfields)
    # fill null values
    df = df.fillna(value=fill_na_dict)
    # drop description field, if exists
    if 'Description' in df.columns and drop_description is True:
        df = df.drop(columns='Description')
    if flowbyfields == 'flow_by_sector_fields':
        # harmonize units across dfs
        df = harmonize_units(df)
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
            df[y] = df[y].replace({'nan': None,
                                   'None': None,
                                   np.nan: None,
                                   '': None})
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
            df.loc[:, y] = df[y].replace({'nan': '',
                                          'None': '',
                                          np.nan: '',
                                          None: ''})
    return df


def add_missing_flow_by_fields(flowby_partial_df, flowbyfields):
    """
    Add in missing fields to have a complete and ordered df
    :param flowby_partial_df: Either flowbyactivity or flowbysector df
    :param flowbyfields: Either flow_by_activity_fields, flow_by_sector_fields, or flow_by_sector_collapsed_fields
    :return:
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


def harmonize_units(df):
    """
    Convert unit to standard
    Timeframe is over one year
    :param df: Either flowbyactivity or flowbysector
    :return: Df with standarized units
    """

    days_in_year = 365
    sq_ft_to_sq_m_multiplier = 0.092903
    gallon_water_to_kg = 3.79  # rounded to match USGS_NWIS_WU mapping file on FEDEFL
    ac_ft_water_to_kg = 1233481.84
    acre_to_m2 = 4046.8564224

    # class = employment, unit = 'p'
    # class = energy, unit = MJ
    # class = land, unit = m2
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'ACRES', df['FlowAmount'] * acre_to_m2,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'ACRES', 'm2', df['Unit'])
    df.loc[:, 'FlowAmount'] = np.where(df['Unit'] == 'Acres', df['FlowAmount'] * acre_to_m2,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'] == 'Acres', 'm2', df['Unit'])

    df.loc[:, 'FlowAmount'] = np.where(df['Unit'].isin(['million sq ft', 'million square feet']),
                                       df['FlowAmount'] * sq_ft_to_sq_m_multiplier * 1000000,
                                       df['FlowAmount'])
    df.loc[:, 'Unit'] = np.where(df['Unit'].isin(['million sq ft', 'million square feet']), 'm2', df['Unit'])

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
    string_cols = ['Flowable', 'Class', 'SectorProducedBy', 'SectorConsumedBy',  'SectorSourceName', 'Context',
                   'Location', 'LocationSystem', 'Unit', 'FlowType', 'Year', 'MeasureofSpread', 'MetaSources']
    df_sub = df[string_cols].drop_duplicates().reset_index(drop=True)
    # sort df
    df_sub = df_sub.sort_values(['MetaSources', 'SectorProducedBy', 'SectorConsumedBy']).reset_index(drop=True)

    # new group cols
    group_no_meta = [e for e in string_cols if e not in ('MetaSources')]

    # combine/sum columns that share the same data other than Metasources, combining MetaSources string in process
    df_sub = df_sub.groupby(group_no_meta)['MetaSources'].apply(', '.join).reset_index()
    # drop the MetaSources col in original df and replace with the MetaSources col in df_sub
    df = df.drop('MetaSources', 1)
    harmonized_df = df.merge(df_sub, how='left')
    harmonized_df = replace_strings_with_NoneType(harmonized_df)

    return harmonized_df