# stewiFBS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to access data from stewi and stewicombo for use in flowbysector

These functions are called if referenced in flowbysectormethods as
data_format FBS_outside_flowsa with the function specified in FBS_datapull_fxn
"""

import sys
import os
import pandas as pd
import numpy as np
from esupy.processed_data_mgmt import read_source_metadata

import flowsa.flowbysector
from flowsa.flowbysector import FlowBySector
from flowsa.flowbyactivity import FlowByActivity
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.flowsa_log import log
from flowsa.location import apply_county_FIPS, update_geoscale
from flowsa.settings import process_adjustmentpath
from flowsa.naics import convert_naics_year
import stewicombo
import stewi
from stewicombo.globals import addChemicalMatches, compile_metadata,\
    set_stewicombo_meta
import facilitymatcher


def stewicombo_to_sector(
        config,
        full_name,
        external_config_path: str = None,
        **_
        ) -> 'FlowBySector':
    """
    Returns emissions from stewicombo in fbs format, requires stewi >= 0.9.5
    :param config: which may contain the following elements:
        local_inventory_name: (optional) a string naming the file from which to
                source a pregenerated stewicombo file stored locally (e.g.,
                'CAP_HAP_national_2017_v0.9.7_5cf36c0.parquet' or
                'CAP_HAP_national_2017')
        inventory_dict: a dictionary of inventory types and years (e.g.,
                {'NEI':'2017', 'TRI':'2017'})
        compartments: list of compartments to include (e.g., 'water', 'air',
                'soil'), use None to include all compartments
        functions: list of functions (str) to call for additional processing
    :param method: dictionary, FBS method
    :param external_config_path, str, optional path to an FBS method outside
        flowsa repo
    :return: FlowBySector object
    """
    inventory_name = config.get('local_inventory_name')
    config['full_name'] = full_name

    df = None
    if inventory_name is not None:
        df = stewicombo.getInventory(inventory_name,
                                     download_if_missing=True)
    if df is None:
        # run stewicombo to combine inventories, filter for LCI, remove overlap
        log.info('generating inventory in stewicombo')
        df = stewicombo.combineFullInventories(
            config['inventory_dict'], filter_for_LCI=True,
            remove_overlap=True, compartments=config.get('compartments'))

    if df is None:
        # Inventories not found for stewicombo, return empty FBS
        return

    facility_mapping = extract_facility_data(config['inventory_dict'])

    # merge dataframes to assign facility information based on facility IDs
    df = (df.drop(columns=['SRS_CAS', 'SRS_ID', 'FacilityIDs_Combined'])
            .merge(facility_mapping.loc[:, facility_mapping.columns != 'NAICS'],
                   how='inner',
                   on='FacilityID')
          )

    all_NAICS = obtain_NAICS_from_facility_matcher(
        list(config['inventory_dict'].keys()))

    df = assign_naics_to_stewicombo(df, all_NAICS, facility_mapping)

    if 'reassign_process_to_sectors' in config:
        df = reassign_process_to_sectors(
                df, config['inventory_dict']['NEI'],
                config['reassign_process_to_sectors'],
                external_config_path)

    fbs = prepare_stewi_fbs(df, config)

    return fbs


def stewi_to_sector(
        config,
        full_name,
        external_config_path: str = None,
        **_
        ) -> 'FlowBySector':
    """
    Returns emissions from stewi in fbs format, requires stewi >= 0.9.5
    :param config: which may contain the following elements:
        inventory_dict: a dictionary of inventory types and years (e.g.,
                {'NEI':'2017', 'TRI':'2017'})
        compartments: list of compartments to include (e.g., 'water', 'air',
                'soil'), use None to include all compartments
        functions: list of functions (str) to call for additional processing
    :return: FlowBySector object
    """
    # determine if fxns specified in FBS method yaml
    functions = config.get('functions', [])
    config['full_name'] = full_name

    # run stewi to generate inventory and filter for LCI
    df = pd.DataFrame()
    for database, year in config['inventory_dict'].items():
        inv = (stewi.getInventory(
                database, year, filters=['filter_for_LCI', 'US_States_only'],
                download_if_missing=True)
            .assign(Year=year)
            .assign(Source=database)
            )
        df = pd.concat([df, inv], ignore_index=True)
    if config.get('compartments'):
        # Subset based on primary compartment
        df = df[df['Compartment'].str.split('/', expand=True)
                [0].isin(config.get('compartments'))]
    facility_mapping = extract_facility_data(config['inventory_dict'])
    # Convert NAICS to string (first to int to avoid decimals)
    facility_mapping['NAICS'] = \
        facility_mapping['NAICS'].astype(int).astype(str)

    # merge dataframes to assign facility information based on facility IDs
    df = df.merge(facility_mapping, how='left', on='FacilityID')
    fbs = prepare_stewi_fbs(df, config)

    for function in functions:
        fbs = getattr(sys.modules[__name__], function)(fbs)

    return fbs


def reassign_process_to_sectors(df, year, file_list, external_config_path):
    """
    Reassigns emissions from a specific process or SCC and NAICS combination
    to a new NAICS.

    :param df: a dataframe of emissions and mapped faciliites from stewicombo
    :param year: year as str
    :param file_list: list, one or more names of csv files in
        process_adjustmentpath
    :param external_config_path, str, optional path to an FBS method outside
        flowsa repo
    :return: df
    """
    df_adj = pd.DataFrame()
    for file in file_list:
        fpath = process_adjustmentpath / f"{file}.csv"
        if external_config_path:
            f_out_path = f"{external_config_path}process_adjustments/{file}.csv"
            if os.path.isfile(f_out_path):
                fpath = f_out_path
        log.debug(f"modifying processes from {fpath}")
        df_adj0 = pd.read_csv(fpath, dtype='str')
        df_adj = pd.concat([df_adj, df_adj0], ignore_index=True)

    # Eliminate duplicate adjustments
    df_adj = df_adj.drop_duplicates()
    if sum(df_adj.duplicated(subset=['source_naics', 'source_process'],
                             keep=False)) > 0:
        log.warning('duplicate process adjustments')
        df_adj = df_adj.drop_duplicates(subset=['source_naics',
                                                'source_process'])

    # obtain and prepare SCC dataset
    keep_sec_cntx = True if any('/' in s for s in df.Compartment.unique()) else False
    df_fbp = stewi.getInventory('NEI', year,
                                stewiformat='flowbyprocess',
                                download_if_missing=True,
                                keep_sec_cntx=keep_sec_cntx)
    df_fbp = df_fbp[df_fbp['Process'].isin(df_adj['source_process'])]
    df_fbp = (df_fbp.assign(Source = 'NEI')
                    .pipe(addChemicalMatches)
                    .pipe(stewicombo.overlaphandler.remove_NEI_overlaps,
                          SCC=True)
                    .drop(columns=['_CompartmentPrimary'], errors='ignore')
                    )

    # merge in NAICS data
    facility_df = (df.filter(['FacilityID', 'NAICS', 'Location'])
                     .reset_index(drop=True)
                     .drop_duplicates(keep='first'))
    df_fbp = df_fbp.merge(facility_df, how='left', on='FacilityID')
    df_fbp['Year'] = year

    #TODO: expand naics list in scc file to include child naics automatically
    df_fbp = df_fbp.merge(df_adj, how='inner',
                          left_on=['NAICS', 'Process'],
                          right_on=['source_naics', 'source_process'])

    # subtract emissions by SCC from specific facilities
    df_emissions = (df_fbp
                    .groupby(['FacilityID', 'FlowName', 'Compartment'])
                    .agg({'FlowAmount': 'sum'})
                    .rename(columns={'FlowAmount': 'Emissions'}))
    df = (df.merge(df_emissions, how='left',
                   on=['FacilityID', 'FlowName', 'Compartment'])
            .assign(Emissions = lambda x: x['Emissions'].fillna(value=0))
            .assign(FlowAmount = lambda x: x['FlowAmount'] - x['Emissions'])
            .drop(columns=['Emissions'])
            )

    # add back in emissions under the correct target NAICS
    df_fbp = (
        df_fbp.drop(columns=['Process', 'NAICS', 'source_naics', 'source_process',
                             'ProcessType', 'SRS_CAS', 'SRS_ID'])
              .rename(columns={'target_naics': 'NAICS'})
              )
    df = pd.concat([df, df_fbp], ignore_index=True)
    return df


def extract_facility_data(inventory_dict):
    """
    Returns df of facilities from each inventory in inventory_dict,
    including FIPS code
    :param inventory_dict: a dictionary of inventory types and years (e.g.,
                {'NEI':'2017', 'TRI':'2017'})
    :return: df
    """
    facilities_list = []
    # load facility data from stewi output directory, keeping only the
    # facility IDs, and geographic information
    for database, year in inventory_dict.items():
        facilities = stewi.getInventoryFacilities(database, year,
                                                  download_if_missing=True)
        facilities = facilities[['FacilityID', 'State', 'County', 'NAICS']]
        if len(facilities[facilities.duplicated(
                subset='FacilityID', keep=False)]) > 0:
            log.debug(f'Duplicate facilities in {database}_{year} - '
                      'keeping first listed')
            facilities = facilities.drop_duplicates(subset='FacilityID',
                                                    keep='first')
        facilities_list.append(facilities)

    facility_mapping = pd.concat(facilities_list, ignore_index=True)
    return facility_mapping.pipe(apply_county_FIPS)


def obtain_NAICS_from_facility_matcher(inventory_list):
    """
    Returns dataframe of all facilities with included in inventory_list with
    their first or primary NAICS.
    :param inventory_list: a list of inventories (e.g., ['NEI', 'TRI'])
    :return: df
    """
    # Access NAICS From facility matcher and assign based on FRS_ID
    all_NAICS = \
        facilitymatcher.get_FRS_NAICSInfo_for_facility_list(
            frs_id_list=None, inventories_of_interest_list=inventory_list,
            download_if_missing=True)
    all_NAICS = (all_NAICS
                 .query('PRIMARY_INDICATOR == "PRIMARY"')
                 .drop(columns=['PRIMARY_INDICATOR']))
    return all_NAICS


def assign_naics_to_stewicombo(df, all_NAICS, facility_mapping):
    """
    Apply naics to combined inventory preferentially using FRS_ID.
    When FRS_ID does not provide unique NAICS, then use NAICS assigned by
    inventory source
    :param df: combined inventory from stewicombo
    :param all_NAICS: df of NAICS by FRS_ID
    :param facility_mapping: df of NAICS by Facility_ID
    """
    # first merge in NAICS by FRS, but only where the FRS has a single NAICS
    df = df.merge(all_NAICS[~all_NAICS.duplicated(subset=['FRS_ID', 'Source'],
                                                  keep=False)],
                  how='left',
                  on=['FRS_ID', 'Source'])

    # next use NAICS from inventory sources
    df = (df.merge(facility_mapping[['FacilityID', 'NAICS']],
                   how='left',
                   on='FacilityID',
                   suffixes=(None, "_y"))
            .assign(NAICS = lambda x: x['NAICS'].fillna(x['NAICS_y']))
            .drop(columns=['NAICS_y'])
            .query('NAICS != "None"')
            )
    return df


def prepare_stewi_fbs(df_load, config) -> 'FlowBySector':
    """
    Function to prepare an emissions df from stewi or stewicombo for use as FBS
    :param df_load: a dataframe of emissions and mapped faciliites from stewi
                    or stewicombo
    :param config: dictionary, FBS method data source configuration
    :return: FlowBySector
    """
    config['fedefl_mapping'] = ([x for x in config.get('inventory_dict').keys()
                                 if x != 'RCRAInfo'])
    config['drop_unmapped_rows'] = True
    if 'year' not in config:
        config['year'] = df_load['Year'][0]

    # find activity schema
    activity_schema = config['activity_schema'] if isinstance(
        config['activity_schema'], str) else config.get(
        'activity_schema', {}).get(config['year'])

    fbs = FlowByActivity(
            df_load
            .pipe(update_geoscale, config['geoscale'])
            # ^^ update location to appropriate geoscale prior to aggregating
            .rename(columns={"NAICS": "ActivityProducedBy",
                             'Source': 'SourceName'})
            .assign(Class='Chemicals')
            .assign(ActivityConsumedBy=np.nan)
            .pipe(convert_naics_year,
                  f"NAICS_{config['target_naics_year']}_Code",
                  activity_schema,
                  config.get('full_name'))
            .assign(FlowType=lambda x: np.where(
                x['SourceName']=='RCRAInfo',
                    'WASTE_FLOW', 'ELEMENTARY_FLOW'))
            .pipe(assign_fips_location_system, config['year'])
            # ^^ Consider upating this old function
            .drop(columns=['FacilityID','FRS_ID','State','County'],
                  errors='ignore')
            .dropna(subset=['Location'])
            .reset_index(drop=True),
            full_name=config.get('full_name'),
            config=config,
        convert_df_to_flowby=True
            ).prepare_fbs()

    fbs.config.update({'data_format': 'FBS'})

    return fbs


def add_stewi_metadata(inventory_dict):
    """
    Access stewi metadata for generating FBS metdata file
    :param inventory_dict: a dictionary of inventory types and years (e.g.,
                {'NEI':'2017', 'TRI':'2017'})
    :return: combined dictionary of metadata from each inventory
    """
    return compile_metadata(inventory_dict)


def add_stewicombo_metadata(inventory_name):
    """Access locally stored stewicombo metadata by filename"""
    return read_source_metadata(stewicombo.globals.paths,
                                set_stewicombo_meta(inventory_name))


if __name__ == "__main__":
    import flowsa
    fbs = flowsa.flowbysector.FlowBySector.generateFlowBySector('CRHW_national_2017')
    fbs = flowsa.flowbysector.FlowBySector.generateFlowBySector('TRI_DMR_state_2017')
