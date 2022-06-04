# stewiFBS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to access data from stewi and stewicombo for use in flowbysector

These functions are called if referenced in flowbysectormethods as
data_format FBS_outside_flowsa with the function specified in FBS_datapull_fxn

Requires StEWI >= 0.9.5. https://github.com/USEPA/standardizedinventories

"""

import sys
import os
import pandas as pd
from esupy.dqi import get_weighted_average
from esupy.processed_data_mgmt import read_source_metadata
from flowsa.allocation import equally_allocate_parent_to_child_naics
from flowsa.flowbyfunctions import assign_fips_location_system,\
    aggregate_and_subset_for_target_sectors
from flowsa.dataclean import add_missing_flow_by_fields
from flowsa.sectormapping import map_flows
from flowsa.location import apply_county_FIPS, update_geoscale
from flowsa.schema import flow_by_sector_fields
from flowsa.settings import log, process_adjustmentpath
from flowsa.validation import replace_naics_w_naics_from_another_year
import stewicombo
import stewi
from stewicombo.overlaphandler import remove_default_flow_overlaps
from stewicombo.globals import addChemicalMatches, compile_metadata,\
    set_stewicombo_meta
import facilitymatcher


def stewicombo_to_sector(yaml_load, method, fbsconfigpath=None):
    """
    Returns emissions from stewicombo in fbs format, requires stewi >= 0.9.5
    :param yaml_load: which may contain the following elements:
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
    :param fbsconfigpath, str, optional path to an FBS method outside flowsa repo
    :return: df, FBS format
    """
    inventory_name = yaml_load.get('local_inventory_name')

    df = None
    if inventory_name is not None:
        df = stewicombo.getInventory(inventory_name,
                                     download_if_missing=True)
    if df is None:
        # run stewicombo to combine inventories, filter for LCI, remove overlap
        log.info('generating inventory in stewicombo')
        df = stewicombo.combineFullInventories(
            yaml_load['inventory_dict'], filter_for_LCI=True,
            remove_overlap=True, compartments=yaml_load['compartments'])

    if df is None:
        # Inventories not found for stewicombo, return empty FBS
        return

    df.drop(
        columns=['SRS_CAS', 'SRS_ID', 'FacilityIDs_Combined'], inplace=True)

    inventory_list = list(yaml_load['inventory_dict'].keys())
    facility_mapping = extract_facility_data(yaml_load['inventory_dict'])

    # merge dataframes to assign facility information based on facility IDs
    df = pd.merge(df,
                  facility_mapping.loc[:, facility_mapping.columns != 'NAICS'],
                  how='left', on='FacilityID')

    all_NAICS = obtain_NAICS_from_facility_matcher(inventory_list)

    df = assign_naics_to_stewicombo(df, all_NAICS, facility_mapping)

    if 'reassign_process_to_sectors' in yaml_load:
        df = reassign_process_to_sectors(
            df, yaml_load['inventory_dict']['NEI'],
            yaml_load['reassign_process_to_sectors'],
            fbsconfigpath)

    df['MetaSources'] = df['Source']

    fbs = prepare_stewi_fbs(df, yaml_load, method)

    return fbs


def stewi_to_sector(yaml_load, method, *_):
    """
    Returns emissions from stewi in fbs format, requires stewi >= 0.9.5
    :param yaml_load: which may contain the following elements:
        inventory_dict: a dictionary of inventory types and years (e.g.,
                {'NEI':'2017', 'TRI':'2017'})
        compartments: list of compartments to include (e.g., 'water', 'air',
                'soil'), use None to include all compartments
        functions: list of functions (str) to call for additional processing
    :param method: dictionary, FBS method
    :return: df, FBS format
    """
    # determine if fxns specified in FBS method yaml
    functions = yaml_load.get('functions', [])

    # run stewi to generate inventory and filter for LCI
    df = pd.DataFrame()
    for database, year in yaml_load['inventory_dict'].items():
        inv = stewi.getInventory(
            database, year, filters=['filter_for_LCI', 'US_States_only'],
            download_if_missing=True)
        inv['Year'] = year
        inv['MetaSources'] = database
        df = df.append(inv)
    if yaml_load['compartments'] is not None:
        df = df[df['Compartment'].isin(yaml_load['compartments'])]
    facility_mapping = extract_facility_data(yaml_load['inventory_dict'])
    # Convert NAICS to string (first to int to avoid decimals)
    facility_mapping['NAICS'] = \
        facility_mapping['NAICS'].astype(int).astype(str)

    # merge dataframes to assign facility information based on facility IDs
    df = pd.merge(df, facility_mapping, how='left',
                  on='FacilityID')

    fbs = prepare_stewi_fbs(df, yaml_load, method)

    for function in functions:
        fbs = getattr(sys.modules[__name__], function)(fbs)

    return fbs


def reassign_process_to_sectors(df, year, file_list, fbsconfigpath):
    """
    Reassigns emissions from a specific process or SCC and NAICS combination
    to a new NAICS.

    :param df: a dataframe of emissions and mapped faciliites from stewicombo
    :param year: year as str
    :param file_list: list, one or more names of csv files in
        process_adjustmentpath
    :param fbsconfigpath, str, optional path to an FBS method outside flowsa repo
    :return: df
    """
    df_adj = pd.DataFrame()
    for file in file_list:
        fpath = f"{process_adjustmentpath}{file}.csv"
        if fbsconfigpath:
            f_out_path = f"{fbsconfigpath}process_adjustments/{file}.csv"
            if os.path.isfile(f_out_path):
                fpath = f_out_path
        log.debug(f"modifying processes from {fpath}")
        df_adj0 = pd.read_csv(fpath, dtype='str')
        df_adj = pd.concat([df_adj, df_adj0], ignore_index=True)

    # Eliminate duplicate adjustments
    df_adj.drop_duplicates(inplace=True)
    if sum(df_adj.duplicated(subset=['source_naics', 'source_process'],
                                  keep=False)) > 0:
        log.warning('duplicate process adjustments')
        df_adj.drop_duplicates(subset=['source_naics', 'source_process'],
                               inplace=True)

    # obtain and prepare SCC dataset
    df_fbp = stewi.getInventory('NEI', year,
                                stewiformat='flowbyprocess',
                                download_if_missing=True)
    df_fbp = df_fbp[df_fbp['Process'].isin(df_adj['source_process'])]
    df_fbp['Source'] = 'NEI'
    df_fbp = addChemicalMatches(df_fbp)
    df_fbp = remove_default_flow_overlaps(df_fbp, SCC=True)

    # merge in NAICS data
    facility_df = df[['FacilityID', 'NAICS', 'Location']].reset_index(drop=True)
    facility_df.drop_duplicates(keep='first', inplace=True)
    df_fbp = df_fbp.merge(facility_df, how='left', on='FacilityID')

    df_fbp['Year'] = year

    #TODO: expand naics list in scc file to include child naics automatically
    df_fbp = df_fbp.merge(df_adj, how='inner',
                          left_on=['NAICS', 'Process'],
                          right_on=['source_naics', 'source_process'])

    # subtract emissions by SCC from specific facilities
    df_emissions = df_fbp.groupby(['FacilityID', 'FlowName']).agg(
        {'FlowAmount': 'sum'})
    df_emissions.rename(columns={'FlowAmount': 'Emissions'}, inplace=True)
    df = df.merge(df_emissions, how='left',
                  on=['FacilityID', 'FlowName'])
    df[['Emissions']] = df[['Emissions']].fillna(value=0)
    df['FlowAmount'] = df['FlowAmount'] - df['Emissions']
    df.drop(columns=['Emissions'], inplace=True)

    # add back in emissions under the correct target NAICS
    df_fbp.drop(columns=['Process', 'NAICS', 'source_naics', 'source_process',
                         'ProcessType', 'SRS_CAS', 'SRS_ID'],
                inplace=True)
    df_fbp.rename(columns={'target_naics': 'NAICS'}, inplace=True)
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
            facilities.drop_duplicates(subset='FacilityID',
                                       keep='first', inplace=True)
        facilities_list.append(facilities)

    facility_mapping = pd.concat(facilities_list, ignore_index=True)
    # Apply FIPS to facility locations
    facility_mapping = apply_county_FIPS(facility_mapping)

    return facility_mapping


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
    all_NAICS = all_NAICS.loc[all_NAICS['PRIMARY_INDICATOR'] == 'PRIMARY']
    all_NAICS.drop(columns=['PRIMARY_INDICATOR'], inplace=True)
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
    df = pd.merge(df, all_NAICS[~all_NAICS.duplicated(
        subset=['FRS_ID', 'Source'], keep=False)],
        how='left', on=['FRS_ID', 'Source'])

    # next use NAICS from inventory sources
    df = pd.merge(df, facility_mapping[['FacilityID', 'NAICS']], how='left',
                  on='FacilityID', suffixes=(None, "_y"))
    df['NAICS'].fillna(df['NAICS_y'], inplace=True)
    df.drop(columns=['NAICS_y'], inplace=True)
    # Drop records where sector can not be assigned
    df = df.loc[df['NAICS']!='None']
    return df


def prepare_stewi_fbs(df_load, yaml_load, method):
    """
    Function to prepare an emissions df from stewi or stewicombo for use as FBS
    :param df_load: a dataframe of emissions and mapped faciliites from stewi
                    or stewicombo
    :param yaml_load: dictionary, FBS method data source configuration
    :param method: dictonary, FBS method
    :return: df
    """
    inventory_dict = yaml_load.get('inventory_dict')
    geo_scale = method.get('target_geoscale')

    # update location to appropriate geoscale prior to aggregating
    df = df_load.dropna(subset=['Location'])
    df['Location'] = df['Location'].astype(str)
    df = update_geoscale(df, geo_scale)

    df = df.rename(columns = {"NAICS": "SectorProducedBy"})
    df.loc[:,'SectorConsumedBy'] = 'None'

    df = replace_naics_w_naics_from_another_year(df, 'NAICS_2012_Code')
    df = equally_allocate_parent_to_child_naics(df, method)

    df_subset = aggregate_and_subset_for_target_sectors(df, method)

    # assign grouping variables based on desired geographic aggregation level
    grouping_vars = ['FlowName', 'Compartment', 'Location',
                     'SectorProducedBy']
    if 'MetaSources' in df:
        grouping_vars.append('MetaSources')

    # aggregate by NAICS code, FlowName, compartment, and geographic level
    fbs = df_subset.groupby(grouping_vars).agg({'FlowAmount': 'sum',
                                                'Year': 'first',
                                                'Unit': 'first'})

    # add reliability score
    fbs['DataReliability'] = get_weighted_average(
        df, 'DataReliability', 'FlowAmount', grouping_vars)
    fbs.reset_index(inplace=True)

    # apply flow mapping separately for elementary and waste flows
    fbs['FlowType'] = 'ELEMENTARY_FLOW'
    fbs.loc[fbs['MetaSources'] == 'RCRAInfo', 'FlowType'] = 'WASTE_FLOW'

    # Add 'SourceName' for mapping purposes
    fbs['SourceName'] = fbs['MetaSources']
    fbs_elem = fbs.loc[fbs['FlowType'] == 'ELEMENTARY_FLOW']
    fbs_waste = fbs.loc[fbs['FlowType'] == 'WASTE_FLOW']
    fbs_list = []
    if len(fbs_elem) > 0:
        fbs_elem = map_flows(fbs_elem, list(inventory_dict.keys()),
                             flow_type='ELEMENTARY_FLOW')
        fbs_list.append(fbs_elem)
    if len(fbs_waste) > 0:
        fbs_waste = map_flows(fbs_waste, list(inventory_dict.keys()),
                              flow_type='WASTE_FLOW')
        fbs_list.append(fbs_waste)

    if len(fbs_list) == 1:
        fbs_mapped = fbs_list[0].copy()
    else:
        fbs_mapped = pd.concat[fbs_list].reset_index(drop=True)

    # add hardcoded data, depending on the source data,
    # some of these fields may need to change
    fbs_mapped['Class'] = 'Chemicals'
    fbs_mapped['SectorSourceName'] = 'NAICS_2012_Code'

    fbs_mapped = assign_fips_location_system(
        fbs_mapped, list(inventory_dict.values())[0])

    # add missing flow by sector fields
    fbs_mapped = add_missing_flow_by_fields(fbs_mapped, flow_by_sector_fields)

    # sort dataframe and reset index
    fbs_mapped = fbs_mapped.sort_values(
        list(flow_by_sector_fields.keys())).reset_index(drop=True)

    return fbs_mapped


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
    flowsa.flowbysector.main(method='CRHW_state_2017')
    #flowsa.flowbysector.main(method='TRI_DMR_state_2017')
