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
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.dataclean import add_missing_flow_by_fields
from flowsa.sectormapping import map_flows
from flowsa.location import apply_county_FIPS, update_geoscale
from flowsa.common import load_crosswalk, sector_level_key
from flowsa.schema import flow_by_sector_fields
from flowsa.settings import log, process_adjustmentpath
from flowsa.validation import replace_naics_w_naics_from_another_year


def stewicombo_to_sector(yaml_load, fbsconfigpath=None):
    """
    Returns emissions from stewicombo in fbs format, requires stewi >= 0.9.5
    :param yaml_load: which may contain the following elements:
        local_inventory_name: (optional) a string naming the file from which to
                source a pregenerated stewicombo file stored locally (e.g.,
                'CAP_HAP_national_2017_v0.9.7_5cf36c0.parquet' or
                'CAP_HAP_national_2017')
        inventory_dict: a dictionary of inventory types and years (e.g.,
                {'NEI':'2017', 'TRI':'2017'})
        NAICS_level: desired NAICS aggregation level, using sector_level_key,
                should match target_sector_level
        geo_scale: desired geographic aggregation level ('national', 'state',
                'county'), should match target_geoscale
        compartments: list of compartments to include (e.g., 'water', 'air',
                'soil'), use None to include all compartments
        functions: list of functions (str) to call for additional processing
    :param fbsconfigpath, str, optional path to an FBS method outside flowsa repo
    :return: df, FBS format
    """

    import stewicombo
    from flowsa.data_source_scripts.EPA_NEI import drop_GHGs

    # determine if fxns specified in FBS method yaml
    functions = yaml_load.get('functions', [])
    inventory_name = yaml_load.get('local_inventory_name')

    NAICS_level_value = sector_level_key[yaml_load['NAICS_level']]

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

    if 'drop_GHGs' in functions:
        df = drop_GHGs(df)
        functions.remove('drop_GHGs')
    facility_mapping = extract_facility_data(yaml_load['inventory_dict'])

    # merge dataframes to assign facility information based on facility IDs
    df = pd.merge(df,
                  facility_mapping.loc[:, facility_mapping.columns != 'NAICS'],
                  how='left', on='FacilityID')

    all_NAICS = obtain_NAICS_from_facility_matcher(inventory_list)

    df = assign_naics_to_stewicombo(df, all_NAICS, facility_mapping)

    # add levelized NAICS code prior to aggregation
    df['NAICS_lvl'] = df['NAICS'].str[0:NAICS_level_value]

    if 'reassign_process_to_sectors' in yaml_load:
        df = reassign_process_to_sectors(
            df, yaml_load['inventory_dict']['NEI'],
            NAICS_level_value,
            yaml_load['reassign_process_to_sectors'],
            fbsconfigpath)

    df['MetaSources'] = df['Source']

    fbs = prepare_stewi_fbs(df, yaml_load['inventory_dict'],
                            yaml_load['NAICS_level'], yaml_load['geo_scale'])

    for function in functions:
        fbs = getattr(sys.modules[__name__], function)(fbs)

    return fbs


def stewi_to_sector(yaml_load, *_):
    """
    Returns emissions from stewi in fbs format, requires stewi >= 0.9.5
    :param yaml_load: which may contain the following elements:
        inventory_dict: a dictionary of inventory types and years (e.g.,
                {'NEI':'2017', 'TRI':'2017'})
        NAICS_level: desired NAICS aggregation level, using sector_level_key,
                should match target_sector_level
        geo_scale: desired geographic aggregation level ('national', 'state',
                'county'), should match target_geoscale
        compartments: list of compartments to include (e.g., 'water', 'air',
                'soil'), use None to include all compartments
        functions: list of functions (str) to call for additional processing
    :return: df, FBS format
    """
    import stewi

    # determine if fxns specified in FBS method yaml
    functions = yaml_load.get('functions', [])

    NAICS_level_value = sector_level_key[yaml_load['NAICS_level']]
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
    facility_mapping = naics_expansion(facility_mapping)

    # merge dataframes to assign facility information based on facility IDs
    df = pd.merge(df, facility_mapping, how='left',
                  on='FacilityID')

    # add levelized NAICS code prior to aggregation
    df['NAICS_lvl'] = df['NAICS'].str[0:NAICS_level_value]

    fbs = prepare_stewi_fbs(df, yaml_load['inventory_dict'],
                            yaml_load['NAICS_level'], yaml_load['geo_scale'])

    for function in functions:
        fbs = getattr(sys.modules[__name__], function)(fbs)

    return fbs


def reassign_process_to_sectors(df, year, NAICS_level_value, file_list,
                            fbsconfigpath):
    """
    Reassigns emissions from a specific process or SCC and NAICS combination
    to a new NAICS.

    :param df: a dataframe of emissions and mapped faciliites from stewicombo
    :param year: year as str
    :param NAICS_level_value: desired NAICS aggregation level,
        using sector_level_key, should match target_sector_level
    :param file_list: list, one or more names of csv files in
        process_adjustmentpath
    :param fbsconfigpath, str, optional path to an FBS method outside flowsa repo
    :return: df
    """
    import stewi
    from stewicombo.overlaphandler import remove_default_flow_overlaps
    from stewicombo.globals import addChemicalMatches

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
    df_fbp.loc[:, 'NAICS_lvl'] = df_fbp['NAICS'].str[0:NAICS_level_value]
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
    import stewi
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
    import facilitymatcher
    # Access NAICS From facility matcher and assign based on FRS_ID
    all_NAICS = \
        facilitymatcher.get_FRS_NAICSInfo_for_facility_list(
            frs_id_list=None, inventories_of_interest_list=inventory_list,
            download_if_missing=True)
    all_NAICS = all_NAICS.loc[all_NAICS['PRIMARY_INDICATOR'] == 'PRIMARY']
    all_NAICS.drop(columns=['PRIMARY_INDICATOR'], inplace=True)
    all_NAICS = naics_expansion(all_NAICS)
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

    return df


def prepare_stewi_fbs(df, inventory_dict, NAICS_level, geo_scale):
    """
    Function to prepare an emissions df from stewi or stewicombo for use as FBS
    :param df: a dataframe of emissions and mapped faciliites from stewi
                or stewicombo
    :param inventory_dict: a dictionary of inventory types and years (e.g.,
                {'NEI':'2017', 'TRI':'2017'})
    :param NAICS_level: desired NAICS aggregation level, using
        sector_level_key, should match target_sector_level
    :param geo_scale: desired geographic aggregation level
        ('national', 'state', 'county'), should match target_geoscale
    :return: df
    """
    # update location to appropriate geoscale prior to aggregating
    df.dropna(subset=['Location'], inplace=True)
    df['Location'] = df['Location'].astype(str)
    df = update_geoscale(df, geo_scale)

    # assign grouping variables based on desired geographic aggregation level
    grouping_vars = ['NAICS_lvl', 'FlowName', 'Compartment', 'Location']
    if 'MetaSources' in df:
        grouping_vars.append('MetaSources')

    # aggregate by NAICS code, FlowName, compartment, and geographic level
    fbs = df.groupby(grouping_vars).agg({'FlowAmount': 'sum',
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
        fbs_mapped = fbs_list[0]
    else:
        fbs_mapped = pd.concat[fbs_list].reset_index(drop=True)

    # rename columns to match flowbysector format
    fbs_mapped = fbs_mapped.rename(columns={"NAICS_lvl": "SectorProducedBy"})

    # add hardcoded data, depending on the source data,
    # some of these fields may need to change
    fbs_mapped['Class'] = 'Chemicals'
    fbs_mapped['SectorConsumedBy'] = 'None'
    fbs_mapped['SectorSourceName'] = 'NAICS_2012_Code'

    fbs_mapped = assign_fips_location_system(
        fbs_mapped, list(inventory_dict.values())[0])

    # add missing flow by sector fields
    fbs_mapped = add_missing_flow_by_fields(fbs_mapped, flow_by_sector_fields)

    fbs_mapped = check_for_missing_sector_data(fbs_mapped, NAICS_level)

    # sort dataframe and reset index
    fbs_mapped = fbs_mapped.sort_values(
        list(flow_by_sector_fields.keys())).reset_index(drop=True)

    # check the sector codes to make sure NAICS 2012 codes
    fbs_mapped = replace_naics_w_naics_from_another_year(
        fbs_mapped, 'NAICS_2012_Code')

    return fbs_mapped


def naics_expansion(facility_NAICS):
    """
    modeled after sector_disaggregation in flowbyfunctions, updates NAICS
    to more granular sectors if there is only one naics at a lower level
    :param facility_NAICS: df of facilities from facility matcher with NAICS
    :return: df
    """

    # load naics 2 to naics 6 crosswalk
    cw_load = load_crosswalk('sector_length')
    cw = cw_load[['NAICS_4', 'NAICS_5', 'NAICS_6']]

    # subset the naics 4 and 5 columns
    cw4 = cw_load[['NAICS_4', 'NAICS_5']]
    cw4 = cw4.drop_duplicates(
        subset=['NAICS_4'], keep=False).reset_index(drop=True)
    naics4 = cw4['NAICS_4'].values.tolist()

    # subset the naics 5 and 6 columns
    cw5 = cw_load[['NAICS_5', 'NAICS_6']]
    cw5 = cw5.drop_duplicates(
        subset=['NAICS_5'], keep=False).reset_index(drop=True)
    naics5 = cw5['NAICS_5'].values.tolist()

    # for loop in reverse order longest length naics minus 1 to 2
    # appends missing naics levels to df
    for i in range(4, 6):
        if i == 4:
            sector_list = naics4
            sector_merge = "NAICS_4"
            sector_add = "NAICS_5"
        elif i == 5:
            sector_list = naics5
            sector_merge = "NAICS_5"
            sector_add = "NAICS_6"

        # subset df to NAICS with length = i
        df_subset = facility_NAICS.loc[facility_NAICS["NAICS"].apply(
            lambda x: len(x) == i)]

        # subset the df to the rows where the tmp sector columns are
        # in naics list
        df_subset = df_subset.loc[(df_subset['NAICS'].isin(sector_list))]

        # merge the naics cw
        new_naics = pd.merge(df_subset, cw[[sector_merge, sector_add]],
                             how='left', left_on=['NAICS'],
                             right_on=[sector_merge])
        # drop columns and rename new sector columns
        new_naics['NAICS'] = new_naics[sector_add]
        new_naics = new_naics.drop(columns=[sector_merge, sector_add])

        # drop records with NAICS that have now been expanded
        facility_NAICS = facility_NAICS[
            ~facility_NAICS['NAICS'].isin(sector_list)]

        # append new naics to df
        facility_NAICS = pd.concat([facility_NAICS, new_naics], sort=True)

    return facility_NAICS


def check_for_missing_sector_data(df, target_sector_level):
    """
    Modeled after validation.py check_if_losing_sector_data
    Allocates flow amount equally across child NAICS when parent NAICS
    is not target_level
    :param df: df
    :param target_sector_level: str, final sector level of FBS (ex. NAICS_6)
    :return: df with missing sector level data
    """

    from flowsa.dataclean import replace_NoneType_with_empty_cells
    from flowsa.dataclean import replace_strings_with_NoneType

    # temporarily replace null values with empty cells
    df = replace_NoneType_with_empty_cells(df)

    activity_field = "SectorProducedBy"
    rows_lost = pd.DataFrame()
    cw_load = load_crosswalk('sector_length')
    for i in range(3, sector_level_key[target_sector_level]):
        # create df of i length
        df_subset = df.loc[df[activity_field].apply(lambda x: len(x) == i)]

        # import cw and subset to current sector length and
        # target sector length

        nlength = list(sector_level_key.keys())[
            list(sector_level_key.values()).index(i)]
        cw = cw_load[[nlength, target_sector_level]].drop_duplicates()
        # add column with counts
        cw['sector_count'] = cw.groupby(nlength)[nlength].transform('count')

        # merge df & replace sector produced columns
        df_x = pd.merge(df_subset, cw, how='left',
                        left_on=[activity_field], right_on=[nlength])
        df_x[activity_field] = df_x[target_sector_level]
        df_x = df_x.drop(columns=[nlength, target_sector_level])

        # calculate new flow amounts, based on sector count,
        # allocating equally to the new sector length codes
        df_x['FlowAmount'] = df_x['FlowAmount'] / df_x['sector_count']
        df_x = df_x.drop(columns=['sector_count'])
        # replace null values with empty cells
        df_x = replace_NoneType_with_empty_cells(df_x)

        # append to df
        sector_list = df_subset[activity_field].drop_duplicates()
        if len(df_x) != 0:
            log.warning('Data found at %s digit NAICS to be allocated: '
                        '{}'.format(' '.join(map(str, sector_list))), str(i))
            rows_lost = rows_lost.append(df_x, ignore_index=True, sort=True)

    if len(rows_lost) == 0:
        log.info('No data loss from NAICS in dataframe')
    else:
        log.info('Allocating FlowAmounts equally to each %s',
                 target_sector_level)

    # add rows of missing data to the fbs sector subset
    df_allocated = pd.concat([df, rows_lost], ignore_index=True, sort=True)
    df_allocated = df_allocated.loc[
        df_allocated[activity_field].apply(
            lambda x: len(x) == sector_level_key[target_sector_level])]
    df_allocated.reset_index(inplace=True)

    # replace empty cells with NoneType (if dtype is object)
    df_allocated = replace_strings_with_NoneType(df_allocated)

    return df_allocated


def add_stewi_metadata(inventory_dict):
    """
    Access stewi metadata for generating FBS metdata file
    :param inventory_dict: a dictionary of inventory types and years (e.g.,
                {'NEI':'2017', 'TRI':'2017'})
    :return: combined dictionary of metadata from each inventory
    """
    from stewicombo.globals import compile_metadata
    return compile_metadata(inventory_dict)


if __name__ == "__main__":
    import flowsa
    flowsa.flowbysector.main(method='CAP_HAP_national_2017')
    #flowsa.flowbysector.main(method='TRI_DMR_national_2017')
