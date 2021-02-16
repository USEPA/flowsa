# fbs_external.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to access data from external packages for use in flowbysector

These functions are called if referenced in flowbysectormethods as
data_format FBS_outside_flowsa with the function specified in FBS_datapull_fxn

"""

import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system, add_missing_flow_by_fields
from flowsa.mapping import map_elementary_flows
from flowsa.common import flow_by_sector_fields, apply_county_FIPS, sector_level_key, \
    update_geoscale, log, load_sector_length_crosswalk
from flowsa.datachecks import replace_naics_w_naics_from_another_year

def stewicombo_to_sector(inventory_dict, NAICS_level, geo_scale, compartments):
    """
    Returns emissions from stewicombo in fbs format, requires stewi >= 0.9.5
    :param inventory_dict: a dictionary of inventory types and years (e.g., 
                {'NEI':'2017', 'TRI':'2017'})
    :param NAICS_level: desired NAICS aggregation level, using sector_level_key,
                should match target_sector_level
    :param geo_scale: desired geographic aggregation level ('national', 'state',
                'county'), should match target_geoscale
    :param compartments: list of compartments to include (e.g., 'water', 'air',
                'soil'), use None to include all compartments
    """

    import stewicombo
    from flowsa.EPA_NEI import drop_GHGs

    NAICS_level_value=sector_level_key[NAICS_level]
    ## run stewicombo to combine inventories, filter for LCI, remove overlap
    df = stewicombo.combineFullInventories(inventory_dict, filter_for_LCI=True, remove_overlap=True, compartments=compartments)
    df.drop(columns = ['SRS_CAS','SRS_ID','FacilityIDs_Combined'], inplace=True)

    inventory_list = list(inventory_dict.keys())

    if 'NEI' in inventory_list and not 'GHGRP' in inventory_list:
        df = drop_GHGs(df)
    facility_mapping = extract_facility_data(inventory_dict)
    # use NAICS from facility matcher so drop them here
    facility_mapping.drop(columns = ['NAICS'], inplace = True)
    # merge dataframes to assign facility information based on facility IDs
    df = pd.merge(df, facility_mapping, how = 'left',
                  on= 'FacilityID')
    
    all_NAICS = obtain_NAICS_from_facility_matcher(inventory_list)
    df = pd.merge(df, all_NAICS, how = 'left', on = ['FRS_ID','Source'])

    # add levelized NAICS code prior to aggregation
    df['NAICS_lvl'] = df['NAICS'].str[0:NAICS_level_value]

    if 'NEI' in inventory_list:
        df = reassign_airplane_emissions(df, inventory_dict['NEI'], NAICS_level_value)
        
    df['MetaSources'] = df['Source']
    
    fbs = prepare_stewi_fbs(df, inventory_dict, NAICS_level, geo_scale)

    return fbs

def stewi_to_sector(inventory_dict, NAICS_level, geo_scale, compartments = None):
    """
    Returns emissions from stewi in fbs format, requires stewi >= 0.9.5
    :param inventory_dict: a dictionary of inventory types and years (e.g., 
                {'NEI':'2017', 'TRI':'2017'})
    :param NAICS_level: desired NAICS aggregation level, using sector_level_key,
                should match target_sector_level
    :param geo_scale: desired geographic aggregation level ('national', 'state',
                'county'), should match target_geoscale
    :param compartments: list of compartments to include (e.g., 'water', 'air',
                'soil'), use None to include all compartments
    """
    import stewi

    NAICS_level_value=sector_level_key[NAICS_level]
    # run stewicombo to combine inventories, filter for LCI, remove overlap
    df = pd.DataFrame()
    for database, year in inventory_dict.items():
        inv = stewi.getInventory(database, year, filter_for_LCI=True, US_States_Only=True)
        inv['Year'] = year
        inv['MetaSources'] = database
        df = df.append(inv)
    if compartments != None:
        df = df[df['Compartment'].isin(compartments)]
    facility_mapping = extract_facility_data(inventory_dict)
    # Convert NAICS to string (first to int to avoid decimals)
    facility_mapping['NAICS'] =  facility_mapping['NAICS'].astype(int).astype(str)
    facility_mapping = naics_expansion(facility_mapping)

    # merge dataframes to assign facility information based on facility IDs
    df = pd.merge(df, facility_mapping, how = 'left',
                  on= 'FacilityID')

    # add levelized NAICS code prior to aggregation
    df['NAICS_lvl'] = df['NAICS'].str[0:NAICS_level_value]
    
    fbs = prepare_stewi_fbs(df, inventory_dict, NAICS_level, geo_scale)

    return fbs    

def reassign_airplane_emissions(df, year, NAICS_level_value):
    import stewi
    from stewicombo.overlaphandler import remove_default_flow_overlaps
    from stewicombo.globals import addChemicalMatches
    
    ## subtract emissions for air transportation from airports in NEI
    airport_NAICS = '4881'
    air_transportation_SCC = '2275020000'
    air_transportation_naics = '481111'
    log.info('Reassigning emissions from air transportation from airports')

    # obtain and prepare SCC dataset
    df_airplanes = stewi.getInventory('NEI', year,
                                      stewiformat='flowbySCC')
    df_airplanes = df_airplanes[df_airplanes['SCC']==air_transportation_SCC]
    df_airplanes['Source']='NEI'
    df_airplanes = addChemicalMatches(df_airplanes)
    df_airplanes = remove_default_flow_overlaps(df_airplanes, SCC=True)
    df_airplanes.drop(columns=['SCC'], inplace=True)
    
    facility_mapping_air = df[['FacilityID','NAICS']]
    facility_mapping_air.drop_duplicates(keep = 'first', inplace = True)
    df_airplanes = df_airplanes.merge(facility_mapping_air, how = 'left',
                                      on='FacilityID')

    df_airplanes['Year']=year
    df_airplanes = df_airplanes[
        df_airplanes['NAICS'].str[0:len(airport_NAICS)]==airport_NAICS]

    # subtract airplane emissions from airport NAICS at individual facilities
    df_planeemissions = df_airplanes[['FacilityID','FlowName','FlowAmount']] 
    df_planeemissions.rename(columns={'FlowAmount':'PlaneEmissions'}, inplace=True)
    df = df.merge(df_planeemissions, how = 'left',
                                    on = ['FacilityID','FlowName'])
    df[['PlaneEmissions']] = df[['PlaneEmissions']].fillna(value=0)       
    df['FlowAmount']=df['FlowAmount']-df['PlaneEmissions']
    df.drop(columns=['PlaneEmissions'], inplace=True)
    
    # add airplane emissions under air transport NAICS
    df_airplanes.loc[:,'NAICS_lvl']=air_transportation_naics[0:NAICS_level_value]
    df = pd.concat([df, df_airplanes], ignore_index=True)
    
    return df

def extract_facility_data(inventory_dict):
    import stewi
    facility_mapping = pd.DataFrame()
    # load facility data from stewi output directory, keeping only the facility IDs, and geographic information
    inventory_list = list(inventory_dict.keys())

    for i in range(len(inventory_dict)):
        # define inventory name as inventory type + inventory year (e.g., NEI_2017)
        database = inventory_list[i]
        year = list(inventory_dict.values())[i]
        inventory_name = database + '_' + year
        facilities = stewi.getInventoryFacilities(database, year)
        facilities = facilities[['FacilityID', 'State', 'County','NAICS']]
        if len(facilities[facilities.duplicated(subset='FacilityID',keep=False)])>0:
            log.info('Duplicate facilities in '+ inventory_name +' - keeping first listed')
            facilities.drop_duplicates(subset='FacilityID',
                                       keep='first', inplace = True)
        facility_mapping = facility_mapping.append(facilities)
    
    # Apply FIPS to facility locations
    facility_mapping = apply_county_FIPS(facility_mapping)
    
    return facility_mapping

def obtain_NAICS_from_facility_matcher(inventory_list):
    import facilitymatcher
    ## Access NAICS From facility matcher and assign based on FRS_ID
    all_NAICS = facilitymatcher.get_FRS_NAICSInfo_for_facility_list(frs_id_list = None,
        inventories_of_interest_list=inventory_list)
    all_NAICS = all_NAICS.loc[all_NAICS['PRIMARY_INDICATOR']=='PRIMARY']
    all_NAICS.drop(columns=['PRIMARY_INDICATOR'], inplace=True)
    all_NAICS = naics_expansion(all_NAICS)
    if len(all_NAICS[all_NAICS.duplicated(subset = ['FRS_ID','Source'], keep = False)])>0:
        log.info('Duplicate primary NAICS reported - keeping first')
        all_NAICS.drop_duplicates(subset = ['FRS_ID','Source'], 
                                  keep = 'first', inplace = True)
    return all_NAICS

def prepare_stewi_fbs(df, inventory_dict, NAICS_level, geo_scale):
    from stewi.globals import weighted_average

    # update location to appropriate geoscale prior to aggregating
    df.dropna(subset=['Location'], inplace=True)
    df['Location']=df['Location'].astype(str)
    df = update_geoscale(df, geo_scale)

    # assign grouping variables based on desired geographic aggregation level
    grouping_vars = ['NAICS_lvl', 'FlowName', 'Compartment', 'Location']
    if 'MetaSources' in df:
        grouping_vars.append('MetaSources')

    # aggregate by NAICS code, FlowName, compartment, and geographic level
    fbs = df.groupby(grouping_vars).agg({'FlowAmount':'sum', 
                                         'Year':'first',
                                         'Unit':'first'})
  
    # add reliability score
    fbs['DataReliability']=weighted_average(df, 'DataReliability', 'FlowAmount', grouping_vars)
    fbs.reset_index(inplace=True)
    
    # apply flow mapping
    fbs = map_elementary_flows(fbs, list(inventory_dict.keys()))
    
    # rename columns to match flowbysector format
    fbs = fbs.rename(columns={"NAICS_lvl": "SectorProducedBy"})
    
    # add hardcoded data, depending on the source data, some of these fields may need to change
    fbs['Class'] = 'Chemicals'
    fbs['SectorConsumedBy'] = 'None'
    fbs['SectorSourceName'] = 'NAICS_2012_Code'
    fbs['FlowType'] = 'ELEMENTARY_FLOW'
    
    fbs = assign_fips_location_system(fbs, list(inventory_dict.values())[0])

    # add missing flow by sector fields
    fbs = add_missing_flow_by_fields(fbs, flow_by_sector_fields)
    
    fbs = check_for_missing_sector_data(fbs, NAICS_level)

    # sort dataframe and reset index
    fbs = fbs.sort_values(list(flow_by_sector_fields.keys())).reset_index(drop=True)

    # check the sector codes to make sure NAICS 2012 codes
    fbs = replace_naics_w_naics_from_another_year(fbs, 'NAICS_2012_Code')

    return fbs 

def naics_expansion(facility_NAICS):
    """ modeled after sector_disaggregation in flowbyfunctions, updates NAICS 
    to more granular sectors if there is only one naics at a lower level
    :param facility_NAICS: df of facilities from facility matcher with NAICS
    """

    # load naics 2 to naics 6 crosswalk
    cw_load = load_sector_length_crosswalk()
    cw = cw_load[['NAICS_4', 'NAICS_5', 'NAICS_6']]

    # subset the naics 4 and 5 columns
    cw4 = cw_load[['NAICS_4', 'NAICS_5']]
    cw4 = cw4.drop_duplicates(subset=['NAICS_4'], keep=False).reset_index(drop=True)
    naics4 = cw4['NAICS_4'].values.tolist()

    # subset the naics 5 and 6 columns
    cw5 = cw_load[['NAICS_5', 'NAICS_6']]
    cw5 = cw5.drop_duplicates(subset=['NAICS_5'], keep=False).reset_index(drop=True)
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
        df_subset = facility_NAICS.loc[facility_NAICS["NAICS"].apply(lambda x: len(x) == i)]

        # subset the df to the rows where the tmp sector columns are in naics list
        df_subset = df_subset.loc[(df_subset['NAICS'].isin(sector_list))]

        # merge the naics cw
        new_naics = pd.merge(df_subset, cw[[sector_merge, sector_add]],
                             how='left', left_on=['NAICS'], right_on=[sector_merge])
        # drop columns and rename new sector columns
        new_naics['NAICS'] = new_naics[sector_add]
        new_naics = new_naics.drop(columns=[sector_merge, sector_add])

        # drop records with NAICS that have now been expanded
        facility_NAICS = facility_NAICS[~facility_NAICS['NAICS'].isin(sector_list)]

        # append new naics to df
        facility_NAICS = pd.concat([facility_NAICS, new_naics], sort=True)

    return facility_NAICS

def check_for_missing_sector_data(df, target_sector_level):
    """
    Modeled after datachecks.py check_if_losing_sector_data
    Allocates flow amount equally across child NAICS when parent NAICS is not target_level
    :param df:
    :param target_sector_level:
    :return:
    """

    from flowsa.flowbyfunctions import replace_NoneType_with_empty_cells, replace_strings_with_NoneType

    # temporarily replace null values with empty cells
    df = replace_NoneType_with_empty_cells(df)

    activity_field = "SectorProducedBy"
    rows_lost = pd.DataFrame()
    cw_load = load_sector_length_crosswalk()
    for i in range(3, sector_level_key[target_sector_level]):
        # create df of i length
        df_subset = df.loc[df[activity_field].apply(lambda x: len(x) == i)]

        # import cw and subset to current sector length and target sector length

        nlength = list(sector_level_key.keys())[list(sector_level_key.values()).index(i)]
        cw = cw_load[[nlength, target_sector_level]].drop_duplicates()
        # add column with counts
        cw['sector_count'] = cw.groupby(nlength)[nlength].transform('count')

        # merge df & replace sector produced columns
        df_x = pd.merge(df_subset, cw, how='left', left_on=[activity_field], right_on=[nlength])
        df_x[activity_field]=df_x[target_sector_level]
        df_x= df_x.drop(columns=[nlength, target_sector_level])

        # calculate new flow amounts, based on sector count, allocating equally to the new sector length codes
        df_x['FlowAmount'] = df_x['FlowAmount'] / df_x['sector_count']
        df_x = df_x.drop(columns=['sector_count'])
        # replace null values with empty cells
        df_x = replace_NoneType_with_empty_cells(df_x)

        # append to df
        sector_list = df_subset[activity_field].drop_duplicates()
        if len(df_x) != 0:
            log.warning('Data found at ' + str(i) + ' digit NAICS to be allocated'
                                                    ': {}'.format(' '.join(map(str, sector_list))))
            rows_lost = rows_lost.append(df_x, ignore_index=True, sort=True)

    if len(rows_lost) == 0:
        log.info('No data loss from NAICS in dataframe')
    else:
        log.info('Allocating FlowAmounts equally to each ' + target_sector_level)

    # add rows of missing data to the fbs sector subset
    df_allocated = pd.concat([df, rows_lost], ignore_index=True, sort=True)
    df_allocated = df_allocated.loc[df_allocated[activity_field].apply(lambda x: len(x)==sector_level_key[target_sector_level])]
    df_allocated.reset_index(inplace=True)

    # replace empty cells with NoneType (if dtype is object)
    df_allocated = replace_strings_with_NoneType(df_allocated)

    return df_allocated
