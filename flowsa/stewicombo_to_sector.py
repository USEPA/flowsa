# fbs_external.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to access data from external packages for use in flowbysector

These functions are called if referenced in flowbysectormethods as
data_format FBS_outside_flowsa with the function specified in FBS_datapull_fxn

"""

import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system, add_missing_flow_by_fields,\
    sector_disaggregation
from flowsa.mapping import map_elementary_flows
from flowsa.common import flow_by_sector_fields, apply_county_FIPS, sector_level_key, \
    update_geoscale, log

def stewicombo_to_sector(inventory_dict, NAICS_level, geo_scale, compartments):
    """
    Returns emissions from stewicombo in fbs format
    :param inventory_dict: a dictionary of inventory types and years (e.g., 
                {'NEI':'2017', 'TRI':'2017'})
    :param NAICS_level: desired NAICS aggregation level, using sector_level_key,
                should match target_sector_level
    :param geo_scale: desired geographic aggregation level ('national', 'state',
                'county'), should match target_geoscale
    :param compartments: list of compartments to include (e.g., 'water', 'air',
                'soil'), use None to include all compartments
    """
    
    from stewi.globals import output_dir as stw_output_dir
    from stewi.globals import weighted_average
    import stewi
    import stewicombo
    import facilitymatcher
    from stewicombo.overlaphandler import remove_default_flow_overlaps
    from stewicombo.globals import addChemicalMatches
    from facilitymatcher import output_dir as fm_output_dir

    NAICS_level_value=sector_level_key[NAICS_level]
    ## run stewicombo to combine inventories, filter for LCI, remove overlap
    df = stewicombo.combineFullInventories(inventory_dict, filter_for_LCI=True, remove_overlap=True, compartments=compartments)
    df.drop(columns = ['SRS_CAS','SRS_ID','FacilityIDs_Combined'], inplace=True)

    facility_mapping = pd.DataFrame()
    # load facility data from stewi output directory, keeping only the facility IDs, and geographic information
    inventory_list = list(inventory_dict.keys())
    for i in range(len(inventory_dict)):
        # define inventory name as inventory type + inventory year (e.g., NEI_2017)
        inventory_name = inventory_list[i] + '_' + list(inventory_dict.values())[i]
        facilities = pd.read_csv(stw_output_dir + 'facility/' + inventory_name + '.csv',
                                 usecols=['FacilityID', 'State', 'County'],
                                 dtype={'FacilityID':str})
        if len(facilities[facilities.duplicated(subset='FacilityID',keep=False)])>0:
            log.info('Duplicate facilities in '+ inventory_name +' - keeping first listed')
            facilities.drop_duplicates(subset='FacilityID',
                                       keep='first', inplace = True)
        facility_mapping = facility_mapping.append(facilities)

    # Apply FIPS to facility locations
    facility_mapping = apply_county_FIPS(facility_mapping)
         
    ## merge dataframes to assign facility information based on facility IDs
    df = pd.merge(df, facility_mapping, how = 'left',
                  on= 'FacilityID')
    
    ## Access NAICS From facility matcher and assign based on FRS_ID
    all_NAICS = facilitymatcher.get_FRS_NAICSInfo_for_facility_list(frs_id_list = None,
        inventories_of_interest_list=inventory_list)
    all_NAICS = all_NAICS.loc[all_NAICS['PRIMARY_INDICATOR']=='PRIMARY']
    all_NAICS.drop(columns=['PRIMARY_INDICATOR'], inplace=True)
    if len(all_NAICS[all_NAICS.duplicated(subset = ['FRS_ID','Source'], keep = False)])>0:
        log.info('Duplicate primary NAICS reported - keeping first')
        all_NAICS.drop_duplicates(subset = ['FRS_ID','Source'], 
                                  keep = 'first', inplace = True)
    df = pd.merge(df, all_NAICS, how = 'left', on = ['FRS_ID','Source'])

    # add levelized NAICS code prior to aggregation
    df['NAICS_lvl'] = df['NAICS'].str[0:NAICS_level_value]

    ## subtract emissions for air transportation from airports in NEI
    airport_NAICS = '4881'
    air_transportation_SCC = '2275020000'
    air_transportation_naics = '481111'
    if 'NEI' in inventory_list:
        log.info('Reassigning emissions from air transportation from airports')

        # obtain and prepare SCC dataset
        df_airplanes = stewi.getInventory('NEI', inventory_dict['NEI'],
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

        df_airplanes['Year']=inventory_dict['NEI']
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
        
    # update location to appropriate geoscale prior to aggregating
    df.dropna(subset=['Location'], inplace=True)
    df['Location']=df['Location'].astype(str)
    df = update_geoscale(df, geo_scale)

    # assign grouping variables based on desired geographic aggregation level
    grouping_vars = ['NAICS_lvl', 'FlowName', 'Compartment', 'Location']

    # aggregate by NAICS code, FlowName, compartment, and geographic level
    fbs = df.groupby(grouping_vars).agg({'FlowAmount':'sum', 
                                         'Year':'first',
                                         'Unit':'first'})
  
    # add reliability score
    fbs['DataReliability']=weighted_average(df, 'ReliabilityScore', 'FlowAmount', grouping_vars)
    fbs.reset_index(inplace=True)
    
    # apply flow mapping
    fbs = map_elementary_flows(fbs, inventory_list)
    
    # rename columns to match flowbysector format
    fbs = fbs.rename(columns={"NAICS_lvl": "SectorProducedBy"})
    
    # add hardcoded data, depending on the source data, some of these fields may need to change
    fbs['Class'] = 'Chemicals'
    fbs['SectorConsumedBy'] = 'None'
    fbs['SectorSourceName'] = 'NAICS_2012_Code'
    fbs['FlowType'] = 'ELEMENTARY_FLOW'

    # carry out to 6-digit NAICS where possible
    #fbs = sector_disaggregation(fbs)
    
    fbs = assign_fips_location_system(fbs, list(inventory_dict.values())[0])
    # add missing flow by sector fields
    fbs = add_missing_flow_by_fields(fbs, flow_by_sector_fields)
    
    # sort dataframe and reset index
    fbs = fbs.sort_values(list(flow_by_sector_fields.keys())).reset_index(drop=True)

    return fbs
