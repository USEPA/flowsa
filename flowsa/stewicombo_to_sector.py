# fbs_external.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to access data from external packages for use in flowbysector

These functions are called if referenced in flowbysectormethods as
data_format FBS_outside_flowsa with the function specified in FBS_datapull_fxn

"""

import pandas as pd
import os
from flowsa.flowbyfunctions import assign_fips_location_system, add_missing_flow_by_fields
from flowsa.mapping import map_elementary_flows
from flowsa.common import flow_by_sector_fields, apply_county_FIPS, sector_level_key, \
    update_geoscale

def stewicombo_to_sector(inventory_dict, NAICS_level, geo_scale, compartments):
    """
    This function takes the following inputs:
        - inventory_dict: a dictionary of inventory types and years (e.g., {'NEI':'2017', 'TRI':'2017'})
        - NAICS_level: desired NAICS aggregation level, using sector_level_key
        - geo_scale: desired geographic aggregation level ('national', 'state', 'county')
        - compartments: list of compartments to include (e.g., 'water', 'air', 'land')
    """
    
    from stewi.globals import output_dir as stw_output_dir
    from stewi.globals import weighted_average
    import stewicombo
    
    NAICS_level_value=sector_level_key[NAICS_level]
    ## run stewicombo to combine inventories, filter for LCI, remove overlap
    #df = stewicombo.combineFullInventories(inventory_dict, filter_for_LCI=True, remove_overlap=True, compartments=compartments)
    df = stewicombo.combineFullInventories(inventory_dict, filter_for_LCI=True, remove_overlap=True)    
    
    ## create mapping to convert facility IDs --> NAICS codes 
    facility_mapping = pd.DataFrame()
    # for all inventories in list:
    # - load facility data from stewi output directory, keeping only the facility IDs, NAICS codes, and geographic information
    # - create new column indicating inventory source (database and year)
    # - append data to master data frame
    inventory_list = list(inventory_dict.keys())
    for i in range(len(inventory_dict)):
        # define inventory name as inventory type + inventory year (e.g., NEI_2017)
        inventory_name = inventory_list[i] + '_' + list(inventory_dict.values())[i]
        facilities = pd.read_csv(stw_output_dir + 'facility/' + inventory_name + '.csv',
                                 usecols=['FacilityID', 'NAICS', 'State', 'County'],
                                 dtype={'FacilityID':str, 'NAICS':int})
        facilities['SourceYear'] = inventory_name
        facility_mapping = facility_mapping.append(facilities)

    # Apply FIPS to facility locations
    facility_mapping = apply_county_FIPS(facility_mapping)
         
    ## merge dataframes to assign NAICS codes based on facility IDs
    df['SourceYear'] = df['Source'] + '_' + df['Year']
    df = pd.merge(df, facility_mapping, how = 'left',
                  left_on=['FacilityID', 'SourceYear'],
                  right_on=['FacilityID', 'SourceYear'])
      
    ## subtract emissions for air transportation from airports in NEI
    # PLACEHOLDER TO SUBTRACT EMISSIONS FOR AIR TRANSPORT
    
    # add levelized NAICS code prior to aggregation
    df['NAICS_lvl'] = df['NAICS'].astype(str).str[0:NAICS_level_value]
    
    # update location to appropriate geoscale prior to aggregating
    df.dropna(subset=['Location'], inplace=True)
    df['Locationl']=df['Location'].astype(str)
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
    fbs = assign_fips_location_system(fbs, list(inventory_dict.values())[0])
    # add missing flow by sector fields
    fbs = add_missing_flow_by_fields(fbs, flow_by_sector_fields)
    # sort dataframe and reset index
    fbs = fbs.sort_values(list(flow_by_sector_fields.keys())).reset_index(drop=True)
    
    return fbs