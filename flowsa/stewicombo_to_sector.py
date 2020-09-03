# -*- coding: utf-8 -*-
"""
Created on Thu Aug 20 11:25:04 2020

@author: EBell
"""

import stewicombo
import pandas as pd
import os
from facilitymatcher.globals import output_dir as fm_output_dir
from stewi.globals import output_dir as stw_output_dir
from stewi.globals import weighted_average
from flowsa.flowbyfunctions import assign_fips_location_system, add_missing_flow_by_fields
from flowsa.common import flow_by_sector_fields

try: modulepath = os.path.dirname(os.path.realpath(__file__)).replace('\\', '/') + '/'
except NameError: modulepath = 'flowsa/'
output_dir = modulepath + 'output/FlowbySector/'

def stewicombo_to_sector(inventory_dict, NAICS_level, geo_level, compartments):
    """
    This function takes the following inputs:
        - inventory_dict: a dictionary of inventory types and years (e.g., {'NEI':'2017', 'TRI':'2017'})
        - NAICS_level: desired NAICS aggregation level (2-6)
        - geo_level: desired geographic aggregation level ('National', 'State', 'County')
        - compartments: list of compartments to include (e.g., 'water', 'air', 'land')
    """
    
    ## run stewicombo to combine inventories, filter for LCI, remove overlap
    df = stewicombo.combineFullInventories(inventory_dict, filter_for_LCI=True, remove_overlap=True, compartments=compartments)
        
    ## create mapping to convert facility IDs --> NAICS codes 
    facility_mapping = pd.DataFrame()
    # for all inventories in list:
    # - load facility data from stewi output directory, keeping only the facility IDs, NAICS codes, and geographic information
    # - create new column indicating inventory source (database and year)
    # - append data to master data frame
    for i in range(len(inventory_dict)):
        # define inventory name as inventory type + inventory year (e.g., NEI_2017)
        inventory_name = list(inventory_dict.keys())[i] + '_' + list(inventory_dict.values())[i]
        facilities = pd.read_csv(stw_output_dir + 'facility/' + inventory_name + '.csv',
                                 usecols=['FacilityID', 'NAICS', 'State', 'County'],
                                 dtype={'FacilityID':str, 'NAICS':int})
        # rename counties as County + State (e.g., Bristol_MA), since some states share county names
        facilities['County'] = facilities['County'] + '_' + facilities['State']
        facilities['SourceYear'] = inventory_name
        facility_mapping = facility_mapping.append(facilities)
    
    ## merge dataframes to assign NAICS codes based on facility IDs
    df['SourceYear'] = df['Source'] + '_' + df['Year']
    df = pd.merge(df, facility_mapping, how = 'left',
                  left_on=['FacilityID', 'SourceYear'],
                  right_on=['FacilityID', 'SourceYear'])
      
    ## subtract emissions for air transportation from airports
    # PLACEHOLDER TO SUBTRACT EMISSIONS FOR AIR TRANSPORT
    
    ## aggregate data based on NAICS code and chemical ID
    # add levelized NAICS code
    df['NAICS_lvl'] = df['NAICS'].astype(str).str[0:NAICS_level]
    # assign grouping variables based on desired geographic aggregation level
    if geo_level == 'National':
        grouping_vars = ['NAICS_lvl', 'SRS_ID', 'Compartment']
    elif geo_level == 'State':
        grouping_vars = ['NAICS_lvl', 'SRS_ID', 'Compartment', 'State']
    elif geo_level == 'County':
        grouping_vars = ['NAICS_lvl', 'SRS_ID', 'Compartment', 'County']
    # aggregate by NAICS code, chemical ID, compartment, and geographic level
    fbs = df.groupby(grouping_vars).agg({'FlowAmount':'sum', 
                                         'NAICS_lvl':'first',
                                         'Compartment':'first',
                                         'FlowName':'first',
                                         'Year':'first',
                                         'Unit':'first',
                                         'State':'first',
                                         'County':'first'})
    # add reliability score
    fbs['DataReliability']=weighted_average(df, 'ReliabilityScore', 'FlowAmount', grouping_vars)
    
    ## perform operations to match flowbysector format
    # rename columns to match flowbysector format
    fbs = fbs.rename(columns={"NAICS_lvl": "SectorProducedBy",
                              "FlowName": "Flowable",
                              "Compartment": "Context"})
    # add hardcoded data
    fbs['National'] = 'United States'
    fbs['Class'] = 'Chemicals'
    fbs['SectorConsumedBy'] = 'None'
    fbs['Location'] = fbs[geo_level]
    fbs = assign_fips_location_system(fbs, list(inventory_dict.values())[0])
    # add missing flow by sector fields
    fbs = add_missing_flow_by_fields(fbs, flow_by_sector_fields)
    # sort dataframe and reset index
    fbs = fbs.sort_values(list(flow_by_sector_fields.keys())).reset_index(drop=True)
    
    ## save result to output directory
    fbs.to_csv(output_dir + 'Chemicals_' + geo_level + '.csv')