# mapping.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Contains mapping functions
"""
import pandas as pd
import numpy as np
from flowsa.common import datapath, sector_source_name, activity_fields, load_source_catalog, \
    load_sector_crosswalk

def get_activitytosector_mapping(source):
    """
    Gets  the activity-to-sector mapping
    :param source: The data source name
    :return: a pandas df for a standard ActivitytoSector mapping
    """
    mapping = pd.read_csv(datapath+'activitytosectormapping/'+'Crosswalk_'+source+'_toNAICS.csv')
    return mapping


def add_sectors_to_flowbyactivity(flowbyactivity_df, sectorsourcename=sector_source_name):
    """
    Add Sectors from the Activity fields and mapped them to Sector from the crosswalk.
    No allocation is performed.
    :param flowbyactivity_df: A standard flowbyactivity data frame
    :param sectorsourcename: A sector source name, using package default
    :return: a df with activity fields mapped to 'sectors'
    """
    mappings = []

    #First check if source activities are NAICS like - if so make it into a mapping file

    cat = load_source_catalog()

    for s in pd.unique(flowbyactivity_df['SourceName']):
        src_info = cat[s]
        # if data are provided in NAICS format, use the mastercrosswalk
        if src_info['sector-like_activities']:
            cw = load_sector_crosswalk()
            sectors = cw.loc[:,[sector_source_name]]
            #Create mapping df that's just the sectors at first
            mapping = sectors.drop_duplicates()
            #Add the sector twice as activities so mapping is identical
            mapping['Activity'] = sectors[sector_source_name].copy()
            mapping = mapping.rename(columns={sector_source_name: "Sector"})
        else:
            # if source data activities are text strings, call on the manually created source crosswalks
            mapping = get_activitytosector_mapping(s)
            #filter by SectorSourceName of interest
            mapping = mapping[mapping['SectorSourceName']==sectorsourcename]
            #drop SectorSourceName
            mapping = mapping.drop(columns=['SectorSourceName'])
        mappings.append(mapping)
    mappings_df = pd.concat(mappings)
    #Merge in with flowbyactivity by
    flowbyactivity_wsector_df = flowbyactivity_df
    for k,v in activity_fields.items():
            sector_direction = k
            flowbyactivity_field = v[0]["flowbyactivity"]
            flowbysector_field = v[1]["flowbysector"]
            sector_type_field = sector_direction+'SectorType'
            mappings_df_tmp = mappings_df.rename(columns={'Activity':flowbyactivity_field,
                                                          'Sector':flowbysector_field,
                                                          'SectorType':sector_type_field})
            # column doesn't exist for sector-like activities, so ignore if error occurs
            mappings_df_tmp = mappings_df_tmp.drop(columns=['ActivitySourceName'], errors='ignore')
            #Merge them in. Critical this is a left merge to preserve all unmapped rows
            flowbyactivity_wsector_df = pd.merge(flowbyactivity_wsector_df,mappings_df_tmp, how='left')
            # replace nan in sector columns with none
            flowbyactivity_wsector_df[flowbysector_field] = flowbyactivity_wsector_df[flowbysector_field].replace(
                {np.nan: None}).astype(str)

    return flowbyactivity_wsector_df
