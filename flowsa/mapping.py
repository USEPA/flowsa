# mapping.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Contains mapping functions
"""
import pandas as pd
from flowsa.common import datapath, sector_source_name, activity_fields

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
    for s in pd.unique(flowbyactivity_df['SourceName']):
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
            mappings_df_tmp = mappings_df_tmp.drop(columns=['ActivitySourceName'])
            #Merge them in. Critical this is a left merge to preserve all unmapped rows
            flowbyactivity_wsector_df = pd.merge(flowbyactivity_wsector_df,mappings_df_tmp, how='left')

    return flowbyactivity_wsector_df


