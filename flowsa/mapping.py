# usgs_water_consume.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Contains mapping functions
"""
import pandas as pd
from flowsa.common import datapath, sector_source_name

def get_activitytosector_mapping(source):
    """
    Gets  the activity-to-sector mapping
    :param source: The data source name
    :return: a pandas df for a standard ActivitytoSector mapping
    """
    mapping = pd.read_csv(datapath+source+'Crosswalk_'+source+'toNAICS.csv')
    return mapping

def map_activities_to_sector(flowbyactivity_df, sectorsourcename=sector_source_name):
    """
    Take the Activity fields and mapped them to Sector from the crosswalk.
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
    #Merge in with flowbyactivity
    flowbysector_df = pd.merge(flowbyactivity_df,mappings_df)
    return flowbysector_df


