# mapping.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Contains mapping functions
"""
import pandas as pd
import numpy as np
from flowsa.common import datapath, sector_source_name, activity_fields, load_source_catalog, \
    load_sector_crosswalk, log
from flowsa.flowbyfunctions import fbs_activity_fields

def get_activitytosector_mapping(source):
    """
    Gets  the activity-to-sector mapping
    :param source: The data source name
    :return: a pandas df for a standard ActivitytoSector mapping
    """
    mapping = pd.read_csv(datapath+'activitytosectormapping/'+'Crosswalk_'+source+'_toNAICS.csv')
    return mapping


def add_sectors_to_flowbyactivity(flowbyactivity_df, sectorsourcename=sector_source_name, levelofSectoragg='disagg'):
    """
    Add Sectors from the Activity fields and mapped them to Sector from the crosswalk.
    No allocation is performed.
    :param flowbyactivity_df: A standard flowbyactivity data frame
    :param sectorsourcename: A sector source name, using package default
    :param levelofSectoragg: Option of mapping to the most aggregated "agg" or the most disaggregated "disagg" level
                            of NAICS for an activity
    :return: a df with activity fields mapped to 'sectors'
    """

    # todo: need to add the domestic codes to crosswalk
    # todo: create fxn that does so

    mappings = []

    # First check if source activities are NAICS like - if so make it into a mapping file

    cat = load_source_catalog()

    for s in pd.unique(flowbyactivity_df['SourceName']):
        src_info = cat[s]
        # if data are provided in NAICS format, use the mastercrosswalk
        if src_info['sector-like_activities'] == 'True':
            cw = load_sector_crosswalk()
            sectors = cw.loc[:,[sector_source_name]]
            # Create mapping df that's just the sectors at first
            mapping = sectors.drop_duplicates()
            # Add the sector twice as activities so mapping is identical
            mapping['Activity'] = sectors[sector_source_name]
            mapping = mapping.rename(columns={sector_source_name: "Sector"})
        else:
            # if source data activities are text strings, call on the manually created source crosswalks
            mapping = get_activitytosector_mapping(s)
            # filter by SectorSourceName of interest
            mapping = mapping[mapping['SectorSourceName']==sectorsourcename]
            # drop SectorSourceName
            mapping = mapping.drop(columns=['SectorSourceName'])
            # Include all digits of naics in mapping, if levelofNAICSagg is specified as "disagg"
            if levelofSectoragg == 'disagg':
                mapping = expand_naics_list(mapping, sectorsourcename)
        mappings.append(mapping)
    mappings_df = pd.concat(mappings)
    # Merge in with flowbyactivity by
    flowbyactivity_wsector_df = flowbyactivity_df
    for k, v in activity_fields.items():
            sector_direction = k
            flowbyactivity_field = v[0]["flowbyactivity"]
            flowbysector_field = v[1]["flowbysector"]
            sector_type_field = sector_direction+'SectorType'
            mappings_df_tmp = mappings_df.rename(columns={'Activity':flowbyactivity_field,
                                                          'Sector':flowbysector_field,
                                                          'SectorType':sector_type_field})
            # column doesn't exist for sector-like activities, so ignore if error occurs
            mappings_df_tmp = mappings_df_tmp.drop(columns=['ActivitySourceName'], errors='ignore')
            # Merge them in. Critical this is a left merge to preserve all unmapped rows
            flowbyactivity_wsector_df = pd.merge(flowbyactivity_wsector_df,mappings_df_tmp, how='left', on=flowbyactivity_field)
            # replace nan in sector columns with none
            flowbyactivity_wsector_df[flowbysector_field] = flowbyactivity_wsector_df[flowbysector_field].replace(
                {np.nan: None}).astype(str)

    return flowbyactivity_wsector_df


def expand_naics_list(df, sectorsourcename):

    # load master crosswalk
    cw = load_sector_crosswalk()
    sectors = cw.loc[:, [sectorsourcename]]
    # Create mapping df that's just the sectors at first
    sectors = sectors.drop_duplicates().dropna()

    # fill null values
    df['Sector'] = df['Sector'].astype('str')

    naics_df = pd.DataFrame([])
    for i in df['Sector']:
        dig = len(str(i))
        n = sectors.loc[sectors[sectorsourcename].apply(lambda x: str(x[0:dig])) == i]
        n['Sector'] = i
        naics_df = naics_df.append(n)

    # merge df to retain activityname/sectortype info
    naics_expanded = df.merge(naics_df, how='left')
    # drop column of aggregated naics and rename column of disaggregated naics
    naics_expanded = naics_expanded.drop(columns=["Sector"])
    naics_expanded = naics_expanded.rename(columns={sectorsourcename: 'Sector'})
    # drop duplicates and rearrange df columns
    naics_expanded = naics_expanded.drop_duplicates()
    naics_expanded = naics_expanded[['ActivitySourceName', 'Activity', 'Sector', 'SectorType']]

    return naics_expanded


def get_fba_allocation_subset(fba_allocation, source, activitynames):
    """
    Subset the fba allocation data based on NAICS associated with activity
    :param fba_allocation:
    :param sourcename:
    :param activitynames:
    :return:
    """

    # read in source crosswalk
    df = pd.read_csv(datapath+'activitytosectormapping/'+'Crosswalk_'+source+'_toNAICS.csv')
    sector_source_name = df['SectorSourceName'].all()
    df = expand_naics_list(df, sector_source_name)
    # subset source crosswalk to only contain values pertaining to list of activity names
    df = df.loc[df['Activity'].isin(activitynames)]
    # turn column of sectors related to activity names into list
    sector_list = pd.unique(df['Sector']).tolist()
    # subset fba allocation table to the values in the activity list, based on overlapping sectors
    fba_allocation_subset = fba_allocation.loc[(fba_allocation[fbs_activity_fields[0]].isin(sector_list)) |
                                               (fba_allocation[fbs_activity_fields[1]].isin(sector_list))]

    return fba_allocation_subset


def map_elementary_flows(fba, from_fba_source):
    """
    Applies mapping from fedelemflowlist to convert flows to fedelemflowlist flows
    :param fba: df flow-by-activity or flow-by-sector with 'Flowable', 'Context', and 'Unit' fields
    :param from_fba_source: str Source name of fba list to look for mappings
    :return:
    """
    from fedelemflowlist import get_flowmapping

    flowmapping = get_flowmapping(from_fba_source)
    mapping_fields = ["SourceListName",
                      "SourceFlowName",
                      "SourceFlowContext",
                      "SourceUnit",
                      "ConversionFactor",
                      "TargetFlowName",
                      "TargetFlowContext",
                      "TargetUnit"]
    if flowmapping.empty:
        log.ERROR("No mapping file in fedelemflowlist found for " + from_fba_source)
    flowmapping = flowmapping[mapping_fields]
    # merge fba with flows
    fba_mapped_df = pd.merge(fba, flowmapping,
                             left_on=["Flowable", "Context"],
                             right_on=["SourceFlowName", "SourceFlowContext"],
                             how="left")
    fba_mapped_df.loc[
        fba_mapped_df["TargetFlowName"].notnull(), "Flowable"
    ] = fba_mapped_df["TargetFlowName"]
    fba_mapped_df.loc[
        fba_mapped_df["TargetFlowName"].notnull(), "Context"
    ] = fba_mapped_df["TargetFlowContext"]
    fba_mapped_df.loc[fba_mapped_df["TargetFlowName"].notnull(), "Unit"] = fba_mapped_df[
        "TargetUnit"
    ]
    fba_mapped_df.loc[fba_mapped_df["TargetFlowName"].notnull(), "FlowAmount"] = \
        fba_mapped_df["FlowAmount"] * fba_mapped_df["ConversionFactor"]

    # drop
    fba_mapped_df = fba_mapped_df.drop(
        columns=mapping_fields)
    return fba_mapped_df
