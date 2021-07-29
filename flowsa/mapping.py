# mapping.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Contains mapping functions
"""
import pandas as pd
import numpy as np
from flowsa.common import datapath, SECTOR_SOURCE_NAME, activity_fields, load_source_catalog, \
    load_sector_crosswalk, log, fba_activity_fields
from flowsa.flowbyfunctions import fbs_activity_fields, load_sector_length_crosswalk
from flowsa.datachecks import replace_naics_w_naics_from_another_year


def get_activitytosector_mapping(source):
    """
    Gets  the activity-to-sector mapping
    :param source: str, the data source name
    :return: a pandas df for a standard ActivitytoSector mapping
    """
    if 'EPA_NEI' in source:
        source = 'SCC'
    if 'BEA' in source:
        source = 'BEA_2012_Detail'
    mapping = pd.read_csv(datapath+'activitytosectormapping/'+'Crosswalk_'+source+'_toNAICS.csv',
                          dtype={'Activity': 'str',
                                 'Sector': 'str'})
    return mapping


def add_sectors_to_flowbyactivity(flowbyactivity_df, sectorsourcename=SECTOR_SOURCE_NAME, **kwargs):
    """
    Add Sectors from the Activity fields and mapped them to Sector from the crosswalk.
    No allocation is performed.
    :param flowbyactivity_df: A standard flowbyactivity data frame
    :param sectorsourcename: A sector source name, using package default
    :param kwargs: option to include the parameter 'allocationmethod',
    which modifies function behavoir if = 'direct'
    :return: a df with activity fields mapped to 'sectors'
    """

    # First check if source activities are NAICS like - if so make it into a mapping file
    cat = load_source_catalog()

    # for s in pd.unique(flowbyactivity_df['SourceName']):
    s = pd.unique(flowbyactivity_df['SourceName'])[0]
    # load catalog info for source
    src_info = cat[s]
    # read the pre-determined level of sector aggregation of each crosswalk from the source catalog
    levelofSectoragg = src_info['sector_aggregation_level']
    # if the FBS activity set is 'direct', overwrite the
    # levelofsectoragg, or if specified in fxn call
    if kwargs != {}:
        if 'allocationmethod' in kwargs:
            if kwargs['allocationmethod'] == 'direct':
                levelofSectoragg = 'disaggregated'
        if 'overwrite_sectorlevel' in kwargs:
            levelofSectoragg = kwargs['overwrite_sectorlevel']
    # if data are provided in NAICS format, use the mastercrosswalk
    if src_info['sector-like_activities']:
        cw = load_sector_crosswalk()
        sectors = cw.loc[:, [SECTOR_SOURCE_NAME]]
        # Create mapping df that's just the sectors at first
        mapping = sectors.drop_duplicates()
        # Add the sector twice as activities so mapping is identical
        mapping = mapping.assign(Activity=sectors[SECTOR_SOURCE_NAME])
        mapping = mapping.rename(columns={SECTOR_SOURCE_NAME: "Sector"})
        # add columns so can run expand_naics_list_fxn
        # if sector-like_activities = True, missing columns, so add
        mapping['ActivitySourceName'] = s
        # tmp assignment
        mapping['SectorType'] = None
        # Include all digits of naics in mapping, if levelofNAICSagg is specified as "aggregated"
        if levelofSectoragg == 'aggregated':
            mapping = expand_naics_list(mapping, sectorsourcename)
    else:
        # if source data activities are text strings, or sector-like
        # activities should be modified, call on the manually created source crosswalks
        mapping = get_activitytosector_mapping(s)
        # filter by SectorSourceName of interest
        mapping = mapping[mapping['SectorSourceName'] == sectorsourcename]
        # drop SectorSourceName
        mapping = mapping.drop(columns=['SectorSourceName'])
        # Include all digits of naics in mapping, if levelofNAICSagg is specified as "aggregated"
        if levelofSectoragg == 'aggregated':
            mapping = expand_naics_list(mapping, sectorsourcename)
    # Merge in with flowbyactivity by
    flowbyactivity_wsector_df = flowbyactivity_df
    for k, v in activity_fields.items():
        sector_direction = k
        flowbyactivity_field = v[0]["flowbyactivity"]
        flowbysector_field = v[1]["flowbysector"]
        sector_type_field = sector_direction+'SectorType'
        mappings_df_tmp = mapping.rename(columns={'Activity': flowbyactivity_field,
                                                      'Sector': flowbysector_field,
                                                      'SectorType': sector_type_field})
        # column doesn't exist for sector-like activities, so ignore if error occurs
        mappings_df_tmp = mappings_df_tmp.drop(columns=['ActivitySourceName'], errors='ignore')
        # Merge them in. Critical this is a left merge to preserve all unmapped rows
        flowbyactivity_wsector_df = pd.merge(flowbyactivity_wsector_df,mappings_df_tmp,
                                             how='left', on=flowbyactivity_field)
    flowbyactivity_wsector_df = flowbyactivity_wsector_df.replace({np.nan: None})
    # add sector source name
    flowbyactivity_wsector_df = flowbyactivity_wsector_df.assign(SectorSourceName=sectorsourcename)

    # if activities are sector-like check that the sectors are in the crosswalk
    if src_info['sector-like_activities']:
        flowbyactivity_wsector_df =\
            replace_naics_w_naics_from_another_year(flowbyactivity_wsector_df,
                                                    sectorsourcename)

    return flowbyactivity_wsector_df


def expand_naics_list(df, sectorsourcename):
    """
    Add disaggregated sectors to the crosswalks.
    :param df: df, with sector columns
    :param sectorsourcename: str, sectorsourcename for naics year
    :return: df with additional rows for expanded sector list
    """

    # load master crosswalk
    cw = load_sector_crosswalk()
    sectors = cw.loc[:, [sectorsourcename]]
    # drop duplicates
    sectors = sectors.drop_duplicates().dropna()
    # drop rows that contain hyphenated sectors
    sectors = sectors[~sectors[sectorsourcename].str.contains("-")].reset_index(drop=True)
    # Ensure 'None' not added to sectors
    sectors = sectors[sectors[sectorsourcename] != "None"]

    # create list of sectors that exist in original df, which,
    # if created when expanding sector list cannot be added
    existing_sectors = df[['Sector']]
    existing_sectors = existing_sectors.drop_duplicates()

    naics_df = pd.DataFrame([])
    for i in existing_sectors['Sector']:
        dig = len(str(i))
        n = sectors.loc[sectors[sectorsourcename].apply(lambda x: x[0:dig]) == i]
        if len(n) != 0:
            n = n.assign(Sector=i)
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


def get_fba_allocation_subset(fba_allocation, source, activitynames, **kwargs):
    """
    Subset the fba allocation data based on NAICS associated with activity
    :param fba_allocation: df, FBA format
    :param source: str, source name
    :param activitynames: list, activity names in activity set
    :param kwargs: can be the mapping file and method of allocation
    :return: df, FBA subset
    """
    # first determine if there are special cases that would modify the typical method of subset
    # an example of a special case is when the allocation method is 'proportional-flagged'
    subset_by_sector_cols = False
    subset_by_column_value = False
    if kwargs != {}:
        if 'flowSubsetMapped' in kwargs:
            fsm = kwargs['flowSubsetMapped']
        if 'allocMethod' in kwargs:
            am = kwargs['allocMethod']
            if am == 'proportional-flagged':
                subset_by_sector_cols = True
        if 'activity_set_names' in kwargs:
            asn = kwargs['activity_set_names']
            if asn is not None:
                if 'allocation_subset_col' in asn:
                    subset_by_column_value = True

    # load the source catalog
    cat = load_source_catalog()
    src_info = cat[source]
    if src_info['sector-like_activities'] is False:
        # read in source crosswalk
        df = get_activitytosector_mapping(source)
        sec_source_name = df['SectorSourceName'].all()
        df = expand_naics_list(df, sec_source_name)
        # subset source crosswalk to only contain values pertaining to list of activity names
        df = df.loc[df['Activity'].isin(activitynames)]
        # turn column of sectors related to activity names into list
        sector_list = pd.unique(df['Sector']).tolist()
        # subset fba allocation table to the values in
        # the activity list, based on overlapping sectors
        if 'Sector' in fba_allocation:
            fba_allocation_subset =\
                fba_allocation.loc[fba_allocation['Sector'].isin(
                    sector_list)].reset_index(drop=True)
        else:
            fba_allocation_subset = \
                fba_allocation.loc[(fba_allocation[fbs_activity_fields[0]].isin(sector_list)) |
                                   (fba_allocation[fbs_activity_fields[1]].isin(sector_list))]. \
                    reset_index(drop=True)
    else:
        if 'Sector' in fba_allocation:
            fba_allocation_subset =\
                fba_allocation.loc[fba_allocation['Sector'].isin(
                    activitynames)].reset_index(drop=True)
        elif subset_by_sector_cols:
            # if it is a special case, then base the subset of data on
            # sectors in the sector columns, not on activitynames
            fsm_sub = fsm.loc[(fsm[fba_activity_fields[0]].isin(activitynames)) |
                              (fsm[fba_activity_fields[1]].isin(activitynames))
                              ].reset_index(drop=True)
            part1 = fsm_sub[['SectorConsumedBy']]
            part2 = fsm_sub[['SectorProducedBy']]
            part1.columns = ['Sector']
            part2.columns = ['Sector']
            modified_activitynames = pd.concat([part1, part2], ignore_index=True).drop_duplicates()
            modified_activitynames =\
                modified_activitynames[modified_activitynames['Sector'].notnull()]
            modified_activitynames = modified_activitynames['Sector'].tolist()
            fba_allocation_subset = \
                fba_allocation.loc[
                    (fba_allocation[fbs_activity_fields[0]].isin(modified_activitynames)) |
                    (fba_allocation[fbs_activity_fields[1]].isin(modified_activitynames))]. \
                    reset_index(drop=True)

        else:
            fba_allocation_subset =\
                fba_allocation.loc[(fba_allocation[fbs_activity_fields[0]].isin(activitynames)) |
                                   (fba_allocation[fbs_activity_fields[1]].isin(activitynames))].\
                    reset_index(drop=True)

    # if activity set names included in function call and activity set names is not null, \
    # then subset data based on value and column specified
    if subset_by_column_value:
        # create subset of activity names and allocation subset metrics
        asn_subset = asn[asn['name'].isin(activitynames)].reset_index(drop=True)
        if asn_subset['allocation_subset'].isna().all():
            pass
        elif asn_subset['allocation_subset'].isna().any():
            log.error('Define column and value to subset on in the activity set csv for all rows')
        else:
            col_to_subset = asn_subset['allocation_subset_col'][0]
            val_to_subset = asn_subset['allocation_subset'][0]
            # subset fba_allocation_subset further
            log.debug('Subset the allocation dataset where %s = %s', str(col_to_subset), str(val_to_subset))
            fba_allocation_subset = fba_allocation_subset[fba_allocation_subset[col_to_subset]
                                                          == val_to_subset].reset_index(drop=True)

    return fba_allocation_subset


def map_elementary_flows(fba, from_fba_source, keep_unmapped_rows=False):
    """
    Applies mapping from fedelemflowlist to convert flows to fedelemflowlist flows
    :param fba: df flow-by-activity or flow-by-sector with 'Flowable', 'Context', and 'Unit' fields
    :param from_fba_source: str Source name of fba list to look for mappings
    :param keep_unmapped_rows: False if want unmapped rows dropped, True if want to retain
    :return: df, with flows mapped using federal elementary flow list
    """

    from fedelemflowlist import get_flowmapping

    # rename columns to match FBS formatting
    fba = fba.rename(columns={"FlowName": 'Flowable',
                              "Compartment": "Context"})

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
        log.warning("No mapping file in fedelemflowlist found for %s", ' '.join(from_fba_source))
        # return the original df but with columns renamed so can continue working on the FBS
        fba_mapped_df = fba.copy()
    else:
        flowmapping = flowmapping[mapping_fields]

        # define merge type based on keeping or dropping unmapped data
        if keep_unmapped_rows is False:
            merge_type = 'inner'
        else:
            merge_type = 'left'

        # merge fba with flows
        fba_mapped_df = pd.merge(fba, flowmapping,
                                 left_on=["Flowable", "Context"],
                                 right_on=["SourceFlowName", "SourceFlowContext"],
                                 how=merge_type)
        fba_mapped_df.loc[fba_mapped_df["TargetFlowName"].notnull(), "Flowable"] =\
            fba_mapped_df["TargetFlowName"]
        fba_mapped_df.loc[fba_mapped_df["TargetFlowName"].notnull(), "Context"] =\
            fba_mapped_df["TargetFlowContext"]
        fba_mapped_df.loc[fba_mapped_df["TargetFlowName"].notnull(), "Unit"] =\
            fba_mapped_df["TargetUnit"]
        fba_mapped_df.loc[fba_mapped_df["TargetFlowName"].notnull(), "FlowAmount"] = \
            fba_mapped_df["FlowAmount"] * fba_mapped_df["ConversionFactor"]

        # drop
        fba_mapped_df = fba_mapped_df.drop(columns=mapping_fields)

    return fba_mapped_df


def get_sector_list(sector_level):
    """
    Create a sector list at the specified sector level
    :param sector_level: str, NAICS level
    :return: list, sectors at specified sector level
    """

    cw = load_sector_length_crosswalk()
    sector_list = cw[sector_level].unique().tolist()

    return sector_list
