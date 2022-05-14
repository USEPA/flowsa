# sectormapping.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Contains mapping functions
"""
import os.path
import pandas as pd
import numpy as np
from esupy.mapping import apply_flow_mapping
import flowsa
from flowsa.common import get_flowsa_base_name, \
    return_true_source_catalog_name, check_activities_sector_like, \
    load_yaml_dict, fba_activity_fields, SECTOR_SOURCE_NAME
from flowsa.schema import activity_fields, dq_fields
from flowsa.settings import log
from flowsa.flowbyfunctions import fbs_activity_fields, load_crosswalk
from flowsa.validation import replace_naics_w_naics_from_another_year


def get_activitytosector_mapping(source, fbsconfigpath=None):
    """
    Gets  the activity-to-sector mapping
    :param source: str, the data source name
    :return: a pandas df for a standard ActivitytoSector mapping
    """
    from flowsa.settings import crosswalkpath
    # identify mapping file name
    mapfn = f'NAICS_Crosswalk_{source}'

    # if FBS method file loaded from outside the flowsa directory, check if
    # there is also a crosswalk
    external_mappingpath = f"{fbsconfigpath}activitytosectormapping/"
    if os.path.exists(external_mappingpath):
        activity_mapping_source_name = get_flowsa_base_name(
            external_mappingpath, mapfn, 'csv')
        if os.path.isfile(f"{external_mappingpath}"
                          f"{activity_mapping_source_name}.csv"):
            log.info(f"Loading {activity_mapping_source_name}.csv "
                     f"from {external_mappingpath}")
            crosswalkpath = external_mappingpath
    activity_mapping_source_name = get_flowsa_base_name(
        crosswalkpath, mapfn, 'csv')
    mapping = pd.read_csv(f'{crosswalkpath}{activity_mapping_source_name}.csv',
                          dtype={'Activity': 'str', 'Sector': 'str'})
    # some mapping tables will have data for multiple sources, while other
    # mapping tables are used for multiple sources (like EPA_NEI or BEA
    # mentioned above) so if find the exact source name in the
    # ActivitySourceName column use those rows if the mapping file returns
    # empty, use the original mapping file subset df to keep rows where
    # ActivitySourceName matches source name
    mapping2 = mapping[mapping['ActivitySourceName'] == source].reset_index(
        drop=True)
    if len(mapping2) > 0:
        return mapping2
    else:
        return mapping


def add_sectors_to_flowbyactivity(
    flowbyactivity_df,
    activity_to_sector_mapping=None,
    sectorsourcename=SECTOR_SOURCE_NAME,
    allocationmethod=None,
    overwrite_sectorlevel=None,
    fbsconfigpath=None
):
    """
    Add Sectors from the Activity fields and mapped them to Sector
    from the crosswalk. No allocation is performed.
    :param flowbyactivity_df: A standard flowbyactivity data frame
    :param activity_to_sector_mapping: str, name for activity_to_sector mapping
    :param sectorsourcename: A sector source name, using package default
    :param allocationmethod: str, modifies function behavoir if = 'direct'
    :param fbsconfigpath, str, opt ional path to an FBS method outside flowsa
        repo
    :return: a df with activity fields mapped to 'sectors'
    """
    # First check if source activities are NAICS like -
    # if so make it into a mapping file
    s = pd.unique(flowbyactivity_df['SourceName'])[0]
    # load catalog info for source, first check for sourcename used
    # in source catalog
    ts = return_true_source_catalog_name(s)
    src_info = load_yaml_dict('source_catalog')[ts]
    # read the pre-determined level of sector aggregation of
    # each crosswalk from the source catalog
    levelofSectoragg = src_info['sector_aggregation_level']
    # if the FBS activity set is 'direct', overwrite the
    # levelofsectoragg, or if specified in fxn call
    if allocationmethod == 'direct':
        levelofSectoragg = 'disaggregated'
    if overwrite_sectorlevel is not None:
        levelofSectoragg = overwrite_sectorlevel
    # if data are provided in NAICS format, use the mastercrosswalk
    if src_info['sector-like_activities']:
        cw = load_crosswalk('sector_timeseries')
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
        # Include all digits of naics in mapping, if levelofNAICSagg
        # is specified as "aggregated"
        if levelofSectoragg == 'aggregated':
            mapping = expand_naics_list(mapping, sectorsourcename)
    else:
        # if source data activities are text strings, or sector-like
        # activities should be modified, call on the manually
        # created source crosswalks
        if activity_to_sector_mapping:
            s = activity_to_sector_mapping
        mapping = get_activitytosector_mapping(s, fbsconfigpath=fbsconfigpath)
        # filter by SectorSourceName of interest
        mapping = mapping[mapping['SectorSourceName'] == sectorsourcename]
        # drop SectorSourceName
        mapping = mapping.drop(columns=['SectorSourceName'])
        # Include all digits of naics in mapping, if levelofNAICSagg
        # is specified as "aggregated"
        if levelofSectoragg == 'aggregated':
            mapping = expand_naics_list(mapping, sectorsourcename)
    # Merge in with flowbyactivity by
    flowbyactivity_wsector_df = flowbyactivity_df.copy(deep=True)
    for k, v in activity_fields.items():
        sector_direction = k
        flowbyactivity_field = v[0]["flowbyactivity"]
        flowbysector_field = v[1]["flowbysector"]
        sector_type_field = sector_direction+'SectorType'
        mappings_df_tmp = mapping.rename(
            columns={'Activity': flowbyactivity_field,
                     'Sector': flowbysector_field,
                     'SectorType': sector_type_field})
        # column doesn't exist for sector-like activities,
        # so ignore if error occurs
        mappings_df_tmp = mappings_df_tmp.drop(
            columns=['ActivitySourceName'], errors='ignore')
        # Merge them in. Critical this is a left merge to
        # preserve all unmapped rows
        flowbyactivity_wsector_df = pd.merge(
            flowbyactivity_wsector_df, mappings_df_tmp, how='left',
            on=flowbyactivity_field)
    for c in ['SectorProducedBy', 'ProducedBySectorType',
              'SectorConsumedBy', 'ConsumedBySectorType']:
        flowbyactivity_wsector_df[c] = \
            flowbyactivity_wsector_df[c].replace({np.nan: None})
    # add sector source name
    flowbyactivity_wsector_df = \
        flowbyactivity_wsector_df.assign(SectorSourceName=sectorsourcename)

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
    cw = load_crosswalk('sector_timeseries')
    sectors = cw.loc[:, [sectorsourcename]]
    # drop duplicates
    sectors = sectors.drop_duplicates().dropna()
    # drop rows that contain hyphenated sectors
    sectors = sectors[
        ~sectors[sectorsourcename].str.contains("-")].reset_index(drop=True)
    # Ensure 'None' not added to sectors
    sectors = sectors[sectors[sectorsourcename] != "None"]

    # create list of sectors that exist in original df, which,
    # if created when expanding sector list cannot be added
    existing_sectors = df[['Sector']]
    existing_sectors = existing_sectors.drop_duplicates()

    naics_df = pd.DataFrame([])
    for i in existing_sectors['Sector']:
        dig = len(str(i))
        n = sectors.loc[
            sectors[sectorsourcename].apply(lambda x: x[0:dig]) == i]
        if len(n) != 0:
            n = n.assign(Sector=i)
            naics_df = pd.concat([naics_df, n])

    # merge df to retain activityname/sectortype info
    naics_expanded = df.merge(naics_df, how='left')
    # drop column of aggregated naics and rename column of disaggregated naics
    naics_expanded = naics_expanded.drop(columns=["Sector"])
    naics_expanded = naics_expanded.rename(
        columns={sectorsourcename: 'Sector'})
    # drop duplicates and rearrange df columns
    naics_expanded = naics_expanded.drop_duplicates()
    naics_expanded = naics_expanded[['ActivitySourceName', 'Activity',
                                     'Sector', 'SectorType']]

    return naics_expanded


def get_fba_allocation_subset(fba_allocation, source, activitynames,
                              sourceconfig=False, flowSubsetMapped=None,
                              allocMethod=None, fbsconfigpath=None):
    """
    Subset the fba allocation data based on NAICS associated with activity
    :param fba_allocation: df, FBA format
    :param source: str, source name
    :param activitynames: list, activity names in activity set
    :param kwargs: can be the mapping file and method of allocation
    :return: df, FBA subset
    """
    # first determine if there are special cases that would modify the
    # typical method of subset an example of a special case is when the
    # allocation method is 'proportional-flagged'
    subset_by_sector_cols = False
    if flowSubsetMapped is not None:
        fsm = flowSubsetMapped
    if allocMethod is not None:
        am = allocMethod
        if am == 'proportional-flagged':
            subset_by_sector_cols = True

    if check_activities_sector_like(fba_allocation, sourcename=source) is False:
        # read in source crosswalk
        df = get_activitytosector_mapping(
            sourceconfig.get('activity_to_sector_mapping', source),
            fbsconfigpath=fbsconfigpath)
        sec_source_name = df['SectorSourceName'][0]
        df = expand_naics_list(df, sec_source_name)
        # subset source crosswalk to only contain values
        # pertaining to list of activity names
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
                fba_allocation.loc[
                    (fba_allocation[fbs_activity_fields[0]].isin(sector_list)
                     ) |
                    (fba_allocation[fbs_activity_fields[1]].isin(sector_list)
                     )].reset_index(drop=True)
    else:
        if 'Sector' in fba_allocation:
            fba_allocation_subset =\
                fba_allocation.loc[fba_allocation['Sector'].isin(
                    activitynames)].reset_index(drop=True)
        elif subset_by_sector_cols:
            # if it is a special case, then base the subset of data on
            # sectors in the sector columns, not on activitynames
            fsm_sub = fsm.loc[
                (fsm[fba_activity_fields[0]].isin(activitynames)) |
                (fsm[fba_activity_fields[1]].isin(activitynames)
                 )].reset_index(drop=True)
            part1 = fsm_sub[['SectorConsumedBy']]
            part2 = fsm_sub[['SectorProducedBy']]
            part1.columns = ['Sector']
            part2.columns = ['Sector']
            modified_activitynames = \
                pd.concat([part1, part2], ignore_index=True).drop_duplicates()
            modified_activitynames = modified_activitynames[
                modified_activitynames['Sector'].notnull()]
            modified_activitynames = modified_activitynames['Sector'].tolist()
            fba_allocation_subset = fba_allocation.loc[
                (fba_allocation[fbs_activity_fields[0]].isin(
                    modified_activitynames)) |
                (fba_allocation[fbs_activity_fields[1]].isin(
                    modified_activitynames)
                 )].reset_index(drop=True)

        else:
            fba_allocation_subset = fba_allocation.loc[
                (fba_allocation[fbs_activity_fields[0]].isin(activitynames)) |
                (fba_allocation[fbs_activity_fields[1]].isin(activitynames)
                 )].reset_index(drop=True)

    return fba_allocation_subset


def convert_units_to_annual(df):
    """
    Convert data and units to annual flows
    :param df: df with 'FlowAmount' and 'Unit' column
    :return: df with annual FlowAmounts
    """
    # convert unit per day to year
    df['FlowAmount'] = np.where(df['Unit'].str.contains('/d'),
                                df['FlowAmount'] * 365,
                                df['FlowAmount'])
    df['Unit'] = df['Unit'].apply(lambda x: x.replace('/d', ""))

    return df


def map_flows(fba, from_fba_source, flow_type='ELEMENTARY_FLOW',
              ignore_source_name=False, **kwargs):
    """
    Applies mapping via esupy from fedelemflowlist or material
    flow list to convert flows to standardized list of flows
    :param fba: df flow-by-activity or flow-by-sector
    :param from_fba_source: str Source name of fba list to look for mappings
    :param flow_type: str either 'ELEMENTARY_FLOW', 'TECHNOSPHERE_FLOW',
        or 'WASTE_FLOW'
    :param ignore_source_name: bool, passed to apply_flow_mapping
    :param kwargs: optional - keep_unmapped_rows: False if want
        unmapped rows dropped, True if want to retain and keep_fba_columns:
        boolean, True or False, indicate if want to maintain
        'FlowName' and 'Compartment' columns in returned df
    :return: df, with flows mapped using federal elementary flow list or
        material flow list
    """

    # prior to mapping elementary flows, ensure all data
    # are in an annual format
    fba = convert_units_to_annual(fba)

    keep_unmapped_rows = False

    # if need to maintain FBA columns, create copies of columns
    if kwargs != {}:
        if ('keep_fba_columns' in kwargs) & \
                (kwargs['keep_fba_columns'] is True):
            fba['Flowable'] = fba['FlowName']
            fba['Context'] = fba['Compartment']
        # if keep unmapped rows identified in kwargs, then use
        if 'keep_unmapped_rows' in kwargs:
            keep_unmapped_rows = kwargs['keep_unmapped_rows']

    # else, rename
    else:
        fba = fba.rename(columns={'FlowName': 'Flowable',
                                  'Compartment': 'Context'})

    mapped_df = apply_flow_mapping(fba, from_fba_source,
                                   flow_type=flow_type,
                                   keep_unmapped_rows=keep_unmapped_rows,
                                   ignore_source_name=ignore_source_name)

    if mapped_df is None or len(mapped_df) == 0:
        # return the original df but with columns renamed so
        # can continue working on the FBS
        log.warning("Error in flow mapping, flows not mapped")
        mapped_df = fba.copy()
        mapped_df['FlowUUID'] = None

    return mapped_df


def map_fbs_flows(fbs, from_fba_source, v, **kwargs):
    """
    Identifies the mapping file and applies mapping to fbs flows
    :param fbs: flow-by-sector dataframe
    :param from_fba_source: str Source name of fba list to look for mappings
    :param v: dictionary, The datasource parameters
    :param kwargs: includes keep_unmapped_columns and keep_fba_columns
    :return fbs_mapped: df, with flows mapped using federal elementary
           flow list or material flow list
    :return mapping_files: str, name of mapping file
    """
    ignore_source_name = False
    if 'mfl_mapping' in v:
        mapping_files = v['mfl_mapping']
        log.info("Mapping flows in %s to material flow list", from_fba_source)
        flow_type = 'WASTE_FLOW'
        ignore_source_name = True
    else:
        log.info("Mapping flows in %s to federal elementary flow list",
                 from_fba_source)
        if 'fedefl_mapping' in v:
            mapping_files = v['fedefl_mapping']
            ignore_source_name = True
        else:
            mapping_files = from_fba_source
        flow_type = 'ELEMENTARY_FLOW'

    fbs_mapped = map_flows(fbs, mapping_files, flow_type,
                           ignore_source_name, **kwargs)

    return fbs_mapped, mapping_files


def get_sector_list(sector_level, secondary_sector_level_dict=None):
    """
    Create a sector list at the specified sector level
    :param sector_level: str, NAICS level
    :param secondary_sector_level_dict: dict, additional sectors to keep,
    key is the secondary target NAICS level, value is a list of NAICS at the
    "sector_level" that should also include a further disaggregated subset
    of the data
    ex. sector_level = 'NAICS_4'
        secondary_sector_level_dict = {'NAICS_6': ['1133', '1125']}
    :return: list, sectors at specified sector level
    """
    # load crosswalk
    cw = load_crosswalk('sector_length')

    # first determine if there are sectors in a secondary target sector
    # level that should be included in the sector list. If there are,
    # add the sectors at the specified sector length and add the parent
    # sectors at the target sector length to a list to be dropped from
    # the sector list.

    # create empty lists for sector list and parent sectors to drop
    sector_list = []
    sector_drop = []
    # loop through identified secondary sector levels in a dictionary
    if secondary_sector_level_dict is not None:
        for k, v in secondary_sector_level_dict.items():
            cw_melt = cw.melt(
                id_vars=[k], var_name="NAICS_Length",
                value_name="NAICS_Match").drop_duplicates()
            cw_sub = cw_melt[cw_melt['NAICS_Match'].isin(v)]
            sector_add = cw_sub[k].unique().tolist()
            sector_list = sector_list + sector_add
            sector_drop = sector_drop + v

    # sectors at primary sector level
    sector_col = cw[[sector_level]].drop_duplicates()
    # drop any sectors that are already accounted for at a secondary sector
    # length
    sector_col = sector_col[~sector_col[sector_level].isin(sector_drop)]
    # add sectors to list
    sector_add = sector_col[sector_level].tolist()
    sector_list = sector_list + sector_add

    return sector_list


def map_to_BEA_sectors(fbs_load, region, io_level, year):
    """
    Map FBS sectors from NAICS to BEA, allocating by gross industry output.

    :param fbs_load: df completed FlowBySector collapsed to single 'Sector'
    :param region: str, 'state' or 'national'
    :param io_level: str, 'summary' or 'detail'
    :param year: year for industry output
    """
    from flowsa.sectormapping import get_activitytosector_mapping

    bea = get_BEA_industry_output(region, io_level, year)

    if io_level == 'summary':
        mapping_file = 'BEA_2012_Summary'
    elif io_level == 'detail':
        mapping_file = 'BEA_2012_Detail'

    # Prepare NAICS:BEA mapping file
    mapping = (
        get_activitytosector_mapping(mapping_file)
        .rename(columns={'Activity': 'BEA'}))
    mapping = mapping.drop(
        columns=mapping.columns.difference(['Sector','BEA']))

    # Create allocation ratios where one to many NAICS:BEA
    dup = mapping[mapping['Sector'].duplicated(keep=False)]
    dup = dup.merge(bea, how='left', on='BEA')
    dup['Allocation'] = dup['Output']/dup.groupby(
        ['Sector','Location']).Output.transform('sum')

    # Update and allocate to sectors
    fbs = (fbs_load.merge(
        mapping.drop_duplicates(subset='Sector', keep=False),
        how='left',
        on='Sector'))
    fbs = fbs.merge(dup.drop(columns='Output'),
                    how='left', on=['Sector', 'Location'],
                    suffixes=(None, '_y'))
    fbs['Allocation'] = fbs['Allocation'].fillna(1)
    fbs['BEA'] = fbs['BEA'].fillna(fbs['BEA_y'])
    fbs['FlowAmount'] = fbs['FlowAmount'] * fbs['Allocation']

    fbs = (fbs.drop(columns=dq_fields +
                    ['Sector', 'SectorSourceName',
                     'BEA_y', 'Allocation'], errors='ignore')
           .rename(columns={'BEA':'Sector'}))

    if (abs(1-(sum(fbs['FlowAmount']) /
               sum(fbs_load['FlowAmount'])))) > 0.005:
        log.warning('Data loss upon BEA mapping')

    return fbs


def get_BEA_industry_output(region, io_level, year):
    """
    Get FlowByActivity for industry output from state or national datasets
    :param region: str, 'state' or 'national'
    :param io_level: str, 'summary' or 'detail'
    :param year: year for industry output
    """
    if region == 'state':
        fba = 'stateio_Industry_GO'
        if io_level == 'detail':
            raise TypeError ('detail models not available for states')
    elif region == 'national':
        fba = 'BEA_GDP_GrossOutput'

    # Get output by BEA sector
    bea = flowsa.getFlowByActivity(fba, year)
    bea = (
        bea.drop(columns=bea.columns.difference(
            ['FlowAmount','ActivityProducedBy','Location']))
        .rename(columns={'FlowAmount':'Output',
                         'ActivityProducedBy': 'BEA'}))

    # If needed, aggregate from detial to summary
    if region == 'national' and io_level == 'summary':
        bea_mapping = (load_crosswalk('BEA')
                       [['BEA_2012_Detail_Code','BEA_2012_Summary_Code']]
                       .drop_duplicates()
                       .rename(columns={'BEA_2012_Detail_Code': 'BEA'}))
        bea = (bea.merge(bea_mapping, how='left', on='BEA')
               .drop(columns=['BEA'])
               .rename(columns={'BEA_2012_Summary_Code': 'BEA'}))
        bea = (bea.groupby(['BEA','Location']).agg({'Output': 'sum'})
               .reset_index())

    return bea
