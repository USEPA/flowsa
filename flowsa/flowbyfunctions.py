# flowbyfunctions.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Helper functions for flowbyactivity and flowbysector data
"""

import numpy as np
from esupy.dqi import get_weighted_average
import flowsa
import flowsa.flowbyactivity
from flowsa.common import fbs_collapsed_default_grouping_fields
from flowsa.dataclean import clean_df, standardize_units
from flowsa.flowsa_log import log
from flowsa.location import US_FIPS, get_state_FIPS, get_county_FIPS
from flowsa.schema import flow_by_activity_fields, flow_by_sector_fields, \
    flow_by_sector_collapsed_fields


def create_geoscale_list(df, geoscale, year='2015'):
    """
    Create a list of FIPS associated with given geoscale

    :param df: FlowBySector of FlowByActivity df
    :param geoscale: 'national', 'state', or 'county'
    :param year: str, year of FIPS, defaults to 2015
    :return: list of relevant FIPS
    """

    # filter by geoscale depends on Location System
    fips = []
    if geoscale == "national":
        fips.append(US_FIPS)
    elif df['LocationSystem'].str.contains('FIPS').any():
        if geoscale == "state":
            state_FIPS = get_state_FIPS(year)
            state_FIPS = state_FIPS[state_FIPS['FIPS'] != '72000']
            fips = list(state_FIPS['FIPS'])
        elif geoscale == "county":
            county_FIPS = get_county_FIPS(year)
            fips = list(county_FIPS['FIPS'])

    return fips


def filter_by_geoscale(df, geoscale):
    """
    Filter flowbyactivity by FIPS at the given scale
    :param df: Either flowbyactivity or flowbysector
    :param geoscale: string, either 'national', 'state', or 'county'
    :return: filtered flowbyactivity or flowbysector
    """

    fips = create_geoscale_list(df, geoscale)

    df = df[df['Location'].isin(fips)].reset_index(drop=True)

    if len(df) == 0:
        raise flowsa.exceptions.FBSMethodConstructionError(
            message="No flows found in the flow dataset at "
            f"the {geoscale} scale")
    else:
        return df


def aggregator(df, groupbycols, retain_zeros=True, flowcolname='FlowAmount'):
    """
    Aggregates flowbyactivity or flowbysector 'FlowAmount' column in df and
    generate weighted average values based on FlowAmount values for numeric
    columns
    :param df: df, Either flowbyactivity or flowbysector
    :param groupbycols: list, Either flowbyactivity or flowbysector columns
    :param retain_zeros, bool, default True, if set to True, all rows that
    have a FlowAmount = 0 will be returned in df. If False, those rows will
    be dropped
    :return: df, with aggregated columns
    """
    # drop group_id from cols before aggregating
    df = (df
          .drop(columns='group_id', errors='ignore')
          .reset_index(drop=True)
          )

    # drop columns with flowamount = 0
    if retain_zeros is False:
        df = df[df[flowcolname] != 0]

    # list of column headers, that if exist in df, should be
    # aggregated using the weighted avg fxn
    possible_column_headers = \
        ('Spread', 'Min', 'Max', 'DataReliability', 'TemporalCorrelation',
         'GeographicalCorrelation', 'TechnologicalCorrelation',
         'DataCollection')

    # list of column headers that do exist in the df being aggregated
    column_headers = [e for e in possible_column_headers
                      if e in df.columns.values.tolist()]

    groupbycols = [c for c in groupbycols if c not in column_headers]
    # check cols exist in df
    groupbycols = [c for c in groupbycols if c in df.columns]

    df_dfg = df.groupby(groupbycols, dropna=False).agg({flowcolname: ['sum']})

    def is_identical(s):
        a = s.to_numpy()
        return (a[0] == a).all()

    # run through other columns creating weighted average
    for e in column_headers:
        if len(df) > 0 and is_identical(df[e]):
            df_dfg.loc[:, e] = df[e].iloc[0]
        else:
            df_dfg[e] = get_weighted_average(df, e, flowcolname, groupbycols)

    df_dfg = df_dfg.reset_index()
    df_dfg.columns = df_dfg.columns.droplevel(level=1)

    return df_dfg


def remove_parent_sectors_from_crosswalk(cw_load, sector_list):
    """
    Remove parent sectors to a list of sectors from the crosswalk
    :return:
    """
    cw_filtered = cw_load.applymap(lambda x:
                                   x in sector_list)
    locations = cw_filtered[cw_filtered > 0].stack().index.tolist()
    for r, i in locations:
        col_index = cw_load.columns.get_loc(i)
        cw_load.iloc[r, 0:col_index] = np.nan

    return cw_load

# todo: revise to work for recursively built data
# def sector_aggregation(df_load, return_all_possible_sector_combos=False,
#                        sectors_to_exclude_from_agg=None):
#     """
#     Function that checks if a sector length exists, and if not,
#     sums the less aggregated sector
#     :param df_load: Either a flowbyactivity df with sectors or
#        a flowbysector df
#     :param return_all_possible_sector_combos: bool, default false, if set to
#     true, will return all possible combinations of sectors at each sector
#     length (ex. a 4 digit SectorProducedBy will have rows for 2-6 digit
#     SectorConsumedBy). This will result in a df with double counting.
#     :param sectors_to_exclude_from_agg: list or dict, sectors that should not be
#     aggregated beyond the sector level provided. Dictionary if separate lists
#     for SectorProducedBy and SectorConsumedBy
#     :return: df, with aggregated sector values
#     """
#     df = df_load.copy()
#
#     # determine grouping columns - based on datatype
#     group_cols = list(df.select_dtypes(include=['object', 'int']).columns)
#     sector_cols = ['SectorProducedBy', 'SectorConsumedBy']
#     if 'Sector' in df.columns:
#         sector_cols = ['Sector']
#
#     if 'ActivityProducedBy' in df_load.columns:
#         # determine if activities are sector-like, if aggregating a df with a
#         # 'SourceName'
#         sector_like_activities = check_activities_sector_like(df_load)
#         # if activities are sector like, drop columns while running ag then
#         # add back in
#         if sector_like_activities:
#             # subset df
#             df_cols = [e for e in df.columns if e not in
#                        ('ActivityProducedBy', 'ActivityConsumedBy')]
#             group_cols = [e for e in group_cols if e not in
#                           ('ActivityProducedBy', 'ActivityConsumedBy')]
#             df = df[df_cols]
#             df = df.reset_index(drop=True)
#
#     # load naics length crosswwalk
#     cw_load = load_crosswalk('NAICS_2012_Crosswalk')
#     # remove any parent sectors of sectors identified as those that should
#     # not be aggregated
#     if sectors_to_exclude_from_agg is not None:
#         # if sectors are in a dictionary create cw for sectorproducedby and
#         # sectorconsumedby otherwise single cr
#         if isinstance(sectors_to_exclude_from_agg, dict):
#             cws = {}
#             for s in sector_cols:
#                 try:
#                     cw = remove_parent_sectors_from_crosswalk(
#                         cw_load, sectors_to_exclude_from_agg[s])
#                     cws[s] = cw
#                 except KeyError:
#                     cws[s] = cw_load
#             cw_load = cws.copy()
#         else:
#             cw_load = remove_parent_sectors_from_crosswalk(
#                 cw_load, sectors_to_exclude_from_agg)
#
#     # find the longest length sector
#     length = df[sector_cols].apply(lambda x: x.str.len()).max().max()
#     length = int(length)
#     # for loop in reverse order longest length NAICS minus 1 to 2
#     # appends missing naics levels to df
#     for i in range(length, 2, -1):
#         if return_all_possible_sector_combos:
#             for j in range(1, i-1):
#                 df = append_new_sectors(df, i, j, cw_load, group_cols)
#         else:
#             df = append_new_sectors(df, i, 1, cw_load, group_cols)
#
#     if 'ActivityProducedBy' in df_load.columns:
#         # if activities are source-like, set col values as
#         # copies of the sector columns
#         if sector_like_activities & ('FlowAmount' in df.columns) & \
#                 ('ActivityProducedBy' in df_load.columns):
#             df = df.assign(ActivityProducedBy=df['SectorProducedBy'])
#             df = df.assign(ActivityConsumedBy=df['SectorConsumedBy'])
#
#     return df.reset_index(drop=True)


def assign_fips_location_system(df, year_of_data):
    """
    Add location system based on year of data. County level FIPS
    change over the years.
    :param df: df with FIPS location system
    :param year_of_data: int, year of data pulled
    :return: df, with 'LocationSystem' column values
    """
    # ensure year integer
    year_of_data = int(year_of_data)
    if year_of_data >= 2015:
        df['LocationSystem'] = 'FIPS_2015'
    elif 2013 <= year_of_data < 2015:
        df['LocationSystem'] = 'FIPS_2013'
    elif 2010 <= year_of_data < 2013:
        df['LocationSystem'] = 'FIPS_2010'
    elif year_of_data < 2010:
        log.warning(f"Missing FIPS codes from crosswalk for {year_of_data}. "
                    f"Assigning to FIPS_2010")
        df['LocationSystem'] = 'FIPS_2010'

    return df


def collapse_fbs_sectors(fbs):
    """
    Collapses the Sector Produced/Consumed into a single column named "Sector"
    uses based on identified rules for flowtypes
    :param fbs: df, a standard FlowBySector (format)
    :return: df, FBS with single Sector column
    """
    # ensure correct datatypes and order
    fbs = clean_df(fbs, flow_by_sector_fields)

    if fbs['SectorProducedBy'].isnull().all():
        fbs['Sector'] = fbs['SectorConsumedBy']
    elif fbs['SectorConsumedBy'].isnull().all():
        fbs['Sector'] = fbs['SectorProducedBy']
    else:
        # collapse the FBS sector columns into one column based on FlowType
        fbs.loc[fbs["FlowType"] == 'TECHNOSPHERE_FLOW', 'Sector'] = \
            fbs["SectorConsumedBy"]
        fbs.loc[fbs["FlowType"] == 'WASTE_FLOW', 'Sector'] = \
            fbs["SectorProducedBy"]
        fbs.loc[(fbs["FlowType"] == 'WASTE_FLOW') &
                (fbs['SectorProducedBy'].isnull()),
                'Sector'] = fbs["SectorConsumedBy"]
        fbs.loc[(fbs["FlowType"] == 'ELEMENTARY_FLOW') &
                (fbs['SectorProducedBy'].isnull()),
                'Sector'] = fbs["SectorConsumedBy"]
        fbs.loc[(fbs["FlowType"] == 'ELEMENTARY_FLOW') &
                (fbs['SectorConsumedBy'].isnull()),
                'Sector'] = fbs["SectorProducedBy"]
        fbs.loc[(fbs["FlowType"] == 'ELEMENTARY_FLOW') &
                (fbs['SectorConsumedBy'].isin(['F010', 'F0100', 'F01000'])) &
                (fbs['SectorProducedBy'].isin(
                    ['22', '221', '2213', '22131', '221310'])),
                'Sector'] = fbs["SectorConsumedBy"]

    # drop sector consumed/produced by columns
    fbs_collapsed = fbs.drop(columns=['SectorProducedBy', 'SectorConsumedBy'])
    # aggregate
    fbs_collapsed = \
        aggregator(fbs_collapsed, fbs_collapsed_default_grouping_fields)
    # sort dataframe
    fbs_collapsed = clean_df(fbs_collapsed, flow_by_sector_collapsed_fields)
    fbs_collapsed = fbs_collapsed.sort_values(
        ['Sector', 'Flowable', 'Context', 'Location']).reset_index(drop=True)

    return fbs_collapsed


def load_fba_w_standardized_units(datasource, year, **kwargs):
    """
    Standardize how a FBA is loaded for allocation purposes when
    generating a FBS. Important to immediately convert the df units to
    standardized units.
    :param datasource: string, FBA source name
    :param year: int, year of data
    :param kwargs: optional parameters include flowclass, geographic_level,
           download_if_missing, allocation_map_to_flow_list
    :return: fba df with standardized units
    """

    from flowsa.sectormapping import map_fbs_flows

    # determine if any addtional parameters required to load a Flow-By-Activity
    # add parameters to dictionary if exist in method yaml
    fba_dict = {}
    if 'flowclass' in kwargs:
        fba_dict['flowclass'] = kwargs['flowclass']
    if 'geographic_level' in kwargs:
        fba_dict['geographic_level'] = kwargs['geographic_level']
    if 'download_FBA_if_missing' in kwargs:
        fba_dict['download_FBA_if_missing'] = kwargs['download_FBA_if_missing']
    # load the allocation FBA
    fba = flowsa.flowbyactivity.getFlowByActivity(
        datasource, year, **fba_dict).reset_index(drop=True)
    # convert to standardized units either by mapping to federal
    # flow list/material flow list or by using function. Mapping will add
    # context and flowable columns
    if 'allocation_map_to_flow_list' in kwargs:
        if kwargs['allocation_map_to_flow_list']:
            # ensure df loaded correctly/has correct dtypes
            fba = clean_df(fba, flow_by_activity_fields,
                           drop_description=False)
            fba, mapping_files = map_fbs_flows(
                fba, datasource, kwargs, keep_fba_columns=True,
                keep_unmapped_rows=True)
        else:
            # ensure df loaded correctly/has correct dtypes
            fba = clean_df(fba, flow_by_activity_fields)
            fba = standardize_units(fba)
    else:
        # ensure df loaded correctly/has correct dtypes
        fba = clean_df(fba, flow_by_activity_fields,
                       drop_description=False)
        fba = standardize_units(fba)

    return fba
