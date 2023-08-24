# flowbyfunctions.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Helper functions for flowbyactivity and flowbysector data
"""

import pandas as pd
import numpy as np
from esupy.dqi import get_weighted_average
import flowsa
from flowsa.common import \
    load_crosswalk, fbs_fill_na_dict, \
    fbs_collapsed_default_grouping_fields, fbs_collapsed_fill_na_dict, \
    fba_activity_fields, fba_default_grouping_fields, \
    load_sector_length_cw_melt, fba_fill_na_dict, \
    fba_mapped_default_grouping_fields
from flowsa.dataclean import clean_df, replace_strings_with_NoneType, \
    replace_NoneType_with_empty_cells, standardize_units
from flowsa.flowsa_log import log, vlog
from flowsa.location import US_FIPS, get_state_FIPS, \
    get_county_FIPS, update_geoscale, fips_number_key
from flowsa.schema import flow_by_activity_fields, flow_by_sector_fields, \
    flow_by_sector_collapsed_fields, flow_by_activity_mapped_fields


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


def agg_by_geoscale(df, from_scale, to_scale, groupbycols):
    """
    Aggregate a df by geoscale
    :param df: flowbyactivity or flowbysector df
    :param from_scale: str, geoscale to aggregate from
        ('national', 'state', 'county')
    :param to_scale: str, geoscale to aggregate to (
        'national', 'state', 'county')
    :param groupbycols: flowbyactivity or flowbysector default groupby columns
    :return: df, at identified to_scale geographic level
    """

    # use from scale to filter by these values
    df = filter_by_geoscale(df, from_scale).reset_index(drop=True)

    df = update_geoscale(df, to_scale)

    fba_agg = aggregator(df, groupbycols)

    return fba_agg


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

    # if datatypes are strings, ensure that Null values remain NoneType
    df_dfg = replace_strings_with_NoneType(df_dfg)

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

# todo: delete
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
#     cw_load = load_crosswalk('sector_length')
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


def append_new_sectors(df, i, j, cw_load, group_cols):
    """
    Function to append new sectors at more aggregated levels
    :param df: df, FBS
    :param i: numeric, sector length to aggregate
    :param j: numeric, value to subtract from sector length for new sector
    length to add
    :param cw_load: df, sector crosswalk
    :param group_cols: list, cols to group by
    :param sectors_to_exclude_from_agg: list, sectors that should not be
    aggregated beyond the sector level provided
    :return:
    """

    # load crosswalk
    sector_merge = 'NAICS_' + str(i)
    sector_add = 'NAICS_' + str(i - j)

    cw_dict = {}
    if 'Sector' in df.columns:
        cw_dict['Sector'] = cw_load[
            [sector_merge, sector_add]].drop_duplicates()
    else:
        if isinstance(cw_load, dict):
            for s in ['Produced', 'Consumed']:
                cw = cw_load[f'Sector{s}By'][[sector_merge,
                                              sector_add]].drop_duplicates()
                cw_dict[s] = cw
        else:
            cw_dict['SectorProducedBy'] = cw_load[
                [sector_merge, sector_add]].drop_duplicates()
            cw_dict['SectorConsumedBy'] = cw_load[
                [sector_merge, sector_add]].drop_duplicates()

    cw_melt = load_sector_length_cw_melt()
    cw_sub = cw_melt[cw_melt['SectorLength'] == i]
    sector_list = cw_sub['Sector'].drop_duplicates().values.tolist()

    # loop through and add additional sectors
    if 'Sector' in df.columns:
        sectype_list = ['Sector']
    else:
        sectype_list = ['SectorProducedBy', 'SectorConsumedBy']
    for s in sectype_list:
        dfm = df[df[s].isin(sector_list)]
        dfm = dfm.merge(cw_dict[s], how='left', left_on=[s],
                        right_on=sector_merge)
        # replace sector column with matched sector add
        dfm[s] = np.where(
            ~dfm[sector_add].isnull(), dfm[sector_add],
            dfm[s])
        dfm = dfm.drop(columns=[sector_merge, sector_add])
        dfm = replace_NoneType_with_empty_cells(dfm)

        # aggregate the new sector flow amounts
        if 'FlowAmount' in dfm.columns:
            agg_sectors = aggregator(dfm, group_cols)
        # if FlowName is not in column and instead aggregating for the
        # HelperFlow then simply sum helper flow column
        else:
            agg_sectors = dfm.groupby(group_cols)['HelperFlow'] \
                .sum().reset_index()
        # append to df
        agg_sectors = replace_NoneType_with_empty_cells(agg_sectors)
        cols = [e for e in df.columns if e in
                ['FlowName', 'Flowable', 'Class', 'SectorProducedBy',
                 'SectorConsumedBy', 'Sector', 'Compartment', 'Context',
                 'Location', 'Unit', 'FlowType', 'Year']]
        # get copies where the indices are the columns of interest
        df_2 = df.set_index(cols)
        agg_sectors_2 = agg_sectors.set_index(cols)
        # Look for index overlap, ~
        dfi = agg_sectors[~agg_sectors_2.index.isin(df_2.index)]
        df = pd.concat([df, dfi], ignore_index=True).reset_index(
            drop=True)

    return df


# def sector_disaggregation(df_load):
#     """
#     function to disaggregate sectors if there is only one
#     naics at a lower level works for lower than naics 4
#     :param df_load: A FBS df, must have sector columns
#     :return: A FBS df with values for the missing naics5 and naics6
#     """
#
#     # ensure None values are not strings
#     df = replace_NoneType_with_empty_cells(df_load)
#
#     # determine if activities are sector-like, if aggregating
#     # a df with a 'SourceName'
#     sector_like_activities = check_activities_sector_like(df_load)
#
#     # if activities are sector like, drop columns while running disag then
#     # add back in
#     if sector_like_activities:
#         df = df.drop(columns=['ActivityProducedBy', 'ActivityConsumedBy'],
#                      errors='ignore')
#         df = df.reset_index(drop=True)
#
#     # load naics 2 to naics 6 crosswalk
#     cw_load = load_crosswalk('sector_length')
#
#     # appends missing naics levels to df
#     for i in range(2, 6):
#         dfm = subset_and_merge_df_by_sector_lengths(
#             df, i, i + 1, keep_paired_sectors_not_in_subset_list=True)
#
#         # only keep values in left column, meaning there are no less
#         # aggregated naics in the df
#         dfm2 = dfm.query('_merge=="left_only"').drop(
#             columns=['_merge', 'SPB_tmp', 'SCB_tmp'])
#
#         sector_merge = 'NAICS_' + str(i)
#         sector_add = 'NAICS_' + str(i + 1)
#
#         # subset the df by naics length
#         cw = cw_load[[sector_merge, sector_add]].drop_duplicates()
#         # only keep the rows where there is only one value in sector_add for
#         # a value in sector_merge
#         cw = cw.drop_duplicates(subset=[sector_merge], keep=False).reset_index(
#             drop=True)
#
#         # loop through and add additional naics
#         sectype_list = ['Produced', 'Consumed']
#         for s in sectype_list:
#             # inner join - only keep rows where there are data in the crosswalk
#             dfm2 = dfm2.merge(cw, how='left', left_on=[f'Sector{s}By'],
#                               right_on=sector_merge)
#             dfm2[f'Sector{s}By'] = np.where(
#                 ~dfm2[sector_add].isnull(), dfm2[sector_add],
#                 dfm2[f'Sector{s}By'])
#             dfm2 = dfm2.drop(columns=[sector_merge, sector_add])
#         dfm3 = dfm2.dropna(subset=['SectorProducedBy', 'SectorConsumedBy'],
#                            how='all')
#         dfm3 = dfm3.reset_index(drop=True)
#         dfm3 = replace_NoneType_with_empty_cells(dfm3)
#         df = pd.concat([df, dfm3], ignore_index=True)
#
#     # drop duplicates that can arise if sectors are non-traditional naics
#     # (household and government)
#     df = df.drop_duplicates().reset_index(drop=True)
#
#     # if activities are source-like, set col values
#     # as copies of the sector columns
#     if sector_like_activities:
#         df = df.assign(ActivityProducedBy=df['SectorProducedBy'])
#         df = df.assign(ActivityConsumedBy=df['SectorConsumedBy'])
#
#     return df


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
        log.warning(
            "Missing FIPS codes from crosswalk for %s. "
            "Assigning to FIPS_2010", str(year_of_data))
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
    fbs = clean_df(fbs, flow_by_sector_fields, fbs_fill_na_dict)

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
    fbs_collapsed = clean_df(fbs_collapsed, flow_by_sector_collapsed_fields,
                             fbs_collapsed_fill_na_dict)
    fbs_collapsed = fbs_collapsed.sort_values(
        ['Sector', 'Flowable', 'Context', 'Location']).reset_index(drop=True)

    return fbs_collapsed


def return_activity_from_scale(df, provided_from_scale):
    """
    Determine the 'from scale' used for aggregation/df
    subsetting for each activity combo in a df
    :param df: flowbyactivity df
    :param provided_from_scale: str, The scale to use specified in method yaml
    :return: df, FBA with column indicating the "from" geoscale to
        use for each row
    """

    # determine the unique combinations of activityproduced/consumedby
    df = replace_NoneType_with_empty_cells(df)
    unique_activities = unique_activity_names(df)
    # filter by geoscale
    fips = create_geoscale_list(df, provided_from_scale)
    # determine unique activities after subsetting by geoscale
    unique_activities_sub = \
        unique_activity_names(df[df['Location'].isin(fips)])

    # return df of the difference between unique_activities
    # and unique_activities2
    df_missing = dataframe_difference(
        unique_activities, unique_activities_sub, which='left_only')
    # return df of the similarities between unique_activities
    # and unique_activities2
    df_existing = dataframe_difference(
        unique_activities, unique_activities_sub, which='both')
    df_existing = df_existing.drop(columns='_merge')
    df_existing['activity_from_scale'] = provided_from_scale

    if len(df_missing) > 0:
        # for loop through geoscales until find data for each activity combo
        if provided_from_scale == 'national':
            geoscales = ['state', 'county']
        elif provided_from_scale == 'state':
            geoscales = ['county']
        elif provided_from_scale == 'county':
            log.error('Missing county level data')

        for i in geoscales:
            # filter by geoscale
            fips_i = create_geoscale_list(df, i)
            df_i = df[df['Location'].isin(fips_i)]

            # determine unique activities after subsetting by geoscale
            unique_activities_i = unique_activity_names(df_i)

            # return df of the difference between unique_activities subset and
            # unique_activities for geoscale
            df_missing_i = dataframe_difference(
                unique_activities_sub, unique_activities_i, which='right_only')
            df_missing_i = df_missing_i.drop(columns='_merge')
            df_missing_i['activity_from_scale'] = i
            # return df of the similarities between unique_activities
            # and unique_activities2
            df_existing_i = dataframe_difference(
                unique_activities_sub, unique_activities_i, which='both')

            # append unique activities and df with defined activity_from_scale
            unique_activities_sub = pd.concat([unique_activities_sub,
                df_missing_i[[fba_activity_fields[0], fba_activity_fields[1]]]],
                                              ignore_index=True)
            df_existing = pd.concat([df_existing, df_missing_i],
                                    ignore_index=True)
            df_missing = dataframe_difference(
                df_missing[[fba_activity_fields[0], fba_activity_fields[1]]],
                df_existing_i[[fba_activity_fields[0],
                               fba_activity_fields[1]]], which=None)

    return df_existing


def unique_activity_names(fba_df):
    """
    Determine the unique activity names in a df
    :param fba_df: a flowbyactivity df
    :return: df with ActivityProducedBy and ActivityConsumedBy columns
    """

    activities = fba_df[[fba_activity_fields[0], fba_activity_fields[1]]]
    unique_activities = activities.drop_duplicates().reset_index(drop=True)

    return unique_activities


def dataframe_difference(df1, df2, which=None):
    """
    Find rows which are different between two DataFrames
    :param df1: df, FBA or FBS
    :param df2: df, FBA or FBS
    :param which: 'both', 'right_only', 'left_only'
    :return: df, comparison of data in the two dfs
    """
    comparison_df = df1.merge(df2,
                              indicator=True,
                              how='outer')
    if which is None:
        diff_df = comparison_df[comparison_df['_merge'] != 'both']
    else:
        diff_df = comparison_df[comparison_df['_merge'] == which]

    return diff_df


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
    fba = flowsa.getFlowByActivity(
        datasource, year, **fba_dict).reset_index(drop=True)
    # convert to standardized units either by mapping to federal
    # flow list/material flow list or by using function. Mapping will add
    # context and flowable columns
    if 'allocation_map_to_flow_list' in kwargs:
        if kwargs['allocation_map_to_flow_list']:
            # ensure df loaded correctly/has correct dtypes
            fba = clean_df(fba, flow_by_activity_fields, fba_fill_na_dict,
                           drop_description=False)
            fba, mapping_files = map_fbs_flows(
                fba, datasource, kwargs, keep_fba_columns=True,
                keep_unmapped_rows=True)
        else:
            # ensure df loaded correctly/has correct dtypes
            fba = clean_df(fba, flow_by_activity_fields, fba_fill_na_dict)
            fba = standardize_units(fba)
    else:
        # ensure df loaded correctly/has correct dtypes
        fba = clean_df(fba, flow_by_activity_fields, fba_fill_na_dict,
                       drop_description=False)
        fba = standardize_units(fba)

    return fba


def assign_columns_of_sector_levels(df_load):
    """
    Add additional column capturing the sector level in the two columns
    :param df_load: df with at least on sector column
    :param ambiguous_sector_assignment: if there are sectors that can be
    assigned to multiple sector lengths (e.g., for government or household
    sectors), option to specify which sector assignment to keep.
    :return: df with new column for sector length
    """
    df = replace_NoneType_with_empty_cells(df_load)
    # load cw with column of sector levels
    cw = load_sector_length_cw_melt()
    # merge df assigning sector lengths
    for s in ['Produced', 'Consumed']:
        df = df.merge(cw, how='left', left_on=f'Sector{s}By',
                      right_on='Sector').drop(columns=['Sector']).rename(
            columns={'SectorLength': f'Sector{s}ByLength'})
        df[f'Sector{s}ByLength'] = df[f'Sector{s}ByLength'].fillna(0)

    duplicate_cols = [e for e in df.columns if e not in [
        'SectorProducedByLength', 'SectorConsumedByLength']]
    duplicate_df = df[df.duplicated(subset=duplicate_cols,
                                    keep=False)].reset_index(drop=True)

    if len(duplicate_df) > 0:
        log.warning('There are duplicate rows caused by ambiguous sectors.')

    dfc = df.sort_values(['SectorProducedByLength',
                          'SectorConsumedByLength']).reset_index(drop=True)
    return dfc


def assign_sector_match_column(df_load, sectorcolumn, sectorlength,
                               sectorlengthmatch):

    sector = 'NAICS_' + str(sectorlength)
    sector_add = 'NAICS_' + str(sectorlengthmatch)

    cw_load = load_crosswalk("sector_length")
    cw = cw_load[[sector, sector_add]].drop_duplicates().reset_index(
        drop=True)

    df = df_load.merge(cw, how='left', left_on=sectorcolumn,
                       right_on=sector
                       ).rename(columns={sector_add: 'sector_group'}
                                ).drop(columns=sector)

    return df


# def aggregate_and_subset_for_target_sectors(df, method):
#     """Helper function to create data at aggregated NAICS prior to
#     subsetting based on the target_sector_list. Designed for use when
#     FBS are the source data.
#     """
#     from flowsa.sectormapping import get_sector_list
#     # return sector level specified in method yaml
#     # load the crosswalk linking sector lengths
#     secondary_sector_level = method.get('target_subset_sector_level')
#     sector_list = get_sector_list(
#         method.get('target_sector_level'),
#         secondary_sector_level_dict=secondary_sector_level)
#
#     # subset df to get NAICS at the target level
#     df_agg = sector_aggregation(df)
#     df_subset = subset_df_by_sector_list(df_agg, sector_list)
#
#     return df_subset
