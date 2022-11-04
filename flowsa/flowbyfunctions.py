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
from flowsa.common import fbs_activity_fields, sector_level_key, \
    load_crosswalk, fbs_fill_na_dict, check_activities_sector_like, \
    fbs_collapsed_default_grouping_fields, fbs_collapsed_fill_na_dict, \
    fba_activity_fields, fba_default_grouping_fields, \
    load_sector_length_cw_melt, fba_fill_na_dict, \
    fba_mapped_default_grouping_fields
from flowsa.dataclean import clean_df, replace_strings_with_NoneType, \
    replace_NoneType_with_empty_cells, standardize_units
from flowsa.location import US_FIPS, get_state_FIPS, \
    get_county_FIPS, update_geoscale, fips_number_key
from flowsa.schema import flow_by_activity_fields, flow_by_sector_fields, \
    flow_by_sector_collapsed_fields, flow_by_activity_mapped_fields
from flowsa.settings import log, vLogDetailed, vLog


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


def aggregator(df, groupbycols, retain_zeros=True):
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

    # reset index
    df = df.reset_index(drop=True)
    # tmp replace null values with empty cells
    df = replace_NoneType_with_empty_cells(df)

    # drop columns with flowamount = 0
    if retain_zeros is False:
        df = df[df['FlowAmount'] != 0]

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

    df_dfg = df.groupby(groupbycols).agg({'FlowAmount': ['sum']})

    def is_identical(s):
        a = s.to_numpy()
        return (a[0] == a).all()

    # run through other columns creating weighted average
    for e in column_headers:
        if len(df) > 0 and is_identical(df[e]):
            df_dfg.loc[:, e] = df[e].iloc[0]
        else:
            df_dfg[e] = get_weighted_average(df, e, 'FlowAmount', groupbycols)

    df_dfg = df_dfg.reset_index()
    df_dfg.columns = df_dfg.columns.droplevel(level=1)

    # if datatypes are strings, ensure that Null values remain NoneType
    df_dfg = replace_strings_with_NoneType(df_dfg)

    return df_dfg


def sector_ratios(df, sectorcolumn):
    """
    Determine ratios of the less aggregated sectors within a
    more aggregated sector
    :param df: A df with sector columns
    :param sectorcolumn: 'SectorConsumedBy' or 'SectorProducedBy'
    :return: df, with 'FlowAmountRatio' column
    """

    # drop any null rows (can occur when activities are ranges)
    df = df[~df[sectorcolumn].isnull()]

    # find the longest length sector
    length = max(df[sectorcolumn].apply(lambda x: len(str(x))).unique())
    # for loop in reverse order longest length naics minus 1 to 2
    # appends missing naics levels to df
    sec_ratios = []
    for i in range(length, 3, -1):
        # subset df to sectors with length = i
        df_subset = subset_df_by_sector_lengths(df, [i])
        # create column for sector grouping
        df_subset = assign_sector_match_column(df_subset, sectorcolumn, i, i-1)
        # subset df to create denominator
        df_denom = df_subset[['FlowAmount', 'Location', 'sector_group']]
        df_denom = df_denom.groupby(['Location', 'sector_group'],
                                    as_index=False).agg({"FlowAmount": sum})
        df_denom = df_denom.rename(columns={"FlowAmount": "Denominator"})
        # merge the denominator column with fba_w_sector df
        ratio_df = df_subset.merge(df_denom, how='left')
        # calculate ratio
        ratio_df.loc[:, 'FlowAmountRatio'] = \
            ratio_df['FlowAmount'] / ratio_df['Denominator']
        ratio_df = ratio_df.drop(
            columns=['Denominator', 'sector_group'])
        sec_ratios.append(ratio_df)
    # concat list of dataframes (info on each page)
    df_w_ratios = pd.concat(sec_ratios, ignore_index=True)

    return df_w_ratios


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


def sector_aggregation(df_load, return_all_possible_sector_combos=False,
                       sectors_to_exclude_from_agg=None):
    """
    Function that checks if a sector length exists, and if not,
    sums the less aggregated sector
    :param df_load: Either a flowbyactivity df with sectors or
       a flowbysector df
    :param return_all_possible_sector_combos: bool, default false, if set to
    true, will return all possible combinations of sectors at each sector
    length (ex. a 4 digit SectorProducedBy will have rows for 2-6 digit
    SectorConsumedBy). This will result in a df with double counting.
    :param sectors_to_exclude_from_agg: list or dict, sectors that should not be
    aggregated beyond the sector level provided. Dictionary if separate lists
    for SectorProducedBy and SectorConsumedBy
    :return: df, with aggregated sector values
    """
    # ensure None values are not strings
    df = replace_NoneType_with_empty_cells(df_load)

    # determine grouping columns - based on datatype
    group_cols = list(df.select_dtypes(include=['object', 'int']).columns)
    # determine if activities are sector-like, if aggregating a df with a
    # 'SourceName'
    sector_like_activities = check_activities_sector_like(df_load)

    # if activities are sector like, drop columns while running ag then
    # add back in
    if sector_like_activities:
        # subset df
        df_cols = [e for e in df.columns if e not in
                   ('ActivityProducedBy', 'ActivityConsumedBy')]
        group_cols = [e for e in group_cols if e not in
                      ('ActivityProducedBy', 'ActivityConsumedBy')]
        df = df[df_cols]
        df = df.reset_index(drop=True)

    # load naics length crosswwalk
    cw_load = load_crosswalk('sector_length')
    # remove any parent sectors of sectors identified as those that should
    # not be aggregated
    if sectors_to_exclude_from_agg is not None:
        # if sectors are in a dictionary create cw for sectorproducedby and
        # sectorconsumedby otherwise single cr
        if isinstance(sectors_to_exclude_from_agg, dict):
            cws = {}
            for s in ['Produced', 'Consumed']:
                try:
                    cw = remove_parent_sectors_from_crosswalk(
                        cw_load, sectors_to_exclude_from_agg[f'Sector{s}By'])
                    cws[f'Sector{s}By'] = cw
                except KeyError:
                    cws[f'Sector{s}By'] = cw_load
            cw_load = cws.copy()
        else:
            cw_load = remove_parent_sectors_from_crosswalk(
                cw_load, sectors_to_exclude_from_agg)

    # find the longest length sector
    length = df[[fbs_activity_fields[0], fbs_activity_fields[1]]].apply(
        lambda x: x.str.len()).max().max()
    length = int(length)
    # for loop in reverse order longest length NAICS minus 1 to 2
    # appends missing naics levels to df
    for i in range(length, 2, -1):
        if return_all_possible_sector_combos:
            for j in range(1, i-1):
                df = append_new_sectors(df, i, j, cw_load, group_cols)
        else:
            df = append_new_sectors(df, i, 1, cw_load, group_cols)

    # if activities are source-like, set col values as
    # copies of the sector columns
    if sector_like_activities & ('FlowAmount' in df.columns) & \
            ('ActivityProducedBy' in df_load.columns):
        df = df.assign(ActivityProducedBy=df['SectorProducedBy'])
        df = df.assign(ActivityConsumedBy=df['SectorConsumedBy'])

    # replace null values
    df = replace_strings_with_NoneType(df).reset_index(drop=True)

    return df


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
    if isinstance(cw_load, dict):
        for s in ['Produced', 'Consumed']:
            cw = cw_load[f'Sector{s}By'][[sector_merge,
                                          sector_add]].drop_duplicates()
            cw_dict[s] = cw
    else:
        cw_dict['Produced'] = cw_load[
            [sector_merge, sector_add]].drop_duplicates()
        cw_dict['Consumed'] = cw_load[
            [sector_merge, sector_add]].drop_duplicates()

    cw_melt = load_sector_length_cw_melt()
    cw_sub = cw_melt[cw_melt['SectorLength'] == i]
    sector_list = cw_sub['Sector'].drop_duplicates().values.tolist()

    # loop through and add additional sectors
    sectype_list = ['Produced', 'Consumed']
    for s in sectype_list:
        dfm = df[df[f'Sector{s}By'].isin(sector_list)]
        dfm = dfm.merge(cw_dict[s], how='left', left_on=[f'Sector{s}By'],
                        right_on=sector_merge)
        # replace sector column with matched sector add
        dfm[f'Sector{s}By'] = np.where(
            ~dfm[sector_add].isnull(), dfm[sector_add],
            dfm[f'Sector{s}By'])
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
                 'SectorConsumedBy', 'Compartment', 'Context', 'Location',
                 'Unit', 'FlowType', 'Year']]
        # get copies where the indices are the columns of interest
        df_2 = df.set_index(cols)
        agg_sectors_2 = agg_sectors.set_index(cols)
        # Look for index overlap, ~
        dfi = agg_sectors[~agg_sectors_2.index.isin(df_2.index)]
        df = pd.concat([df, dfi], ignore_index=True).reset_index(
            drop=True)

    return df


def sector_disaggregation(df_load):
    """
    function to disaggregate sectors if there is only one
    naics at a lower level works for lower than naics 4
    :param df_load: A FBS df, must have sector columns
    :return: A FBS df with values for the missing naics5 and naics6
    """

    # ensure None values are not strings
    df = replace_NoneType_with_empty_cells(df_load)

    # determine if activities are sector-like, if aggregating
    # a df with a 'SourceName'
    sector_like_activities = check_activities_sector_like(df_load)

    # if activities are sector like, drop columns while running disag then
    # add back in
    if sector_like_activities:
        df = df.drop(columns=['ActivityProducedBy', 'ActivityConsumedBy'],
                     errors='ignore')
        df = df.reset_index(drop=True)

    # load naics 2 to naics 6 crosswalk
    cw_load = load_crosswalk('sector_length')

    # appends missing naics levels to df
    for i in range(2, 6):
        dfm = subset_and_merge_df_by_sector_lengths(
            df, i, i + 1, keep_paired_sectors_not_in_subset_list=True)

        # only keep values in left column, meaning there are no less
        # aggregated naics in the df
        dfm2 = dfm.query('_merge=="left_only"').drop(
            columns=['_merge', 'SPB_tmp', 'SCB_tmp'])

        sector_merge = 'NAICS_' + str(i)
        sector_add = 'NAICS_' + str(i + 1)

        # subset the df by naics length
        cw = cw_load[[sector_merge, sector_add]].drop_duplicates()
        # only keep the rows where there is only one value in sector_add for
        # a value in sector_merge
        cw = cw.drop_duplicates(subset=[sector_merge], keep=False).reset_index(
            drop=True)

        # loop through and add additional naics
        sectype_list = ['Produced', 'Consumed']
        for s in sectype_list:
            # inner join - only keep rows where there are data in the crosswalk
            dfm2 = dfm2.merge(cw, how='left', left_on=[f'Sector{s}By'],
                              right_on=sector_merge)
            dfm2[f'Sector{s}By'] = np.where(
                ~dfm2[sector_add].isnull(), dfm2[sector_add],
                dfm2[f'Sector{s}By'])
            dfm2 = dfm2.drop(columns=[sector_merge, sector_add])
        dfm3 = dfm2.dropna(subset=['SectorProducedBy', 'SectorConsumedBy'],
                           how='all')
        dfm3 = dfm3.reset_index(drop=True)
        dfm3 = replace_NoneType_with_empty_cells(dfm3)
        df = pd.concat([df, dfm3], ignore_index=True)

    # drop duplicates that can arise if sectors are non-traditional naics
    # (household and government)
    df = df.drop_duplicates().reset_index(drop=True)

    # if activities are source-like, set col values
    # as copies of the sector columns
    if sector_like_activities:
        df = df.assign(ActivityProducedBy=df['SectorProducedBy'])
        df = df.assign(ActivityConsumedBy=df['SectorConsumedBy'])

    return df


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


def return_primary_sector_column(df_load):
    """
    Determine sector column with values
    :param fbs: fbs df with two sector columns
    :return: string, primary sector column
    """
    # determine the df_w_sector column to merge on
    if 'Sector' in df_load.columns:
        primary_sec_column = 'Sector'
    else:
        df = replace_strings_with_NoneType(df_load)
        sec_consumed_list = \
            df['SectorConsumedBy'].drop_duplicates().values.tolist()
        sec_produced_list = \
            df['SectorProducedBy'].drop_duplicates().values.tolist()
        # if a sector field column is not all 'none', that is the column to
        # merge
        if all(v is None for v in sec_consumed_list):
            primary_sec_column = 'SectorProducedBy'
        elif all(v is None for v in sec_produced_list):
            primary_sec_column = 'SectorConsumedBy'
        else:
            log.error('There are values in both SectorProducedBy and '
                      'SectorConsumedBy, cannot isolate Sector column')
    return primary_sec_column


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


def subset_df_by_geoscale(df, activity_from_scale, activity_to_scale):
    """
    Subset a df by geoscale or agg to create data specified in method yaml
    :param df: df, FBA format
    :param activity_from_scale: str, identified geoscale by which to subset or
                                aggregate from ('national', 'state', 'county')
    :param activity_to_scale: str, identified geoscale by which to subset or
                              aggregate to ('national', 'state', 'county')
    :return: df, FBA, subset or aggregated to a single geoscale for all rows
    """

    # detect grouping cols by columns
    if 'Context' in df.columns:
        groupbycols = fba_mapped_default_grouping_fields
        cols_to_keep = flow_by_activity_mapped_fields
    else:
        groupbycols = fba_default_grouping_fields
        cols_to_keep = flow_by_activity_fields

    # method of subset dependent on LocationSystem
    if df['LocationSystem'].str.contains('FIPS').any():
        df = df[df['LocationSystem'].str.contains(
            'FIPS')].reset_index(drop=True)
        # determine 'activity_from_scale' for use in df
        # geoscale subset, by activity
        modified_from_scale = \
            return_activity_from_scale(df, activity_from_scale)
        # add 'activity_from_scale' column to df
        df2 = pd.merge(df, modified_from_scale)

        # list of unique 'from' geoscales
        unique_geoscales = modified_from_scale[
            'activity_from_scale'].drop_duplicates().values.tolist()
        if len(unique_geoscales) > 1:
            log.info('Dataframe has a mix of geographic levels: %s',
                     ', '.join(unique_geoscales))

        # to scale
        if fips_number_key[activity_from_scale] > \
                fips_number_key[activity_to_scale]:
            to_scale = activity_to_scale
        else:
            to_scale = activity_from_scale

        df_subset_list = []
        # subset df based on activity 'from' scale
        for i in unique_geoscales:
            df3 = df2[df2['activity_from_scale'] == i]
            # if desired geoscale doesn't exist, aggregate existing data
            # if df is less aggregated than allocation df, aggregate
            # fba activity to allocation geoscale
            if fips_number_key[i] > fips_number_key[to_scale]:
                log.info("Aggregating subset from %s to %s", i, to_scale)
                df_sub = agg_by_geoscale(df3, i, to_scale, groupbycols)
            # else filter relevant rows
            else:
                log.info("Subsetting %s data", i)
                df_sub = filter_by_geoscale(df3, i)
            df_subset_list.append(df_sub)
        df_subset = pd.concat(df_subset_list, ignore_index=True)

        # drop unused columns
        df_subset = clean_df(df_subset, cols_to_keep,
                             fba_fill_na_dict, drop_description=False)

        return df_subset

    # right now, the only other location system is for Statistics Canada data
    else:
        return df


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


def equally_allocate_suppressed_parent_to_child_naics(
        df_load, method, sector_column, groupcols,
        equally_allocate_parent_to_child=True):
    """
    Estimate data suppression, by equally allocating parent NAICS
    values to child NAICS
    :param df_load: df with sector columns
    :param method: dictionary, FBS method yaml
    :param sector_column: str, column to estimate suppressed data for
    :param groupcols: list, columns to group df by
    :param equally_allocate_parent_to_child: default True, if True will
    first equally allocate parent to child sectors if the child sector is
    missing
    :return: df, with estimated suppressed data
    """
    from flowsa.allocation import equally_allocate_parent_to_child_naics
    from flowsa.validation import \
        compare_child_to_parent_sectors_flowamounts, \
        compare_summation_at_sector_lengths_between_two_dfs

    vLogDetailed.info('Estimating suppressed data by equally allocating '
                      'parent to child sectors.')
    df = sector_disaggregation(df_load)

    # equally allocate parent to child naics where child naics are not
    # included in the dataset. This step is necessary to accurately
    # calculate the flow that has already been allocated. Must allocate to
    # NAICS_6 for suppressed data function to work correctly.
    if equally_allocate_parent_to_child:
        vLogDetailed.info('Before estimating suppressed data, equally '
                          'allocate parent sectors to child sectors.')
        df = equally_allocate_parent_to_child_naics(
            df, method, overwritetargetsectorlevel='NAICS_6')

    df = replace_NoneType_with_empty_cells(df)
    df = df[df[sector_column] != '']

    # determine if activities are sector-like,
    # if aggregating a df with a 'SourceName'
    sector_like_activities = check_activities_sector_like(df_load)
    if sector_like_activities is False:
        log.error('Function is not written to estimate suppressed data when '
                  'activities are not NAICS-like.')

    # if activities are source like, drop from df,
    # add back in as copies of sector columns columns to keep
    if sector_like_activities:
        # subset df
        df_cols = [e for e in df.columns if e not in
                   ('ActivityProducedBy', 'ActivityConsumedBy')]
        df = df[df_cols]
        # drop activity from groupby
        groupcols = [e for e in groupcols if e
                     not in ['ActivityConsumedBy', 'ActivityProducedBy',
                             'Description']]

    # load naics 2 to naics 6 crosswalk
    cw_load = load_crosswalk('sector_length')
    # only keep official naics
    cw = cw_load.drop(columns=['NAICS_7']).drop_duplicates()
    cw_melt = pd.melt(cw,
        id_vars=["NAICS_6"], var_name="NAICS_Length",
        value_name="NAICS_Match").drop(
        columns=['NAICS_Length']).drop_duplicates()

    df_sup = df[df['FlowAmount'] == 0].reset_index(drop=True)
    # merge the naics cw
    new_naics = pd.merge(df_sup, cw_melt, how='left',
                         left_on=[sector_column], right_on=['NAICS_Match'])
    # drop rows where match is null because no additional naics to add
    new_naics = new_naics.dropna()
    new_naics[sector_column] = new_naics['NAICS_6'].copy()
    new_naics = new_naics.drop(columns=['NAICS_6', 'NAICS_Match'])

    # if a parent and child naics are both suppressed, can get situations
    # where a naics6 code is duplicated because both the parent and child
    # will match with the naics6. Therefore, drop duplicates
    new_naics2 = new_naics.drop_duplicates()

    # merge the new naics with the existing df, if data already
    # existed for a NAICS6, keep the original
    dfm = pd.merge(
        new_naics2[groupcols], df, how='left', on=groupcols,
        indicator=True).query('_merge=="left_only"').drop('_merge', axis=1)
    dfm = replace_NoneType_with_empty_cells(dfm)
    dfm = dfm.fillna(0)
    df = pd.concat([df, dfm], ignore_index=True)
    # add length column and subset the data
    # subtract out existing data at NAICS6 from total data
    # at a length where no suppressed data
    drop_col = 'SectorConsumedByLength'
    if sector_column == 'SectorConsumedBy':
        drop_col = 'SectorProducedByLength'
    df = assign_columns_of_sector_levels(df).rename(
        columns={f'{sector_column}Length': 'SectorLength'}).drop(columns=[
        drop_col])
    # df with non-suppressed data only
    dfns = df[df['FlowAmount'] != 0].reset_index(drop=True)

    df_sup2 = pd.DataFrame()
    cw_load = load_crosswalk('sector_length')
    df_sup = df_sup.assign(SectorMatchFlow=np.nan)
    merge_cols = list(df_sup.select_dtypes(
        include=['object', 'int']).columns)
    # also drop sector and description cols
    merge_cols = [c for c in merge_cols
                  if c not in ['SectorConsumedBy', 'SectorProducedBy',
                               'Description']]
    # subset the df by length i
    dfs = subset_df_by_sector_lengths(df_sup, [6])

    counter = 1
    while dfs.isnull().values.any() and 6-counter > 1:
        # subset the crosswalk by i and i-1
        cw = cw_load[[f'NAICS_6',
                      f'NAICS_{6-counter}']].drop_duplicates()
        # merge df with the cw to determine which sector to look for in
        # non-suppressed data
        for s in ['Produced', 'Consumed']:
            dfs = dfs.merge(cw, how='left', left_on=f'Sector{s}By',
                            right_on=f'NAICS_6').drop(
                columns=f'NAICS_6').rename(
                columns={f'NAICS_{6-counter}': f'Sector{s}Match'})
            dfs[f'Sector{s}Match'] = dfs[f'Sector{s}Match'].fillna('')
        # merge with non suppressed data
        dfs = dfs.merge(dfns, how='left',
                        left_on=merge_cols + ['SectorProducedMatch',
                                              'SectorConsumedMatch'],
                        right_on=merge_cols + ['SectorProducedBy',
                                               'SectorConsumedBy'])
        dfs['SectorMatchFlow'].fillna(dfs['FlowAmount_y'], inplace=True)
        # drop all columns from the non suppressed data
        dfs = dfs[dfs.columns[~dfs.columns.str.endswith('_y')]]
        dfs.columns = dfs.columns.str.replace('_x', '')
        # subset the df into rows assigned a new value and those not
        dfs_assigned = dfs[~dfs['SectorMatchFlow'].isnull()]
        dfs = dfs[dfs['SectorMatchFlow'].isnull()].drop(
            columns=['SectorProducedMatch', 'SectorConsumedMatch',
                     'SectorLength']).reset_index(drop=True)
        df_sup2 = pd.concat([df_sup2, dfs_assigned], ignore_index=True)
        counter = counter + 1

    # merge in the df where calculated how much flow has already been
    # allocated to NAICS6
    mergecols = [e for e in groupcols if e not in
                 ['SectorProducedBy', 'SectorConsumedBy']]
    mergecols = mergecols + ['SectorProducedMatch', 'SectorConsumedMatch']
    meltcols = mergecols + ['sector_allocated']

    if len(df_sup2) > 0:
        for ii in range(5, 1, -1):
            # subset the df by length i
            dfs = df_sup2[df_sup2['SectorLength'] == ii]

            dfns_sub = dfns[dfns['SectorLength'] == 6].reset_index(drop=True)
            for s in ['Produced', 'Consumed']:
                dfns_sub = assign_sector_match_column(
                    dfns_sub, f'Sector{s}By', 6, ii).rename(
                    columns={'sector_group': f'Sector{s}Match'})
                dfns_sub = dfns_sub.fillna('')
            dfsum = dfns_sub.groupby(mergecols, as_index=False).agg(
                {"FlowAmount": sum}).rename(columns={
                "FlowAmount": 'sector_allocated'})

            df_sup3 = dfs.merge(dfsum[meltcols], on=mergecols, how='left')
            df_sup3['sector_allocated'] = df_sup3['sector_allocated'].fillna(0)
            # calc the remaining flow that can be allocated
            df_sup3['FlowRemainder'] = df_sup3['SectorMatchFlow'] - \
                                       df_sup3['sector_allocated']
            # Due to rounding, there can be slight differences in data at
            # sector levels, which can result in some minor negative values.
            # If the percent of FlowRemainder is less than the assigned
            # tolerance for negative numbers, or if the flowremainder is
            # -1, reset the number to 0. If it is greater, issue a warning.
            percenttolerance = 1
            flowtolerance = -1
            df_sup3 = df_sup3.assign(PercentOfAllocated=
                                     (abs(df_sup3['FlowRemainder']) / df_sup3[
                                         'SectorMatchFlow']) * 100)
            df_sup3['FlowRemainder'] = np.where(
                (df_sup3["FlowRemainder"] < 0) &
                (df_sup3['PercentOfAllocated'] < percenttolerance), 0,
                df_sup3['FlowRemainder'])
            df_sup3['FlowRemainder'] = np.where(
                df_sup3["FlowRemainder"].between(flowtolerance, 0), 0,
                df_sup3['FlowRemainder'])

            # check for negative values
            negv = df_sup3[df_sup3['FlowRemainder'] < 0]
            if len(negv) > 0:
                col_subset = [e for e in negv.columns if e in
                              ['Class', 'SourceName', 'FlowName',
                               'Flowable', 'FlowAmount', 'Unit',
                               'Compartment', 'Context', 'Location', 'Year',
                               'SectorProducedBy', 'SectorConsumedBy',
                               'SectorMatchFlow', 'SectorProducedMatch',
                               'SectorConsumedMatch', 'sector_allocated',
                               'FlowRemainder']]
                negv = negv[col_subset].reset_index(drop=True)
                vLog.info(
                    'There are negative values when allocating suppressed '
                    'parent data to child sector. The values are more than '
                    '%s%% of the total parent sector with a negative flow '
                    'amount being allocated more than %s. Resetting flow '
                    'values to be allocated to 0. See validation log for '
                    'details.', str(percenttolerance), str(flowtolerance))
                vLogDetailed.info('Values where flow remainders are '
                                  'negative, resetting to 0: '
                                  '\n {}'.format(negv.to_string()))
            df_sup3['FlowRemainder'] = np.where(df_sup3["FlowRemainder"] < 0,
                                                0, df_sup3['FlowRemainder'])
            df_sup3 = df_sup3.drop(columns=[
                'SectorMatchFlow', 'sector_allocated', 'PercentOfAllocated'])
            # add count column used to divide the unallocated flows
            sector_column_match = sector_column.replace('By', 'Match')
            df_sup3 = df_sup3.assign(secCount=df_sup3.groupby(mergecols)[
                sector_column_match].transform('count'))
            df_sup3 = df_sup3.assign(newFlow=df_sup3['FlowRemainder'] /
                                             df_sup3['secCount'])
            # reassign values and drop columns
            df_sup3 = df_sup3.assign(FlowAmount=df_sup3['newFlow'])
            df_sup3 = df_sup3.drop(columns=['SectorProducedMatch',
                                            'SectorConsumedMatch',
                                            'FlowRemainder', 'secCount',
                                            'newFlow'])
            # reset SectorLength
            df_sup3['SectorLength'] = 6
            # add to the df with no suppressed data
            dfns = pd.concat([dfns, df_sup3], ignore_index=True)

    dfns = dfns.drop(columns=['SectorLength'])
    dff = sector_aggregation(dfns)

    # if activities are source-like, set col values as copies
    # of the sector columns
    if sector_like_activities:
        dff = dff.assign(ActivityProducedBy=dff['SectorProducedBy'])
        dff = dff.assign(ActivityConsumedBy=dff['SectorConsumedBy'])
        # reindex columns
        dff = dff.reindex(df_load.columns, axis=1)

    vLogDetailed.info('Checking results of allocating suppressed parent to '
                      'child sectors. ')
    compare_summation_at_sector_lengths_between_two_dfs(df_load, dff)
    compare_child_to_parent_sectors_flowamounts(dff)
    # todo: add third check comparing smallest child naics (6) to largest (2)

    # replace null values
    dff = replace_strings_with_NoneType(dff).reset_index(drop=True)

    return dff


def collapse_activity_fields(df):
    """
    The 'activityconsumedby' and 'activityproducedby' columns from the
    allocation dataset do not always align with
    the dataframe being allocated. Generalize the allocation activity column.
    :param df: df, FBA used to allocate another FBA
    :return: df, single Activity column
    """

    df = replace_strings_with_NoneType(df)

    activity_consumed_list = \
        df['ActivityConsumedBy'].drop_duplicates().values.tolist()
    activity_produced_list = \
        df['ActivityProducedBy'].drop_duplicates().values.tolist()

    # if an activity field column is all 'none', drop the column and
    # rename renaming activity columns to generalize
    if all(v is None for v in activity_consumed_list):
        df = df.drop(columns=['ActivityConsumedBy', 'SectorConsumedBy'])
        df = df.rename(columns={'ActivityProducedBy': 'Activity',
                                'SectorProducedBy': 'Sector'})
    elif all(v is None for v in activity_produced_list):
        df = df.drop(columns=['ActivityProducedBy', 'SectorProducedBy'])
        df = df.rename(columns={'ActivityConsumedBy': 'Activity',
                                'SectorConsumedBy': 'Sector'})
    else:
        log.error('Cannot generalize dataframe')

    # drop other columns
    df = df.drop(columns=['ProducedBySectorType', 'ConsumedBySectorType'])

    return df


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


def subset_df_by_sector_lengths(df_load, sector_length_list, **_):
    """
    :param df_load:
    :param sector_length_list: list (int) of the naics sector lengths that
    should be subset
    :return:
    """
    # subset the df by naics length
    cw_load = load_sector_length_cw_melt()
    cw = cw_load[cw_load['SectorLength'].isin(sector_length_list)]
    sector_list = cw['Sector'].drop_duplicates().values.tolist()

    # if retaining values in adjacent sector columns that are not in the
    # sector list, the sector length of those values should be less than the
    # sector length of the approved list to prevent double counting when
    # looping through all data and pulling the same data twice
    sector_sub_list = []
    if _.get('keep_paired_sectors_not_in_subset_list'):
        if _.get('keep_shorter_sector_lengths'):
            v = min(sector_length_list)
            possible_sector_subset_lengths = list(range(2, 8))
            sector_subset_length_list = [
                x for x in possible_sector_subset_lengths if x < v]
        elif _.get('keep_shorter_sector_lengths') is False:
            v = max(sector_length_list)
            possible_sector_subset_lengths = list(range(2, 8))
            sector_subset_length_list = [
                x for x in possible_sector_subset_lengths if x > v]
        else:
            sector_subset_length_list = list(range(2, 8))

        cw_sub = cw_load[cw_load['SectorLength'].isin(
            sector_subset_length_list)]
        sector_sub_list = cw_sub['Sector'].drop_duplicates().values.tolist()

    df = subset_df_by_sector_list(
        df_load, sector_list, sector_sub_list=sector_sub_list, **_)

    return df


def subset_df_by_sector_list(df_load, sector_list, **_):
    """
    :param df_load:
    :param sector_list:
    :param keep_paired_sectors_not_in_subset_list: bool, default False. If
    True, then if values in both sector columns and one value is not in the
    sector list, keep the column. Used for the purposes of sector
    aggregation. Only for sectors where the paired sector has a sector
    length less than sector in sector list, otherwise will get double
    counting in sector_aggregation() and sector_disaggregation()
    :param **_: optional parameters
    :return:
    """
    df = replace_NoneType_with_empty_cells(df_load)

    c1 = (df['SectorProducedBy'].isin(sector_list) &
          (df['SectorConsumedBy'] == ''))
    c2 = (df['SectorProducedBy'] == '') & \
         (df['SectorConsumedBy'].isin(sector_list))
    c3 = (df['SectorProducedBy'].isin(sector_list) &
          df['SectorConsumedBy'].isin(sector_list))

    if _.get('keep_paired_sectors_not_in_subset_list'):
        # conditions if want to keep rows of data where one sector value is not
        # in the sector list
        c4 = (df['SectorProducedBy'].isin(sector_list) &
              (df['SectorConsumedBy'].isin(_['sector_sub_list'])))
        c5 = (df['SectorProducedBy'].isin(_['sector_sub_list'])) & \
             (df['SectorConsumedBy'].isin(sector_list))

        df = df[c1 | c2 | c3 | c4 | c5].reset_index(drop=True)
    else:
        df = df[c1 | c2 | c3].reset_index(drop=True)

    return df


def subset_and_merge_df_by_sector_lengths(
        df, length1, length2, **_):

    sector_merge = 'NAICS_' + str(length1)
    sector_add = 'NAICS_' + str(length2)

    # subset the df by naics length
    cw_load = load_crosswalk("sector_length")
    cw = cw_load[[sector_merge, sector_add]].drop_duplicates().reset_index(
        drop=True)

    # df where either sector column is length or both columns are
    df = df.reset_index(drop=True)
    df1 = subset_df_by_sector_lengths(df, [length1], **_)
    # second dataframe where length is length2
    df2 = subset_df_by_sector_lengths(df, [length2], **_)

    # merge the crosswalk to create new columns where sector length equals
    # "length1"
    df2 = df2.merge(cw, how='left', left_on=['SectorProducedBy'],
                    right_on=[sector_add]).rename(
        columns={sector_merge: 'SPB_tmp'}).drop(columns=sector_add)
    df2 = df2.merge(cw, how='left',
                    left_on=['SectorConsumedBy'], right_on=[sector_add]
                    ).rename(
        columns={sector_merge: 'SCB_tmp'}).drop(columns=sector_add)
    df2 = replace_NoneType_with_empty_cells(df2)
    # if maintaining the values that do not match the sector length
    # requirement, and if a column value is blank, replace with existing value
    if _.get('keep_paired_sectors_not_in_subset_list'):
        df2["SPB_tmp"] = np.where(df2["SPB_tmp"] == '',
                                  df2["SectorProducedBy"], df2["SPB_tmp"])
        df2["SCB_tmp"] = np.where(df2["SCB_tmp"] == '',
                                  df2["SectorConsumedBy"], df2["SCB_tmp"])

    # merge the dfs
    merge_cols = list(df1.select_dtypes(include=['object', 'int']).columns)
    # also drop activity and description cols
    merge_cols = [c for c in merge_cols
                  if c not in ['SectorConsumedBy', 'SectorProducedBy',
                               'Description']]

    dfm = df1.merge(df2[merge_cols + ['SPB_tmp', 'SCB_tmp']],
                    how='outer',
                    left_on=merge_cols + ['SectorProducedBy',
                                          'SectorConsumedBy'],
                    right_on=merge_cols + ['SPB_tmp', 'SCB_tmp'],
                    indicator=True)
    dfm = replace_NoneType_with_empty_cells(dfm)

    return dfm


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


def assign_columns_of_sector_levels_without_ambiguous_sectors(
        df_load, ambiguous_sector_assignment=None):

    dfc = assign_columns_of_sector_levels(df_load)

    # check for duplicates. Rows might be duplicated if a sector is the same
    # for multiple sector lengths
    duplicate_cols = [e for e in dfc.columns if e not in [
        'SectorProducedByLength', 'SectorConsumedByLength']]
    duplicate_df = dfc[dfc.duplicated(subset=duplicate_cols,
                                      keep=False)].reset_index(drop=True)

    if (len(duplicate_df) > 0) % (ambiguous_sector_assignment is not None):
        log.info('Retaining data for %s and dropping remaining '
                 'rows. See validation log for data dropped',
                 ambiguous_sector_assignment)
        # first drop all data in the duplicate_df from dfc
        dfs1 = pd.concat([dfc, duplicate_df]).drop_duplicates(keep=False)
        # drop sector length cols, drop duplicates, aggregate df to ensure
        # keep the intended data, and then reassign column sectors,
        # formatted this way because would like to avoid sector aggreggation
        # on large dfs
        dfs2 = duplicate_df.drop(
            columns=['SectorProducedByLength',
                     'SectorConsumedByLength']).drop_duplicates()
        dfs2 = sector_aggregation(dfs2)
        dfs2 = assign_columns_of_sector_levels(dfs2)
        # then in the duplicate df, only keep the rows that match the
        # parameter indicated in the function call
        sectorlength = sector_level_key[ambiguous_sector_assignment]
        dfs2 = dfs2[
            ((dfs2['SectorProducedByLength'] == sectorlength) &
             (dfs2['SectorConsumedByLength'] == 0))
            |
            ((dfs2['SectorProducedByLength'] == 0) &
             (dfs2['SectorConsumedByLength'] == sectorlength))
            |
            ((dfs2['SectorProducedByLength'] == sectorlength) &
             (dfs2['SectorConsumedByLength'] == sectorlength))
        ].reset_index(drop=True)
        if len(dfs2) == 0:
            log.warning('Data is lost from dataframe because none of the '
                        'ambiguous sectors match %s',
                        ambiguous_sector_assignment)
        # merge the two dfs
        dfc = pd.concat([dfs1, dfs2])
        # print out what data was dropped
        df_dropped = pd.merge(
            duplicate_df, dfs2, how='left', indicator=True).query(
            '_merge=="left_only"').drop('_merge', axis=1)
        df_dropped = df_dropped[
            ['SectorProducedBy', 'SectorConsumedBy',
             'SectorProducedByLength', 'SectorConsumedByLength'
             ]].drop_duplicates().reset_index(drop=True)
        vLogDetailed.info('After assigning a column of sector lengths, '
                          'dropped data with the following sector '
                          'assignments due to ambiguous sector lengths '
                          '%s: \n {}'.format(df_dropped.to_string()))
    dfc = dfc.sort_values(['SectorProducedByLength',
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


def aggregate_and_subset_for_target_sectors(df, method):
    """Helper function to create data at aggregated NAICS prior to
    subsetting based on the target_sector_list. Designed for use when
    FBS are the source data.
    """
    from flowsa.sectormapping import get_sector_list
    # return sector level specified in method yaml
    # load the crosswalk linking sector lengths
    secondary_sector_level = method.get('target_subset_sector_level')
    sector_list = get_sector_list(
        method['target_sector_level'],
        secondary_sector_level_dict=secondary_sector_level)

    # subset df to get NAICS at the target level
    df_agg = sector_aggregation(df)
    df_subset = subset_df_by_sector_list(df_agg, sector_list)

    return df_subset


def add_column_of_allocation_sources(df, attr):
    """

    :param df_load:
    :param attr:
    :return:
    """
    # first assign a data source as the source name
    df = df.assign(AllocationSources=None)

    # if allocation method is not direct, add data sources
    if attr['allocation_method'] != 'direct':
        sources = []
        key_list = ['allocation_source', 'helper_source']
        for k in key_list:
            s = attr.get(k)
            if (s is not None) & (callable(s) is False):
                sources.append(s)
        if 'literature_sources' in attr:
            sources.append('literature values')
        # concat sources into single string
        allocation_sources = ', '.join(sources)
        # update data sources column with additional sources
        df = df.assign(AllocationSources=allocation_sources)
    return df
