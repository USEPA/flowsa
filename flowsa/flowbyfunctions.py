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
from flowsa.common import fbs_activity_fields, US_FIPS, get_state_FIPS, \
    get_county_FIPS, update_geoscale, load_yaml_dict, \
    load_crosswalk, fbs_fill_na_dict, \
    fbs_collapsed_default_grouping_fields, return_true_source_catalog_name, \
    fbs_collapsed_fill_na_dict, fba_activity_fields, \
    fips_number_key, fba_fill_na_dict, check_activities_sector_like, \
    fba_mapped_default_grouping_fields, fba_default_grouping_fields, \
    fba_wsec_default_grouping_fields, get_flowsa_base_name
from flowsa.schema import flow_by_activity_fields, flow_by_sector_fields, \
    flow_by_sector_collapsed_fields, flow_by_activity_mapped_fields
from flowsa.settings import datasourcescriptspath, log
from flowsa.dataclean import clean_df, replace_strings_with_NoneType, \
    replace_NoneType_with_empty_cells, standardize_units


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
        # all_FIPS = read_stored_FIPS()
        if geoscale == "state":
            state_FIPS = get_state_FIPS(year)
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
        log.error("No flows found in the flow dataset at the %s scale",
                  geoscale)
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


def aggregator(df, groupbycols):
    """
    Aggregates flowbyactivity or flowbysector 'FlowAmount' column in df and
    generate weighted average values based on FlowAmount values for numeric
    columns
    :param df: df, Either flowbyactivity or flowbysector
    :param groupbycols: list, Either flowbyactivity or flowbysector columns
    :return: df, with aggregated columns
    """

    # reset index
    df = df.reset_index(drop=True)
    # tmp replace null values with empty cells
    df = replace_NoneType_with_empty_cells(df)

    # drop columns with flowamount = 0
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

    df_dfg = df.groupby(groupbycols).agg({'FlowAmount': ['sum']})

    # run through other columns creating weighted average
    for e in column_headers:
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
        # subset df to sectors with length = i and length = i + 1
        df_subset = df.loc[df[sectorcolumn].apply(lambda x: len(x) == i)]
        # create column for sector grouping
        df_subset = df_subset.assign(
            Sector_group=df_subset[sectorcolumn].apply(lambda x: x[0:i-1]))
        # subset df to create denominator
        df_denom = df_subset[['FlowAmount', 'Location', 'Sector_group']]
        df_denom = df_denom.groupby(['Location', 'Sector_group'],
                                    as_index=False).agg({"FlowAmount": sum})
        df_denom = df_denom.rename(columns={"FlowAmount": "Denominator"})
        # merge the denominator column with fba_w_sector df
        ratio_df = df_subset.merge(df_denom, how='left')
        # calculate ratio
        ratio_df.loc[:, 'FlowAmountRatio'] = \
            ratio_df['FlowAmount'] / ratio_df['Denominator']
        ratio_df = ratio_df.drop(
            columns=['Denominator', 'Sector_group']).reset_index()
        sec_ratios.append(ratio_df)
    # concat list of dataframes (info on each page)
    df_w_ratios = pd.concat(sec_ratios, sort=True).reset_index(drop=True)

    return df_w_ratios


def sector_aggregation(df_load, group_cols):
    """
    Function that checks if a sector length exists, and if not,
    sums the less aggregated sector
    :param df_load: Either a flowbyactivity df with sectors or
       a flowbysector df
    :param group_cols: columns by which to aggregate
    :return: df, with aggregated sector values
    """
    # ensure None values are not strings
    df = replace_NoneType_with_empty_cells(df_load)

    # determine if activities are sector-like,
    # if aggregating a df with a 'SourceName'
    sector_like_activities = False
    if 'SourceName' in df_load.columns:
        s = pd.unique(df_load['SourceName'])[0]
        sector_like_activities = check_activities_sector_like(s)

    # if activities are source like, drop from df and group calls,
    # add back in as copies of sector columns columns to keep
    if sector_like_activities:
        group_cols = [e for e in group_cols if e not in
                      ('ActivityProducedBy', 'ActivityConsumedBy')]
        # subset df
        df_cols = [e for e in df.columns if e not in
                   ('ActivityProducedBy', 'ActivityConsumedBy')]
        df = df[df_cols]

    # find the longest length sector
    length = df[[fbs_activity_fields[0], fbs_activity_fields[1]]].apply(
        lambda x: x.str.len()).max().max()
    length = int(length)
    # for loop in reverse order longest length naics minus 1 to 2
    # appends missing naics levels to df
    for i in range(length, 2, -1):
        # df where either sector column is length or both columns are
        df1 = df[((df['SectorProducedBy'].apply(lambda x: len(x) == i)) |
                 (df['SectorConsumedBy'].apply(lambda x: len(x) == i)))
                 |
                 ((df['SectorProducedBy'].apply(lambda x: len(x) == i)) &
                  (df['SectorConsumedBy'].apply(lambda x: len(x) == i)))]

        # add new columns dropping last digit of sectors
        df1 = df1.assign(
            SPB=df1['SectorProducedBy'].apply(lambda x: x[0:i - 1]))
        df1 = df1.assign(
            SCB=df1['SectorConsumedBy'].apply(lambda x: x[0:i - 1]))

        # second dataframe where length is l - 1
        df2 = df[((df['SectorProducedBy'].apply(lambda x: len(x) == i-1)) |
                 (df['SectorConsumedBy'].apply(lambda x: len(x) == i-1)))
                 |
                 ((df['SectorProducedBy'].apply(lambda x: len(x) == i-1)) &
                  (df['SectorConsumedBy'].apply(lambda x: len(x) == i-1))
                  )].rename(columns={'SectorProducedBy': 'SPB',
                                     'SectorConsumedBy': 'SCB'})

        # merge the dfs
        merge_cols = [col for col in df2.columns if hasattr(df2[col], 'str')]
        # also drop activity and description cols
        merge_cols = [c for c in merge_cols
                      if c not in ['ActivityConsumedBy', 'ActivityProducedBy',
                                   'Description']]

        if len(df2) > 0:
            dfm = df1.merge(
                df2[merge_cols], how='outer',
                on=merge_cols, indicator=True).query(
                '_merge=="left_only"').drop('_merge', axis=1)
        else:
            dfm = df1.copy(deep=True)

        if len(dfm) > 0:
            # replace the SCB and SPB columns then aggregate and add to df
            dfm['SectorProducedBy'] = dfm['SPB']
            dfm['SectorConsumedBy'] = dfm['SCB']
            dfm = dfm.drop(columns=(['SPB', 'SCB']))
            # aggregate the new sector flow amounts
            agg_sectors = aggregator(dfm, group_cols)
            # append to df
            agg_sectors = replace_NoneType_with_empty_cells(agg_sectors)
            df = df.append(agg_sectors, sort=False).reset_index(drop=True)

    # manually modify non-NAICS codes that might exist in sector
    # domestic/household
    df = df.replace({'F0': 'F010',
                     'F01': 'F010'})
    # drop any duplicates created by modifying sector codes
    df = df.drop_duplicates()

    # if activities are source-like, set col values as
    # copies of the sector columns
    if sector_like_activities:
        df = df.assign(ActivityProducedBy=df['SectorProducedBy'])
        df = df.assign(ActivityConsumedBy=df['SectorConsumedBy'])
        # reindex columns
        df = df.reindex(df_load.columns, axis=1)

    # replace null values
    df = replace_strings_with_NoneType(df).reset_index(drop=True)

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
    sector_like_activities = False
    if 'SourceName' in df_load.columns:
        s = pd.unique(df_load['SourceName'])[0]
        sector_like_activities = check_activities_sector_like(s)

    # if activities are source like, drop from df,
    # add back in as copies of sector columns columns to keep
    if sector_like_activities:
        # subset df
        df_cols = [e for e in df.columns if e not in
                   ('ActivityProducedBy', 'ActivityConsumedBy')]
        df = df[df_cols]

    # load naics 2 to naics 6 crosswalk
    cw_load = load_crosswalk('sector_length')

    # for loop min length to 6 digits, where min length cannot be less than 2
    length = df[[fbs_activity_fields[0], fbs_activity_fields[1]]].apply(
        lambda x: x.str.len()).min().min()
    if length < 2:
        length = 2
    # appends missing naics levels to df
    for i in range(length, 6):
        sector_merge = 'NAICS_' + str(i)
        sector_add = 'NAICS_' + str(i+1)

        # subset the df by naics length
        cw = cw_load[[sector_merge, sector_add]]
        # only keep the rows where there is only one value
        # in sector_add for a value in sector_merge
        cw = cw.drop_duplicates(
            subset=[sector_merge], keep=False).reset_index(drop=True)
        sector_list = cw[sector_merge].values.tolist()

        # subset df to sectors with length = i and length = i + 1
        df_subset = df.loc[
            df[fbs_activity_fields[0]].apply(lambda x: i + 1 >= len(x) >= i) |
            df[fbs_activity_fields[1]].apply(lambda x: i + 1 >= len(x) >= i)]
        # create new columns that are length i
        df_subset = df_subset.assign(
            SectorProduced_tmp=df_subset[fbs_activity_fields[0]].apply(
                lambda x: x[0:i]))
        df_subset = df_subset.assign(
            SectorConsumed_tmp=df_subset[fbs_activity_fields[1]].apply(
                lambda x: x[0:i]))
        # subset the df to the rows where the tmp sector columns
        # are in naics list
        df_subset_1 = df_subset.loc[
            (df_subset['SectorProduced_tmp'].isin(sector_list)) &
            (df_subset['SectorConsumed_tmp'] == "")]
        df_subset_2 = df_subset.loc[
            (df_subset['SectorProduced_tmp'] == "") &
            (df_subset['SectorConsumed_tmp'].isin(sector_list))]
        df_subset_3 = df_subset.loc[
            (df_subset['SectorProduced_tmp'].isin(sector_list)) &
            (df_subset['SectorConsumed_tmp'].isin(sector_list))]
        # concat existing dfs
        df_subset = pd.concat([df_subset_1, df_subset_2, df_subset_3],
                              sort=False)
        # drop all rows with duplicate temp values, as a less aggregated
        # naics exists list of column headers, that if exist in df, should
        # be aggregated using the weighted avg fxn
        possible_column_headers = ('Flowable', 'FlowName', 'Unit', 'Context',
                                   'Compartment', 'Location', 'Year',
                                   'SectorProduced_tmp', 'SectorConsumed_tmp')
        # list of column headers that do exist in the df being subset
        cols_to_drop = [e for e in possible_column_headers if e
                        in df_subset.columns.values.tolist()]

        df_subset = df_subset.drop_duplicates(
            subset=cols_to_drop, keep=False).reset_index(drop=True)

        # merge the naics cw
        new_naics = pd.merge(df_subset, cw[[sector_merge, sector_add]],
                             how='left', left_on=['SectorProduced_tmp'],
                             right_on=[sector_merge])
        new_naics = new_naics.rename(columns={sector_add: "SPB"})
        new_naics = new_naics.drop(columns=[sector_merge])
        new_naics = pd.merge(new_naics, cw[[sector_merge, sector_add]],
                             how='left', left_on=['SectorConsumed_tmp'],
                             right_on=[sector_merge])
        new_naics = new_naics.rename(columns={sector_add: "SCB"})
        new_naics = new_naics.drop(columns=[sector_merge])
        # drop columns and rename new sector columns
        new_naics = new_naics.drop(
            columns=["SectorProducedBy", "SectorConsumedBy",
                     "SectorProduced_tmp", "SectorConsumed_tmp"])
        new_naics = new_naics.rename(
            columns={"SPB": "SectorProducedBy",
                     "SCB": "SectorConsumedBy"})
        # append new naics to df
        new_naics['SectorConsumedBy'] = \
            new_naics['SectorConsumedBy'].replace({np.nan: ""})
        new_naics['SectorProducedBy'] = \
            new_naics['SectorProducedBy'].replace({np.nan: ""})
        new_naics = replace_NoneType_with_empty_cells(new_naics)
        df = pd.concat([df, new_naics], sort=True, ignore_index=True)
    # replace blank strings with None
    df = replace_strings_with_NoneType(df)

    # if activities are source-like, set col values
    # as copies of the sector columns
    if sector_like_activities:
        df = df.assign(ActivityProducedBy=df['SectorProducedBy'])
        df = df.assign(ActivityConsumedBy=df['SectorConsumedBy'])
        # reindex columns
        df = df.reindex(df_load.columns, axis=1)

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


def collapse_fbs_sectors(fbs):
    """
    Collapses the Sector Produced/Consumed into a single column named "Sector"
    uses based on identified rules for flowtypes
    :param fbs: df, a standard FlowBySector (format)
    :return: df, FBS with single Sector column
    """
    # ensure correct datatypes and order
    fbs = clean_df(fbs, flow_by_sector_fields, fbs_fill_na_dict)

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
            unique_activities_sub = unique_activities_sub.append(
                df_missing_i[[fba_activity_fields[0], fba_activity_fields[1]]])
            df_existing = df_existing.append(df_missing_i)
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
        df_load, sector_column, groupcols):
    """
    Estimate data suppression, by equally allocating parent NAICS
    values to child NAICS
    :param df_load: df with sector columns
    :param sector_column: str, column to estimate suppressed data for
    :param groupcols: list, columns to group df by
    :return: df, with estimated suppressed data
    """
    df = sector_disaggregation(df_load)
    df = replace_NoneType_with_empty_cells(df)
    df = df[df[sector_column] != '']

    # determine if activities are sector-like,
    # if aggregating a df with a 'SourceName'
    sector_like_activities = False
    if 'SourceName' in df_load.columns:
        s = pd.unique(df_load['SourceName'])[0]
        sector_like_activities = check_activities_sector_like(s)

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
    cw_melt = cw_load.melt(
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

    # merge the new naics with the existing df, if data already
    # existed for a NAICS6, keep the original
    dfm = pd.merge(
        new_naics[groupcols], df, how='left', on=groupcols,
        indicator=True).query('_merge=="left_only"').drop('_merge', axis=1)
    dfm = replace_NoneType_with_empty_cells(dfm)
    dfm = dfm.fillna(0)
    df = pd.concat([df, dfm], sort=True, ignore_index=True)
    # add length column and subset the data
    # subtract out existing data at NAICS6 from total data
    # at a length where no suppressed data
    df = df.assign(secLength=df[sector_column].apply(lambda x: len(x)))

    # add column for each state of sector length where
    # there are no missing values
    df_sup = df_sup.assign(
        secLength=df_sup[sector_column].apply(lambda x: len(x)))
    df_sup2 = (df_sup.groupby(
        ['FlowName', 'Compartment', 'Location'])['secLength'].agg(
        lambda x: x.min()-1).reset_index(name='secLengthsup'))

    # merge the dfs and sub out the last sector lengths with
    # all data for each state drop states that don't have suppressed dat
    df1 = df.merge(df_sup2)

    df2 = df1[df1['secLength'] == 6].reset_index(drop=True)
    # determine sector to merge on
    df2.loc[:, 'mergeSec'] = df2.apply(
        lambda x: x[sector_column][:x['secLengthsup']], axis=1)

    sum_cols = [e for e in fba_default_grouping_fields if e not in
                ['ActivityConsumedBy', 'ActivityProducedBy']]
    sum_cols.append('mergeSec')
    df2 = df2.assign(
        FlowAlloc=df2.groupby(sum_cols)['FlowAmount'].transform('sum'))
    # rename columns for the merge and define merge cols
    df2 = df2.rename(columns={sector_column: 'NewNAICS',
                              'mergeSec': sector_column})
    # keep flows with 0 flow
    df3 = df2[df2['FlowAmount'] == 0].reset_index(drop=True)
    m_cols = groupcols + ['NewNAICS', 'FlowAlloc']
    # merge the two dfs
    dfe = df1.merge(df3[m_cols])
    # add count column used to divide the unallocated flows
    dfe = dfe.assign(
        secCount=dfe.groupby(groupcols)['NewNAICS'].transform('count'))
    dfe = dfe.assign(
        newFlow=(dfe['FlowAmount'] - dfe['FlowAlloc']) / dfe['secCount'])
    # reassign values and drop columns
    dfe = dfe.assign(FlowAmount=dfe['newFlow'])
    dfe[sector_column] = dfe['NewNAICS'].copy()
    dfe = dfe.drop(columns=['NewNAICS', 'FlowAlloc', 'secCount', 'newFlow'])

    # new df with estimated naics6
    dfn = pd.concat([df, dfe], ignore_index=True)
    dfn2 = dfn[dfn['FlowAmount'] != 0].reset_index(drop=True)
    dfn2 = dfn2.drop(columns=['secLength'])

    dff = sector_aggregation(dfn2, fba_wsec_default_grouping_fields)

    # if activities are source-like, set col values as copies
    # of the sector columns
    if sector_like_activities:
        dff = dff.assign(ActivityProducedBy=dff['SectorProducedBy'])
        dff = dff.assign(ActivityConsumedBy=dff['SectorConsumedBy'])
        # reindex columns
        dff = dff.reindex(df_load.columns, axis=1)

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


def dynamically_import_fxn(data_source_scripts_file, function_name):
    """
    Dynamically import a function and call on that function
    :param data_source_scripts_file: str, file name where function is found
    :param function_name: str, name of function to import and call on
    :return: a function
    """
    # if a file does not exist modify file name, dropping
    # extension after last underscore
    data_source_scripts_file = get_flowsa_base_name(datasourcescriptspath,
                                                    data_source_scripts_file,
                                                    'py')

    df = getattr(__import__(
        f"flowsa.data_source_scripts.{data_source_scripts_file}",
        fromlist=function_name), function_name)
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
        fba = clean_df(fba, flow_by_activity_fields, fba_fill_na_dict)
        fba = standardize_units(fba)

    return fba
