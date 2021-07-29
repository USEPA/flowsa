# flowbyfunctions.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Helper functions for flowbyactivity and flowbysector data
"""

import pandas as pd
import numpy as np
from flowsa.common import fbs_activity_fields, US_FIPS, get_state_FIPS, \
    get_county_FIPS, update_geoscale, log, load_source_catalog, \
    load_sector_length_crosswalk, flow_by_sector_fields, fbs_fill_na_dict, \
    fbs_collapsed_default_grouping_fields, flow_by_sector_collapsed_fields, \
    fbs_collapsed_fill_na_dict, fba_activity_fields, fba_default_grouping_fields, \
    fips_number_key, flow_by_activity_fields, fba_fill_na_dict, datasourcescriptspath, \
    find_true_file_path
from flowsa.dataclean import clean_df, replace_strings_with_NoneType,\
    replace_NoneType_with_empty_cells
from esupy.dqi import get_weighted_average


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
        log.error("No flows found in the flow dataset at the %s scale", geoscale)
    else:
        return df


def agg_by_geoscale(df, from_scale, to_scale, groupbycols):
    """
    Aggregate a df by geoscale
    :param df: flowbyactivity or flowbysector df
    :param from_scale: str, geoscale to aggregate from ('national', 'state', 'county')
    :param to_scale: str, geoscale to aggregate to ('national', 'state', 'county')
    :param groupbycolumns: flowbyactivity or flowbysector default groupby columns
    :return: df, at identified to_scale geographic level
    """

    # use from scale to filter by these values
    df = filter_by_geoscale(df, from_scale).reset_index(drop=True)

    df = update_geoscale(df, to_scale)

    fba_agg = aggregator(df, groupbycols)

    return fba_agg


def aggregator(df, groupbycols):
    """
    Aggregates flowbyactivity or flowbysector 'FlowAmount' column in df and generate
    weighted average values based on FlowAmount values for numeric columns

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

    # list of column headers, that if exist in df, should be aggregated using the weighted avg fxn
    possible_column_headers = ('Spread', 'Min', 'Max', 'DataReliability', 'TemporalCorrelation',
                               'GeographicalCorrelation', 'TechnologicalCorrelation',
                               'DataCollection')

    # list of column headers that do exist in the df being aggregated
    column_headers = [e for e in possible_column_headers if e in df.columns.values.tolist()]

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
    Determine ratios of the less aggregated sectors within a more aggregated sector
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
        df_subset = df_subset.assign(Sector_group=df_subset[sectorcolumn].apply(lambda x: x[0:i-1]))
        # subset df to create denominator
        df_denom = df_subset[['FlowAmount', 'Location', 'Sector_group']]
        df_denom = df_denom.groupby(['Location', 'Sector_group'],
                                    as_index=False)[["FlowAmount"]].agg("sum")
        df_denom = df_denom.rename(columns={"FlowAmount": "Denominator"})
        # merge the denominator column with fba_w_sector df
        ratio_df = df_subset.merge(df_denom, how='left')
        # calculate ratio
        ratio_df.loc[:, 'FlowAmountRatio'] = ratio_df['FlowAmount'] / ratio_df['Denominator']
        ratio_df = ratio_df.drop(columns=['Denominator', 'Sector_group']).reset_index()
        sec_ratios.append(ratio_df)
    # concat list of dataframes (info on each page)
    df_w_ratios = pd.concat(sec_ratios, sort=True).reset_index(drop=True)

    return df_w_ratios


def sector_aggregation(df_load, group_cols):
    """
    Function that checks if a sector length exists, and if not, sums the less aggregated sector
    :param df_load: Either a flowbyactivity df with sectors or a flowbysector df
    :param group_cols: columns by which to aggregate
    :return: df, with aggregated sector values
    """

    # determine if activities are sector-like, if aggregating a df with a 'SourceName'
    sector_like_activities = False
    if 'SourceName' in df_load.columns:
        # load source catalog
        cat = load_source_catalog()
        # for s in pd.unique(flowbyactivity_df['SourceName']):
        s = pd.unique(df_load['SourceName'])[0]
        # load catalog info for source
        src_info = cat[s]
        sector_like_activities = src_info['sector-like_activities']

    # ensure None values are not strings
    df = replace_NoneType_with_empty_cells(df_load)

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
    for i in range(length - 1, 1, -1):
        # subset df to sectors with length = i and length = i + 1
        df_subset = df.loc[df[fbs_activity_fields[0]].apply(lambda x: i + 1 >= len(x) >= i) |
                           df[fbs_activity_fields[1]].apply(lambda x: i + 1 >= len(x) >= i)]
        # create a list of i digit sectors in df subset
        sector_subset = df_subset[
            ['Location', fbs_activity_fields[0],
             fbs_activity_fields[1]]].drop_duplicates().reset_index(drop=True)
        df_sectors = sector_subset.copy()
        df_sectors.loc[:, 'SectorProducedBy'] = \
            df_sectors['SectorProducedBy'].apply(lambda x: x[0:i])
        df_sectors.loc[:, 'SectorConsumedBy'] = \
            df_sectors['SectorConsumedBy'].apply(lambda x: x[0:i])
        sector_list = df_sectors.drop_duplicates().values.tolist()
        # create a list of sectors that are exactly i digits long
        # where either sector column is i digits in length
        df_existing_1 = \
            sector_subset.loc[(sector_subset['SectorProducedBy'].apply(lambda x: len(x) == i)) |
                              (sector_subset['SectorConsumedBy'].apply(lambda x: len(x) == i))]
        # where both sector columns are i digits in length
        df_existing_2 = \
            sector_subset.loc[(sector_subset['SectorProducedBy'].apply(lambda x: len(x) == i)) &
                              (sector_subset['SectorConsumedBy'].apply(lambda x: len(x) == i))]
        # concat existing dfs
        df_existing = pd.concat([df_existing_1, df_existing_2], sort=False)
        existing_sectors = df_existing.drop_duplicates().dropna().values.tolist()
        # list of sectors of length i that are not in sector list
        missing_sectors = [e for e in sector_list if e not in existing_sectors]
        if len(missing_sectors) != 0:
            # new df of sectors that start with missing sectors.
            # drop last digit of the sector and sum flows
            # set conditions
            agg_sectors_list = []
            for q, r, s in missing_sectors:
                c1 = df_subset['Location'] == q
                c2 = df_subset[fbs_activity_fields[0]].apply(lambda x: x[0:i] == r)
                c3 = df_subset[fbs_activity_fields[1]].apply(lambda x: x[0:i] == s)
                # subset data
                agg_sectors_list.append(df_subset.loc[c1 & c2 & c3])
            agg_sectors = pd.concat(agg_sectors_list, sort=False)
            agg_sectors = agg_sectors.loc[
                (agg_sectors[fbs_activity_fields[0]].apply(lambda x: len(x) > i)) |
                (agg_sectors[fbs_activity_fields[1]].apply(lambda x: len(x) > i))]
            agg_sectors.loc[:, fbs_activity_fields[0]] = agg_sectors[fbs_activity_fields[0]].apply(
                lambda x: x[0:i])
            agg_sectors.loc[:, fbs_activity_fields[1]] = agg_sectors[fbs_activity_fields[1]].apply(
                lambda x: x[0:i])
            # aggregate the new sector flow amounts
            agg_sectors = aggregator(agg_sectors, group_cols)
            # append to df
            agg_sectors = replace_NoneType_with_empty_cells(agg_sectors)
            df = df.append(agg_sectors, sort=False).reset_index(drop=True)

    # manually modify non-NAICS codes that might exist in sector
    df.loc[:, 'SectorConsumedBy'] = np.where(df['SectorConsumedBy'].isin(['F0', 'F01']),
                                             'F010', df['SectorConsumedBy'])  # domestic/household
    df.loc[:, 'SectorProducedBy'] = np.where(df['SectorProducedBy'].isin(['F0', 'F01']),
                                             'F010', df['SectorProducedBy'])  # domestic/household
    # drop any duplicates created by modifying sector codes
    df = df.drop_duplicates()
    # if activities are source-like, set col values as copies of the sector columns
    if sector_like_activities:
        df = df.assign(ActivityProducedBy=df['SectorProducedBy'])
        df = df.assign(ActivityConsumedBy=df['SectorConsumedBy'])
        # reindex columns
        df = df.reindex(df_load.columns, axis=1)
    # replace null values
    df = replace_strings_with_NoneType(df)

    return df


def sector_disaggregation(df, group_cols):
    """
    function to disaggregate sectors if there is only one naics at a lower level
    works for lower than naics 4
    :param df_load: A FBS df, must have sector columns
    :param group_cols: columns by which to disaggregate sectors
    :return: A FBS df with values for the missing naics5 and naics6
    """

    # ensure None values are not strings
    df = replace_NoneType_with_empty_cells(df)

    # load naics 2 to naics 6 crosswalk
    cw_load = load_sector_length_crosswalk()

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
        # only keep the rows where there is only one value in sector_add for a value in sector_merge
        cw = cw.drop_duplicates(subset=[sector_merge], keep=False).reset_index(drop=True)
        sector_list = cw[sector_merge].values.tolist()

        # subset df to sectors with length = i and length = i + 1
        df_subset = df.loc[df[fbs_activity_fields[0]].apply(lambda x: i + 1 >= len(x) >= i) |
                           df[fbs_activity_fields[1]].apply(lambda x: i + 1 >= len(x) >= i)]
        # create new columns that are length i
        df_subset = df_subset.assign(SectorProduced_tmp=
                                     df_subset[fbs_activity_fields[0]].apply(lambda x: x[0:i]))
        df_subset = df_subset.assign(SectorConsumed_tmp=
                                     df_subset[fbs_activity_fields[1]].apply(lambda x: x[0:i]))
        # subset the df to the rows where the tmp sector columns are in naics list
        df_subset_1 = df_subset.loc[(df_subset['SectorProduced_tmp'].isin(sector_list)) &
                                    (df_subset['SectorConsumed_tmp'] == "")]
        df_subset_2 = df_subset.loc[(df_subset['SectorProduced_tmp'] == "") &
                                    (df_subset['SectorConsumed_tmp'].isin(sector_list))]
        df_subset_3 = df_subset.loc[(df_subset['SectorProduced_tmp'].isin(sector_list)) &
                                    (df_subset['SectorConsumed_tmp'].isin(sector_list))]
        # concat existing dfs
        df_subset = pd.concat([df_subset_1, df_subset_2, df_subset_3], sort=False)
        # drop all rows with duplicate temp values, as a less aggregated naics exists
        # list of column headers, that if exist in df, should be
        # aggregated using the weighted avg fxn
        possible_column_headers = ('Flowable', 'FlowName', 'Unit', 'Context',
                                   'Compartment', 'Location', 'Year',
                                   'SectorProduced_tmp', 'SectorConsumed_tmp')
        # list of column headers that do exist in the df being subset
        cols_to_drop = [e for e in possible_column_headers if e
                        in df_subset.columns.values.tolist()]

        df_subset = df_subset.drop_duplicates(subset=cols_to_drop,
                                              keep=False).reset_index(drop=True)

        # merge the naics cw
        new_naics = pd.merge(df_subset, cw[[sector_merge, sector_add]],
                             how='left', left_on=['SectorProduced_tmp'], right_on=[sector_merge])
        new_naics = new_naics.rename(columns={sector_add: "SPB"})
        new_naics = new_naics.drop(columns=[sector_merge])
        new_naics = pd.merge(new_naics, cw[[sector_merge, sector_add]],
                             how='left', left_on=['SectorConsumed_tmp'], right_on=[sector_merge])
        new_naics = new_naics.rename(columns={sector_add: "SCB"})
        new_naics = new_naics.drop(columns=[sector_merge])
        # drop columns and rename new sector columns
        new_naics = new_naics.drop(columns=["SectorProducedBy", "SectorConsumedBy",
                                            "SectorProduced_tmp", "SectorConsumed_tmp"])
        new_naics = new_naics.rename(columns={"SPB": "SectorProducedBy",
                                              "SCB": "SectorConsumedBy"})
        # append new naics to df
        new_naics['SectorConsumedBy'] = new_naics['SectorConsumedBy'].replace({np.nan: ""})
        new_naics['SectorProducedBy'] = new_naics['SectorProducedBy'].replace({np.nan: ""})
        new_naics = replace_NoneType_with_empty_cells(new_naics)
        df = pd.concat([df, new_naics], sort=True)
    # replace blank strings with None
    df = replace_strings_with_NoneType(df)

    return df


def assign_fips_location_system(df, year_of_data):
    """
    Add location system based on year of data. County level FIPS change over the years.
    :param df: df with FIPS location system
    :param year_of_data: str, year of data pulled
    :return: df, with 'LocationSystem' column values
    """

    if year_of_data >= '2015':
        df.loc[:, 'LocationSystem'] = 'FIPS_2015'
    elif '2013' <= year_of_data < '2015':
        df.loc[:, 'LocationSystem'] = 'FIPS_2013'
    elif '2010' <= year_of_data < '2013':
        df.loc[:, 'LocationSystem'] = 'FIPS_2010'
    elif year_of_data < '2010':
        log.warning(
            "Missing FIPS codes from crosswalk for %s. Assigning to FIPS_2010", year_of_data)
        df.loc[:, 'LocationSystem'] = 'FIPS_2010'

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
    fbs.loc[fbs["FlowType"] == 'TECHNOSPHERE_FLOW', 'Sector'] = fbs["SectorConsumedBy"]
    fbs.loc[fbs["FlowType"] == 'WASTE_FLOW', 'Sector'] = fbs["SectorProducedBy"]
    fbs.loc[(fbs["FlowType"] == 'WASTE_FLOW') & (fbs['SectorProducedBy'].isnull()),
            'Sector'] = fbs["SectorConsumedBy"]
    fbs.loc[(fbs["FlowType"] == 'ELEMENTARY_FLOW') & (fbs['SectorProducedBy'].isnull()),
            'Sector'] = fbs["SectorConsumedBy"]
    fbs.loc[(fbs["FlowType"] == 'ELEMENTARY_FLOW') & (fbs['SectorConsumedBy'].isnull()),
            'Sector'] = fbs["SectorProducedBy"]
    fbs.loc[(fbs["FlowType"] == 'ELEMENTARY_FLOW') &
            (fbs['SectorConsumedBy'].isin(['F010', 'F0100', 'F01000'])) &
            (fbs['SectorProducedBy'].isin(['22', '221', '2213', '22131', '221310'])),
            'Sector'] = fbs["SectorConsumedBy"]

    # drop sector consumed/produced by columns
    fbs_collapsed = fbs.drop(columns=['SectorProducedBy', 'SectorConsumedBy'])
    # aggregate
    fbs_collapsed = aggregator(fbs_collapsed, fbs_collapsed_default_grouping_fields)
    # sort dataframe
    fbs_collapsed = clean_df(fbs_collapsed, flow_by_sector_collapsed_fields,
                             fbs_collapsed_fill_na_dict)
    fbs_collapsed = fbs_collapsed.sort_values(['Sector', 'Flowable',
                                               'Context', 'Location']).reset_index(drop=True)

    return fbs_collapsed


def return_activity_from_scale(df, provided_from_scale):
    """
    Determine the 'from scale' used for aggregation/df subsetting for each activity combo in a df
    :param df: flowbyactivity df
    :param provided_from_scale: str, The scale to use specified in method yaml
    :return: df, FBA with column indicating the "from" geoscale to use for each row
    """

    # determine the unique combinations of activityproduced/consumedby
    unique_activities = unique_activity_names(df)
    # filter by geoscale
    fips = create_geoscale_list(df, provided_from_scale)
    df_sub = df[df['Location'].isin(fips)]
    # determine unique activities after subsetting by geoscale
    unique_activities_sub = unique_activity_names(df_sub)

    # return df of the difference between unique_activities and unique_activities2
    df_missing = dataframe_difference(unique_activities, unique_activities_sub, which='left_only')
    # return df of the similarities between unique_activities and unique_activities2
    df_existing = dataframe_difference(unique_activities, unique_activities_sub, which='both')
    df_existing = df_existing.drop(columns='_merge')
    df_existing['activity_from_scale'] = provided_from_scale

    # for loop through geoscales until find data for each activity combo
    if provided_from_scale == 'national':
        geoscales = ['state', 'county']
    elif provided_from_scale == 'state':
        geoscales = ['county']
    elif provided_from_scale == 'county':
        log.info('No data - skipping')

    if len(df_missing) > 0:
        for i in geoscales:
            # filter by geoscale
            fips_i = create_geoscale_list(df, i)
            df_i = df[df['Location'].isin(fips_i)]

            # determine unique activities after subsetting by geoscale
            unique_activities_i = unique_activity_names(df_i)

            # return df of the difference between unique_activities subset and
            # unique_activities for geoscale
            df_missing_i = dataframe_difference(unique_activities_sub,
                                                unique_activities_i, which='right_only')
            df_missing_i = df_missing_i.drop(columns='_merge')
            df_missing_i['activity_from_scale'] = i
            # return df of the similarities between unique_activities and unique_activities2
            df_existing_i = dataframe_difference(unique_activities_sub,
                                                 unique_activities_i, which='both')

            # append unique activities and df with defined activity_from_scale
            unique_activities_sub = \
                unique_activities_sub.append(df_missing_i[[fba_activity_fields[0],
                                                           fba_activity_fields[1]]])
            df_existing = df_existing.append(df_missing_i)
            df_missing = dataframe_difference(df_missing[[fba_activity_fields[0],
                                                          fba_activity_fields[1]]],
                                              df_existing_i[[fba_activity_fields[0],
                                                             fba_activity_fields[1]]],
                                              which=None)

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

    # method of subset dependent on LocationSystem
    if df['LocationSystem'].str.contains('FIPS').any():
        df = df[df['LocationSystem'].str.contains('FIPS')].reset_index(drop=True)
        # determine 'activity_from_scale' for use in df geoscale subset, by activity
        modified_from_scale = return_activity_from_scale(df, activity_from_scale)
        # add 'activity_from_scale' column to df
        df2 = pd.merge(df, modified_from_scale)

        # list of unique 'from' geoscales
        unique_geoscales =\
            modified_from_scale['activity_from_scale'].drop_duplicates().values.tolist()
        if len(unique_geoscales) > 1:
            log.info('Dataframe has a mix of geographic levels: %s', ', '.join(unique_geoscales))

        # to scale
        if fips_number_key[activity_from_scale] > fips_number_key[activity_to_scale]:
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
                df_sub = agg_by_geoscale(df3, i, to_scale, fba_default_grouping_fields)
            # else filter relevant rows
            else:
                log.info("Subsetting %s data", i)
                df_sub = filter_by_geoscale(df3, i)
            df_subset_list.append(df_sub)
        df_subset = pd.concat(df_subset_list, ignore_index=True)

        # only keep cols associated with FBA
        df_subset = clean_df(df_subset, flow_by_activity_fields,
                             fba_fill_na_dict, drop_description=False)

    # right now, the only other location system is for Statistics Canada data
    else:
        df_subset = df.copy()

    return df_subset


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


def estimate_suppressed_data(df, sector_column, naics_level, sourcename):
    """
    Estimate data suppression, by equally allocating parent NAICS values to child NAICS
    :param df: df with sector columns
    :param sector_column: str, column to estimate suppressed data for
    :param naics_level: numeric, indicate at what NAICS length to base
                        estimated suppresed data off (2 - 5)
    :param sourcename: str, sourcename
    :return: df, with estimated suppressed data
    """

    # exclude nonsectors
    df = replace_NoneType_with_empty_cells(df)

    # find the longest length sector
    max_length = max(df[sector_column].apply(lambda x: len(str(x))).unique())
    # loop through starting at naics_level, use most detailed level possible to save time
    for i in range(naics_level, max_length):
        # create df of i length
        df_x = df.loc[df[sector_column].apply(lambda x: len(x) == i)]
        # create df of i + 1 length
        df_y = df.loc[df[sector_column].apply(lambda x: len(x) == i + 1)]
        # create temp sector columns in df y, that are i digits in length
        df_y = df_y.assign(s_tmp=df_y[sector_column].apply(lambda x: x[0:i]))

        # create list of location and temp activity combos that contain a 0
        missing_sectors_df = df_y[df_y['FlowAmount'] == 0]
        missing_sectors_list = missing_sectors_df[['Location',
                                                   's_tmp']].drop_duplicates().values.tolist()
        # subset the y df
        if len(missing_sectors_list) != 0:
            # new df of sectors that start with missing sectors.
            # drop last digit of the sector and sum flows set conditions
            suppressed_list = []
            for q, r, in missing_sectors_list:
                c1 = df_y['Location'] == q
                c2 = df_y['s_tmp'] == r
                # subset data
                suppressed_list.append(df_y.loc[c1 & c2])
            suppressed_sectors = pd.concat(suppressed_list, sort=False, ignore_index=True)
            # add column of existing allocated data for length of i
            suppressed_sectors['alloc_flow'] =\
                suppressed_sectors.groupby(['Location', 's_tmp'])['FlowAmount'].transform('sum')
            # subset further so only keep rows of 0 value
            suppressed_sectors_sub = suppressed_sectors[suppressed_sectors['FlowAmount'] == 0]
            # add count
            suppressed_sectors_sub = \
                suppressed_sectors_sub.assign(sector_count=
                                              suppressed_sectors_sub.groupby(
                                                  ['Location', 's_tmp']
                                              )['s_tmp'].transform('count'))

            # merge suppressed sector subset with df x
            df_m = pd.merge(df_x,
                            suppressed_sectors_sub[['Class', 'Compartment', 'FlowType',
                                                    'FlowName', 'Location', 'LocationSystem',
                                                    'Unit', 'Year', sector_column, 's_tmp',
                                                    'alloc_flow', 'sector_count']],
                            left_on=['Class', 'Compartment', 'FlowType', 'FlowName',
                                     'Location', 'LocationSystem', 'Unit', 'Year', sector_column],
                            right_on=['Class', 'Compartment', 'FlowType', 'FlowName',
                                      'Location', 'LocationSystem', 'Unit', 'Year', 's_tmp'],
                            how='right')
            # calculate estimated flows by subtracting the flow
            # amount already allocated from total flow of
            # sector one level up and divide by number of sectors with suppresed data
            df_m.loc[:, 'FlowAmount'] = \
                (df_m['FlowAmount'] - df_m['alloc_flow']) / df_m['sector_count']
            # only keep the suppressed sector subset activity columns
            df_m = df_m.drop(columns=[sector_column + '_x', 's_tmp', 'alloc_flow', 'sector_count'])
            df_m = df_m.rename(columns={sector_column + '_y': sector_column})
            # reset activity columns
            if load_source_catalog()[sourcename]['sector-like_activities']:
                df_m = df_m.assign(ActivityProducedBy=df_m['SectorProducedBy'])
                df_m = df_m.assign(ActivityConsumedBy=df_m['SectorConsumedBy'])

            # drop the existing rows with suppressed data and append the new estimates from fba df
            modified_df =\
                pd.merge(df, df_m[['FlowName', 'Location', sector_column]],
                         indicator=True,
                         how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
            df = pd.concat([modified_df, df_m], ignore_index=True)
    df_w_estimated_data = replace_strings_with_NoneType(df)

    return df_w_estimated_data


def collapse_activity_fields(df):
    """
    The 'activityconsumedby' and 'activityproducedby' columns from the
    allocation dataset do not always align with
    the dataframe being allocated. Generalize the allocation activity column.
    :param df: df, FBA used to allocate another FBA
    :return: df, single Activity column
    """

    df = replace_strings_with_NoneType(df)

    activity_consumed_list = df['ActivityConsumedBy'].drop_duplicates().values.tolist()
    activity_produced_list = df['ActivityProducedBy'].drop_duplicates().values.tolist()

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


def allocate_by_sector(df_w_sectors, allocation_method, group_cols, **kwargs):
    """
    Create an allocation ratio for df
    :param df_w_sectors: df with column of sectors
    :param allocation_method: currently written for 'proportional' and 'proportional-flagged'
    :param group_cols: columns on which to base aggregation and disaggregation
    :return: df with FlowAmountRatio for each sector
    """

    # first determine if there is a special case with how the allocation ratios are created
    if allocation_method == 'proportional-flagged':
        # if the allocation method is flagged, subset sectors that are
        # flagged/notflagged, where nonflagged sectors have flowamountratio=1
        if kwargs != {}:
            if 'flowSubsetMapped' in kwargs:
                fsm = kwargs['flowSubsetMapped']
                flagged = fsm[fsm['disaggregate_flag'] == 1]
                #todo: change col to be generalized, not SEctorConsumedBy specific
                flagged_names = flagged['SectorConsumedBy'].tolist()

                nonflagged = fsm[fsm['disaggregate_flag'] == 0]
                nonflagged_names = nonflagged['SectorConsumedBy'].tolist()

                # subset the original df so rows of data that run through the
                # proportional allocation process are
                # sectors included in the flagged list
                df_w_sectors_nonflagged = df_w_sectors.loc[
                    (df_w_sectors[fbs_activity_fields[0]].isin(nonflagged_names)) |
                    (df_w_sectors[fbs_activity_fields[1]].isin(nonflagged_names))
                    ].reset_index(drop=True)
                df_w_sectors_nonflagged = df_w_sectors_nonflagged.assign(FlowAmountRatio=1)

                df_w_sectors = \
                    df_w_sectors.loc[(df_w_sectors[fbs_activity_fields[0]].isin(flagged_names)) |
                                     (df_w_sectors[fbs_activity_fields[1]].isin(flagged_names))
                                    ].reset_index(drop=True)
            else:
                log.error('The proportional-flagged allocation method requires a'
                          'column "disaggregate_flag" in the flow_subset_mapped df')

    # run sector aggregation fxn to determine total flowamount for each level of sector
    if len(df_w_sectors) == 0:
        allocation_df = df_w_sectors_nonflagged.copy()
    else:
        df1 = sector_aggregation(df_w_sectors, group_cols)
        # run sector disaggregation to capture one-to-one naics4/5/6 relationships
        df2 = sector_disaggregation(df1, group_cols)

        # if statements for method of allocation
        # either 'proportional' or 'proportional-flagged'
        allocation_df = []
        if allocation_method == 'proportional' or allocation_method == 'proportional-flagged':
            allocation_df = proportional_allocation_by_location(df2)
        else:
            log.error('Must create function for specified method of allocation')

        if allocation_method == 'proportional-flagged':
            # drop rows where values are not in flagged names
            allocation_df =\
                allocation_df.loc[(allocation_df[fbs_activity_fields[0]].isin(flagged_names)) |
                                  (allocation_df[fbs_activity_fields[1]].isin(flagged_names)
                                   )].reset_index(drop=True)
            # concat the flagged and nonflagged dfs
            allocation_df = \
                pd.concat([allocation_df, df_w_sectors_nonflagged],
                          ignore_index=True).sort_values(['SectorProducedBy', 'SectorConsumedBy'])

    return allocation_df


def proportional_allocation_by_location(df):
    """
    Creates a proportional allocation based on all the most
    aggregated sectors within a location
    Ensure that sectors are at 2 digit level - can run sector_aggregation()
    prior to using this function
    :param df: df, includes sector columns
    :param sectorcolumn: str, sector column by which to base allocation
    :return: df, with 'FlowAmountRatio' column
    """

    # tmp drop NoneType
    df = replace_NoneType_with_empty_cells(df)

    # find the shortest length sector

    denom_df = df.loc[(df['SectorProducedBy'].apply(lambda x: len(x) == 2)) |
                      (df['SectorConsumedBy'].apply(lambda x: len(x) == 2))]
    denom_df = denom_df.assign(Denominator=denom_df['FlowAmount'].groupby(
        denom_df['Location']).transform('sum'))
    denom_df_2 = denom_df[['Location', 'LocationSystem', 'Year', 'Denominator']].drop_duplicates()
    # merge the denominator column with fba_w_sector df
    allocation_df = df.merge(denom_df_2, how='left')
    # calculate ratio
    allocation_df.loc[:, 'FlowAmountRatio'] = allocation_df['FlowAmount'] / allocation_df[
        'Denominator']
    allocation_df = allocation_df.drop(columns=['Denominator']).reset_index()

    # add nonetypes
    allocation_df = replace_strings_with_NoneType(allocation_df)

    return allocation_df


def proportional_allocation_by_location_and_activity(df, sectorcolumn):
    """
    Creates a proportional allocation within each aggregated sector within a location
    :param df: df with sector columns
    :param sectorcolumn: str, sector column for which to create allocation ratios
    :return: df, with 'FlowAmountRatio' and 'HelperFlow' columns
    """

    # tmp replace NoneTypes with empty cells
    df = replace_NoneType_with_empty_cells(df)

    # denominator summed from highest level of sector grouped by location
    short_length = min(df[sectorcolumn].apply(lambda x: len(str(x))).unique())
    # want to create denominator based on short_length
    denom_df = df.loc[df[sectorcolumn].apply(lambda x: len(x) ==
                                                       short_length)].reset_index(drop=True)
    grouping_cols = [e for e in ['FlowName', 'Location', 'Activity',
                                 'ActivityConsumedBy', 'ActivityProducedBy']
                     if e in denom_df.columns.values.tolist()]
    denom_df.loc[:, 'Denominator'] = denom_df.groupby(grouping_cols)['HelperFlow'].transform('sum')

    # list of column headers, that if exist in df, should be aggregated using the weighted avg fxn
    possible_column_headers = ('Location', 'LocationSystem', 'Year',
                               'Activity', 'ActivityConsumedBy', 'ActivityProducedBy')
    # list of column headers that do exist in the df being aggregated
    column_headers = [e for e in possible_column_headers if e in denom_df.columns.values.tolist()]
    merge_headers = column_headers.copy()
    column_headers.append('Denominator')
    # create subset of denominator values based on Locations and Activities
    denom_df_2 = denom_df[column_headers].drop_duplicates().reset_index(drop=True)
    # merge the denominator column with fba_w_sector df
    allocation_df = df.merge(denom_df_2,
                             how='left',
                             left_on=merge_headers,
                             right_on=merge_headers)
    # calculate ratio
    allocation_df.loc[:, 'FlowAmountRatio'] = \
        allocation_df['HelperFlow'] / allocation_df['Denominator']
    allocation_df = allocation_df.drop(columns=['Denominator']).reset_index(drop=True)

    # fill empty cols with NoneType
    allocation_df = replace_strings_with_NoneType(allocation_df)
    # fill na values with 0
    allocation_df['HelperFlow'] = allocation_df['HelperFlow'].fillna(0)

    return allocation_df


def dynamically_import_fxn(data_source_scripts_file, function_name):
    """
    Dynamically import a function and call on that function
    :param data_source_scripts_file: str, file name where function is found
    :param function_name: str, name of function to import and call on
    :return: a function
    """

    # if a file does not exist modify file name, dropping ext after last underscore
    data_source_scripts_file = find_true_file_path(datasourcescriptspath, data_source_scripts_file, 'py')

    df = getattr(__import__(f"{'flowsa.data_source_scripts.'}{data_source_scripts_file}",
                            fromlist=function_name), function_name)
    return df
