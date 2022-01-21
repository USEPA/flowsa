# validation.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to check data is loaded and transformed correctly
"""

import pandas as pd
import numpy as np
from flowsa.flowbyfunctions import aggregator, create_geoscale_list,\
    subset_df_by_geoscale, sector_aggregation
from flowsa.dataclean import replace_strings_with_NoneType, \
    replace_NoneType_with_empty_cells
from flowsa.common import US_FIPS, sector_level_key, \
    check_activities_sector_like, load_crosswalk, \
    SECTOR_SOURCE_NAME, fba_activity_fields, \
    fba_default_grouping_fields, fips_number_key
from flowsa.settings import log, vLog, vLogDetailed


def check_flow_by_fields(flowby_df, flowbyfields):
    """
    Add in missing fields to have a complete and ordered
    :param flowby_df: Either flowbyactivity or flowbysector df
    :param flowbyfields: Either flow_by_activity_fields or
        flow_by_sector_fields
    :return: printout, column datatypes
    """
    for k, v in flowbyfields.items():
        try:
            vLog.debug("fba activity %s data type is %s",
                       k, str(flowby_df[k].values.dtype))
            vLog.debug("standard %s data type is %s", k, str(v[0]['dtype']))
        except:
            vLog.debug("Failed to find field %s in fba", k)


def check_if_activities_match_sectors(fba):
    """
    Checks if activities in flowbyactivity that appear to be like sectors
    are actually sectors
    :param fba: a flow by activity dataset
    :return: A list of activities not marching the default sector list or
        text indicating 100% match
    """
    # Get list of activities in a flowbyactivity file
    activities = []
    for f in fba_activity_fields:
        activities.extend(fba[f])

    # Get list of module default sectors
    flowsa_sector_list = list(load_crosswalk('sector_timeseries')[
                                  SECTOR_SOURCE_NAME])
    activities_missing_sectors = set(activities) - set(flowsa_sector_list)

    if len(activities_missing_sectors) > 0:
        vLog.debug("%s activities not matching sectors in default %s list",
                   str(len(activities_missing_sectors)), SECTOR_SOURCE_NAME)
        return activities_missing_sectors


def check_if_data_exists_at_geoscale(df, geoscale, activitynames='All'):
    """
    Check if an activity or a sector exists at the specified geoscale
    :param df: flowbyactivity dataframe
    :param activitynames: Either an activity name (ex. 'Domestic')
        or a sector (ex. '1124')
    :param geoscale: national, state, or county
    :return: str, 'yes' or 'no'
    """

    # if any activity name is specified, check if activity data
    # exists at the specified geoscale
    activity_list = []
    if activitynames != 'All':
        if isinstance(activitynames, str):
            activity_list.append(activitynames)
        else:
            activity_list = activitynames
        # check for specified activity name
        df = df[(df[fba_activity_fields[0]].isin(activity_list)) |
                (df[fba_activity_fields[1]].isin(activity_list)
                 )].reset_index(drop=True)
    else:
        activity_list.append('activities')

    # filter by geoscale depends on Location System
    fips = create_geoscale_list(df, geoscale)

    df = df[df['Location'].isin(fips)]

    if len(df) == 0:
        vLog.info(
            "No flows found for %s at the %s scale",
            ', '.join(activity_list), geoscale)
        exists = "No"
    else:
        vLog.info("Flows found for %s at the %s scale",
                  ', '.join(activity_list), geoscale)
        exists = "Yes"

    return exists


def check_if_data_exists_at_less_aggregated_geoscale(
        df, geoscale, activityname):
    """
    In the event data does not exist at specified geoscale,
    check if data exists at less aggregated level
    :param df: Either flowbyactivity or flowbysector dataframe
    :param geoscale: national, state, or county
    :param activityname: str, activity col names to check
    :return: str, geoscale to use
    """

    if geoscale == 'national':
        df = df[(df[fba_activity_fields[0]] == activityname) | (
                df[fba_activity_fields[1]] == activityname)]
        fips = create_geoscale_list(df, 'state')
        df = df[df['Location'].isin(fips)]
        if len(df) == 0:
            vLog.info("No flows found for %s at the state scale", activityname)
            fips = create_geoscale_list(df, 'county')
            df = df[df['Location'].isin(fips)]
            if len(df) == 0:
                vLog.info("No flows found for %s at the county scale",
                          activityname)
            else:
                vLog.info("Flow-By-Activity data exists for %s at "
                          "the county level", activityname)
                new_geoscale_to_use = 'county'
                return new_geoscale_to_use
        else:
            vLog.info("Flow-By-Activity data exists for %s at "
                      "the state level", activityname)
            new_geoscale_to_use = 'state'
            return new_geoscale_to_use
    if geoscale == 'state':
        df = df[(df[fba_activity_fields[0]] == activityname) | (
                df[fba_activity_fields[1]] == activityname)]
        fips = create_geoscale_list(df, 'county')
        df = df[df['Location'].isin(fips)]
        if len(df) == 0:
            vLog.info("No flows found for %s at the "
                      "county scale", activityname)
        else:
            vLog.info("Flow-By-Activity data exists for %s "
                      "at the county level", activityname)
            new_geoscale_to_use = 'county'
            return new_geoscale_to_use


def check_if_location_systems_match(df1, df2):
    """
    Check if two dataframes share the same location system
    :param df1: fba or fbs df
    :param df2: fba or fbs df
    :return: warning if Location Systems do not match between dfs
    """

    if df1["LocationSystem"].all() != df2["LocationSystem"].all():
        vLog.warning("LocationSystems do not match, "
                     "might lose county level data")


def check_allocation_ratios(flow_alloc_df_load, activity_set, config, attr):
    """
    Check for issues with the flow allocation ratios
    :param flow_alloc_df_load: df, includes 'FlowAmountRatio' column
    :param activity_set: str, activity set
    :param config: dictionary, method yaml
    :param attr: dictionary, activity set info
    :return: print out information regarding allocation ratios,
             save csv of results to local directory
    """
    # if in the attr dictionary, merge columns are identified,
    # the merge columns need to be accounted for in the grouping/checking of
    # allocation ratios
    if 'allocation_merge_columns' in attr:
        subset_cols = ['FBA_Activity', 'Location', 'SectorLength',
                       'FlowAmountRatio'] + attr['allocation_merge_columns']
        groupcols = ['FBA_Activity', 'Location',
                     'SectorLength'] + attr['allocation_merge_columns']
    else:
        subset_cols = ['FBA_Activity', 'Location',
                       'SectorLength', 'FlowAmountRatio']
        groupcols = ['FBA_Activity', 'Location', 'SectorLength']

    # create column of sector lengths
    flow_alloc_df =\
        flow_alloc_df_load.assign(
            SectorLength=flow_alloc_df_load['Sector'].str.len())
    # subset df
    flow_alloc_df2 = flow_alloc_df[subset_cols]
    # sum the flow amount ratios by location and sector length
    flow_alloc_df3 = \
        flow_alloc_df2.groupby(
            groupcols, dropna=False, as_index=False).agg(
            {"FlowAmountRatio": sum})
    # keep only rows of specified sector length
    flow_alloc_df4 = flow_alloc_df3[
        flow_alloc_df3['SectorLength'] == sector_level_key[
            config['target_sector_level']]].reset_index(drop=True)
    # keep data where the flowamountratio is greater than or
    # less than 1 by 0.005
    tolerance = 0.01
    flow_alloc_df5 = flow_alloc_df4[
        (flow_alloc_df4['FlowAmountRatio'] < 1 - tolerance) |
        (flow_alloc_df4['FlowAmountRatio'] > 1 + tolerance)]

    if len(flow_alloc_df5) > 0:
        vLog.info('There are %s instances at a sector length of %s '
                  'where the allocation ratio for a location is greater '
                  'than or less than 1 by at least %s. See Validation Log',
                  len(flow_alloc_df5), config["target_sector_level"],
                  str(tolerance))

    # add to validation log
    log.info('Save the summary table of flow allocation ratios for each '
             'sector length for %s in validation log', activity_set)
    # if df not empty, print, if empty, print string
    if flow_alloc_df5.empty:
        vLogDetailed.info('Flow allocation ratios for %s '
                          'all round to 1', activity_set)

    else:
        vLogDetailed.info('Flow allocation ratios for %s: '
                          '\n {}'.format(flow_alloc_df5.to_string()),
                          activity_set)


def calculate_flowamount_diff_between_dfs(dfa_load, dfb_load):
    """
    Calculate the differences in FlowAmounts between two dfs
    :param dfa_load: df, initial df
    :param dfb_load: df, modified df
    :return: df, comparing changes in flowamounts between 2 dfs
    """

    # subset the dataframes, only keeping data for easy
    # comparison of flowamounts
    drop_cols = ['Year', 'MeasureofSpread', 'Spread', 'DistributionType',
                 'Min', 'Max', 'DataReliability', 'DataCollection']
    # drop cols and rename, ignore error if a df does not
    # contain a column to drop
    dfa = dfa_load.drop(drop_cols, axis=1, errors='ignore'
                        ).rename(columns={'FlowAmount': 'FlowAmount_Original'})
    dfb = dfb_load.drop(drop_cols, axis=1, errors='ignore'
                        ).rename(columns={'FlowAmount': 'FlowAmount_Modified'})
    # create df dict for modified dfs created in for loop
    df_list = []
    for d in ['a', 'b']:
        df_name = f'df{d}'
        # assign new column of geoscale by which to aggregate
        vars()[df_name+'2'] = vars()[df_name].assign(
            geoscale=np.where(vars()[df_name]['Location'].
                              apply(lambda x: x.endswith('000')),
                              'state', 'county'))
        vars()[df_name+'2'] = vars()[df_name+'2'].assign(
            geoscale=np.where(vars()[df_name+'2']['Location'] == '00000',
                              'national', vars()[df_name+'2']['geoscale']))
        # ensure all nan/nones filled/match
        vars()[df_name + '2'] = \
            replace_strings_with_NoneType(vars()[df_name+'2'])
        df_list.append(vars()[df_name+'2'])
    # merge the two dataframes
    df = df_list[0].merge(df_list[1], how='outer')

    # determine if any new data is negative
    dfn = df[df['FlowAmount_Modified'] < 0].reset_index(drop=True)
    if len(dfn) > 0:
        vLog.info('There are negative FlowAmounts in new dataframe, '
                  'see Validation Log')
        vLogDetailed.info('Negative FlowAmounts in new dataframe: '
                          '\n {}'.format(dfn.to_string()))

    # Because code will sometimes change terminology, aggregate
    # data by context and flowable to compare df differences
    # subset df
    dfs = df[['Flowable', 'Context', 'ActivityProducedBy',
              'ActivityConsumedBy', 'FlowAmount_Original',
              'FlowAmount_Modified', 'Unit', 'geoscale']]
    agg_cols = ['Flowable', 'Context', 'ActivityProducedBy',
                'ActivityConsumedBy', 'Unit', 'geoscale']
    dfagg = dfs.groupby(
        agg_cols, dropna=False, as_index=False).agg(
        {'FlowAmount_Original': sum, 'FlowAmount_Modified': sum})
    # column calculating difference
    dfagg['FlowAmount_Difference'] = \
        dfagg['FlowAmount_Modified'] - dfagg['FlowAmount_Original']
    dfagg['Percent_Difference'] = (dfagg['FlowAmount_Difference'] /
                                   dfagg['FlowAmount_Original']) * 100
    # drop rows where difference = 0
    dfagg2 = dfagg[dfagg['FlowAmount_Difference'] != 0].reset_index(drop=True)
    if len(dfagg2) == 0:
        vLogDetailed.info('No FlowAmount differences')
    else:
        # subset df and aggregate, also print out the total
        # aggregate diff at the geoscale
        dfagg3 = replace_strings_with_NoneType(dfagg).drop(
            columns=['ActivityProducedBy', 'ActivityConsumedBy',
                     'FlowAmount_Difference', 'Percent_Difference'])
        dfagg4 = dfagg3.groupby(
            ['Flowable', 'Context', 'Unit', 'geoscale'],
            dropna=False, as_index=False).agg(
            {'FlowAmount_Original': sum, 'FlowAmount_Modified': sum})
        # column calculating difference
        dfagg4['FlowAmount_Difference'] = \
            dfagg4['FlowAmount_Modified'] - dfagg4['FlowAmount_Original']
        dfagg4['Percent_Difference'] = (dfagg4['FlowAmount_Difference'] /
                                        dfagg4['FlowAmount_Original']) * 100
        # drop rows where difference = 0
        dfagg5 = dfagg4[
            dfagg4['FlowAmount_Difference'] != 0].reset_index(drop=True)
        vLogDetailed.info('Total FlowAmount differences between dataframes: '
                          '\n {}'.format(dfagg5.to_string(), index=False))

        # save detail output in log file
        vLogDetailed.info('Total FlowAmount differences by Activity Columns: '
                          '\n {}'.format(dfagg2.to_string(), index=False))


def compare_activity_to_sector_flowamounts(fba_load, fbs_load,
                                           activity_set, source_name, config):
    """
    Function to compare the loaded flowbyactivity with the final flowbysector
    by activityname (if exists) to target sector level
    output, checking for data loss
    :param fba_load: df, FBA loaded and mapped using FEDEFL
    :param fbs_load: df, final FBS df
    :param activity_set: str, activity set
    :param source_name: str, source name
    :param config: dictionary, method yaml
    :return: printout data differences between loaded FBA and FBS output,
             save results as csv in local directory
    """
    if check_activities_sector_like(source_name):
        vLog.debug('Not comparing loaded FlowByActivity to FlowBySector '
                   'ratios for a dataset with sector-like activities because '
                   'if there are modifications to flowamounts for a sector, '
                   'then the ratios will be different')
    else:
        # subset fba df
        fba = fba_load[['Class', 'MetaSources', 'Flowable', 'Unit', 'FlowType',
                        'ActivityProducedBy', 'ActivityConsumedBy', 'Context',
                        'Location', 'LocationSystem', 'Year',
                        'FlowAmount']].drop_duplicates().reset_index(drop=True)
        fba.loc[:, 'Location'] = US_FIPS
        group_cols = ['ActivityProducedBy', 'ActivityConsumedBy', 'Flowable',
                      'Unit', 'FlowType', 'Context',
                      'Location', 'LocationSystem', 'Year']
        fba_agg = aggregator(fba, group_cols)
        fba_agg.rename(columns={'FlowAmount': 'FBA_amount'}, inplace=True)

        # subset fbs df

        fbs = fbs_load[['Class', 'SectorSourceName', 'Flowable', 'Unit',
                        'FlowType', 'SectorProducedBy', 'SectorConsumedBy',
                        'ActivityProducedBy', 'ActivityConsumedBy',
                        'Context', 'Location', 'LocationSystem', 'Year',
                        'FlowAmount']].drop_duplicates().reset_index(drop=True)

        fbs = replace_NoneType_with_empty_cells(fbs)

        fbs['ProducedLength'] = fbs['SectorProducedBy'].str.len()
        fbs['ConsumedLength'] = fbs['SectorConsumedBy'].str.len()
        fbs['SectorLength'] = fbs[['ProducedLength',
                                   'ConsumedLength']].max(axis=1)
        fbs.loc[:, 'Location'] = US_FIPS
        group_cols = ['ActivityProducedBy', 'ActivityConsumedBy', 'Flowable',
                      'Unit', 'FlowType', 'Context', 'Location',
                      'LocationSystem', 'Year', 'SectorLength']
        fbs_agg = aggregator(fbs, group_cols)
        fbs_agg.rename(columns={'FlowAmount': 'FBS_amount'}, inplace=True)

        # merge compare 1 and compare 2
        df_merge = fba_agg.merge(
            fbs_agg, left_on=['ActivityProducedBy', 'ActivityConsumedBy',
                              'Flowable', 'Unit', 'FlowType', 'Context',
                              'Location', 'LocationSystem', 'Year'],
            right_on=['ActivityProducedBy', 'ActivityConsumedBy',
                      'Flowable', 'Unit', 'FlowType', 'Context',
                      'Location', 'LocationSystem', 'Year'],
            how='left')
        df_merge['Ratio'] = df_merge['FBS_amount'] / df_merge['FBA_amount']

        # reorder
        df_merge = df_merge[['ActivityProducedBy', 'ActivityConsumedBy',
                             'Flowable', 'Unit', 'FlowType', 'Context',
                             'Location', 'LocationSystem', 'Year',
                             'SectorLength', 'FBA_amount', 'FBS_amount',
                             'Ratio']]

        # keep onlyrows of specified sector length
        comparison = df_merge[
            df_merge['SectorLength'] == sector_level_key[
                config['target_sector_level']]].reset_index(drop=True)

        tolerance = 0.01
        comparison2 = comparison[(comparison['Ratio'] < 1 - tolerance) |
                                 (comparison['Ratio'] > 1 + tolerance)]

        if len(comparison2) > 0:
            vLog.info('There are %s combinations of flowable/context/sector '
                      'length where the flowbyactivity to flowbysector ratio '
                      'is less than or greater than 1 by %s',
                      len(comparison2), str(tolerance))

        # include df subset in the validation log
        # only print rows where flowamount ratio is less t
        # han 1 (round flowamountratio)
        df_v = comparison2[comparison2['Ratio'].apply(
            lambda x: round(x, 3) < 1)].reset_index(drop=True)

        # save to validation log
        log.info('Save the comparison of FlowByActivity load '
                 'to FlowBySector ratios for %s in validation log',
                 activity_set)
        # if df not empty, print, if empty, print string
        if df_v.empty:
            vLogDetailed.info('Ratios for %s all round to 1', activity_set)
        else:
            vLogDetailed.info('Comparison of FlowByActivity load to '
                              'FlowBySector ratios for %s: '
                              '\n {}'.format(df_v.to_string()), activity_set)


def compare_fba_geo_subset_and_fbs_output_totals(
        fba_load, fbs_load, activity_set, source_name, source_attr,
        activity_attr, method):
    """
    Function to compare the loaded flowbyactivity total after
    subsetting by activity and geography with the final flowbysector output
    total. Not a direct comparison of the loaded FBA because FBAs are
    modified before being subset by activity for the target sector level
    :param fba_load: df, FBA loaded, before being mapped
    :param fbs_load: df, final FBS df at target sector level
    :param activity_set: str, activity set
    :param source_name: str, source name
    :param source_attr: dictionary, attribute data from method yaml
        for source data
    :param activity_attr: dictionary, attribute data from method yaml
        for activity set
    :param method: dictionary, FBS method yaml
    :return: printout data differences between loaded FBA and FBS output
        totals by location, save results as csv in local directory
    """

    vLog.info('Comparing Flow-By-Activity subset by activity and geography to '
              'the subset Flow-By-Sector FlowAmount total.')

    # determine from scale
    if fips_number_key[source_attr['geoscale_to_use']] < \
            fips_number_key[activity_attr['allocation_from_scale']]:
        from_scale = source_attr['geoscale_to_use']
    else:
        from_scale = activity_attr['allocation_from_scale']

    # extract relevant geoscale data or aggregate existing data
    fba = subset_df_by_geoscale(fba_load, from_scale,
                                method['target_geoscale'])
    if check_activities_sector_like(source_name):
        # if activities are sector-like, run sector aggregation and then
        # subset df to only keep NAICS2
        fba = fba[['Class', 'FlowAmount', 'Unit', 'Context',
                   'ActivityProducedBy', 'ActivityConsumedBy', 'Location',
                   'LocationSystem']]
        # rename the activity cols to sector cols for purposes of aggregation
        fba = fba.rename(columns={'ActivityProducedBy': 'SectorProducedBy',
                                  'ActivityConsumedBy': 'SectorConsumedBy'})
        group_cols_agg = ['Class', 'Context', 'Unit', 'Location',
                          'LocationSystem', 'SectorProducedBy',
                          'SectorConsumedBy']
        fba = sector_aggregation(fba, group_cols_agg)
        # subset fba to only include NAICS2
        fba = replace_NoneType_with_empty_cells(fba)
        fba = fba[fba['SectorConsumedBy'].apply(lambda x: len(x) == 2) |
                  fba['SectorProducedBy'].apply(lambda x: len(x) == 2)]
    # subset/agg dfs
    col_subset = ['Class', 'FlowAmount', 'Unit', 'Context',
                  'Location', 'LocationSystem']
    group_cols = ['Class', 'Unit', 'Context', 'Location', 'LocationSystem']
    # check units
    compare_df_units(fba, fbs_load)
    # fba
    fba = fba[col_subset]
    fba_agg = aggregator(fba, group_cols).reset_index(drop=True)
    fba_agg.rename(columns={'FlowAmount': 'FBA_amount',
                            'Unit': 'FBA_unit'}, inplace=True)

    # fbs
    fbs = fbs_load[col_subset]
    fbs_agg = aggregator(fbs, group_cols)
    fbs_agg.rename(columns={'FlowAmount': 'FBS_amount',
                            'Unit': 'FBS_unit'}, inplace=True)

    try:
        # merge FBA and FBS totals
        df_merge = fba_agg.merge(fbs_agg, how='left')
        df_merge['FlowAmount_difference'] = \
            df_merge['FBA_amount'] - df_merge['FBS_amount']
        df_merge['Percent_difference'] = \
            (df_merge['FlowAmount_difference']/df_merge['FBA_amount']) * 100

        # reorder
        df_merge = df_merge[['Class', 'Context', 'Location', 'LocationSystem',
                             'FBA_amount', 'FBA_unit', 'FBS_amount',
                             'FBS_unit', 'FlowAmount_difference',
                             'Percent_difference']]
        df_merge = replace_NoneType_with_empty_cells(df_merge)

        # list of contexts and locations
        context_list = df_merge[['Context', 'Location']].values.tolist()

        # loop through the contexts and print results of comparison
        vLog.info('Comparing FBA %s %s subset to FBS results. '
                  'Details in Validation Log', activity_set,
                  source_attr['geoscale_to_use'])
        for i, j in context_list:
            df_merge_subset = \
                df_merge[(df_merge['Context'] == i) &
                         (df_merge['Location'] == j)].reset_index(drop=True)
            diff_per = df_merge_subset['Percent_difference'][0]
            if np.isnan(diff_per):
                vLog.info('FlowBySector FlowAmount for %s %s %s '
                          'does not exist in the FBS',
                          source_name, activity_set, i)
                continue
            # make reporting more manageable
            if abs(diff_per) > 0.01:
                diff_per = round(diff_per, 2)
            else:
                diff_per = round(diff_per, 6)

            # diff_units = df_merge_subset['FBS_unit'][0]
            if diff_per > 0:
                vLog.info('FlowBySector FlowAmount for %s %s %s at %s is %s%% '
                          'less than the FlowByActivity FlowAmount',
                          source_name, activity_set, i, j, str(abs(diff_per)))
            elif diff_per < 0:
                vLog.info('FlowBySector FlowAmount for %s %s %s at %s is %s%% '
                          'more than the FlowByActivity FlowAmount',
                          source_name, activity_set, i, j, str(abs(diff_per)))
            elif diff_per == 0:
                vLogDetailed.info('FlowBySector FlowAmount for '
                                  '%s %s %s at %s is equal to the '
                                  'FlowByActivity FlowAmount',
                                  source_name, activity_set, i, j)

        # subset the df to include in the validation log
        # only print rows where the percent difference does not round to 0
        df_v = df_merge[df_merge['Percent_difference'].apply(
            lambda x: round(x, 3) != 0)].reset_index(drop=True)

        # log output
        log.info('Save the comparison of FlowByActivity load to FlowBySector '
                 'total FlowAmounts for %s in validation log file',
                 activity_set)
        # if df not empty, print, if empty, print string
        if df_v.empty:
            vLogDetailed.info('Percent difference for %s all round to 0',
                              activity_set)
        else:
            vLogDetailed.info('Comparison of FBA load to FBS total '
                              'FlowAmounts for %s: '
                              '\n {}'.format(df_v.to_string()), activity_set)
    except:
        vLog.info('Error occurred when comparing total FlowAmounts '
                  'for FlowByActivity and FlowBySector')


def check_summation_at_sector_lengths(df):
    """
    Check summed 'FlowAmount' values at each sector length
    :param df: df, requires Sector column
    :return: df, includes summed 'FlowAmount' values at each sector length
    """

    # columns to keep
    df_cols = [e for e in df.columns if e not in
               ('MeasureofSpread', 'Spread', 'DistributionType', 'Min',
                'Max', 'DataReliability', 'DataCollection', 'FlowType',
                'Compartment', 'Description', 'Activity')]
    # subset df
    df2 = df[df_cols]

    # rename columns and clean up df
    df2 = df2[~df2['Sector'].isnull()]

    df2 = df2.assign(SectorLength=len(df2['Sector']))

    # sum flowamounts by sector length
    denom_df = df2.copy()
    denom_df.loc[:, 'Denominator'] = denom_df.groupby(
        ['Location', 'SectorLength'])['FlowAmount'].transform('sum')

    summed_df = denom_df.drop(
        columns=['Sector', 'FlowAmount']).drop_duplicates().reset_index(
        drop=True)

    # max value
    maxv = max(summed_df['Denominator'].apply(lambda x: x))

    # percent of total accounted for
    summed_df = summed_df.assign(percentOfTot=summed_df['Denominator']/maxv)

    summed_df = summed_df.sort_values(['SectorLength']).reset_index(drop=True)

    return summed_df


def check_for_nonetypes_in_sector_col(df):
    """
    Check for NoneType in columns where datatype = string
    :param df: df with columns where datatype = object
    :return: warning message if there are NoneTypes
    """
    # if datatypes are strings, return warning message
    if df['Sector'].isnull().any():
        vLog.warning("There are NoneType values in the 'Sector' column")
    return df


def check_for_negative_flowamounts(df):
    """
    Check for negative FlowAmounts in a dataframe 'FlowAmount' column
    :param df: df, requires 'FlowAmount' column
    :return: df, unchanged
    """
    # return a warning if there are negative flowamount values
    if (df['FlowAmount'].values < 0).any():
        vLog.warning('There are negative FlowAmounts')

    return df


def check_if_sectors_are_naics(df_load, crosswalk_list, column_headers):
    """
    Check if activity-like sectors are in fact sectors.
    Also works for the Sector column
    :param df_load: df with activity or sector columns
    :param crosswalk_list: list, sectors found in crosswalk
    :param column_headers: list, headers to check for sectors
    :return: list, values that are not sectors
    """

    # create a df of non-sectors to export
    non_sectors_df = []
    # create a df of just the non-sectors column
    non_sectors_list = []
    # loop through the df headers and determine if value
    # is not in crosswalk list
    for c in column_headers:
        # create df where sectors do not exist in master crosswalk
        non_sectors = df_load[~df_load[c].isin(crosswalk_list)]
        # drop rows where c is empty
        non_sectors = non_sectors[non_sectors[c] != '']
        # subset to just the sector column
        if len(non_sectors) != 0:
            sectors = non_sectors[[c]].rename(columns={c: 'NonSectors'})
            non_sectors_df.append(non_sectors)
            non_sectors_list.append(sectors)

    if len(non_sectors_df) != 0:
        # concat the df and the df of sectors
        ns_list = pd.concat(non_sectors_list, sort=False, ignore_index=True)
        # print the NonSectors
        non_sectors = ns_list['NonSectors'].drop_duplicates().tolist()
        vLog.debug('There are sectors that are not NAICS 2012 Codes')
        vLog.debug(non_sectors)
    else:
        vLog.debug('All sectors are NAICS 2012 Codes')

    return non_sectors


def melt_naics_crosswalk():
    """
    Create a melt version of the naics 07 to 17 crosswalk to map
    naics to naics 2012
    :return: df, naics crosswalk melted
    """
    # load the mastercroswalk and subset by sectorsourcename,
    # save values to list
    cw_load = load_crosswalk('sector_timeseries')

    # create melt table of possible 2007 and 2017 naics that can
    # be mapped to 2012
    cw_melt = cw_load.melt(
        id_vars='NAICS_2012_Code', var_name='NAICS_year', value_name='NAICS')
    # drop the naics year because not relevant for replacement purposes
    cw_replacement = cw_melt.dropna(how='any')
    cw_replacement = cw_replacement[
        ['NAICS_2012_Code', 'NAICS']].drop_duplicates()
    # drop rows where contents are equal
    cw_replacement = cw_replacement[
        cw_replacement['NAICS_2012_Code'] != cw_replacement['NAICS']]
    # drop rows where length > 6
    cw_replacement = cw_replacement[cw_replacement['NAICS_2012_Code'].apply(
        lambda x: len(x) < 7)].reset_index(drop=True)
    # order by naics 2012
    cw_replacement = cw_replacement.sort_values(
        ['NAICS', 'NAICS_2012_Code']).reset_index(drop=True)

    # create allocation ratios by determining number of
    # NAICS 2012 to other naics when not a 1:1 ratio
    cw_replacement_2 = cw_replacement.assign(
        naics_count=cw_replacement.groupby(
            ['NAICS'])['NAICS_2012_Code'].transform('count'))
    cw_replacement_2 = cw_replacement_2.assign(
        allocation_ratio=1/cw_replacement_2['naics_count'])

    return cw_replacement_2


def replace_naics_w_naics_from_another_year(df_load, sectorsourcename):
    """
    Replace any non sectors with sectors.
    :param df_load: df with sector columns or sector-like activities
    :param sectorsourcename: str, sector source name (ex. NAICS_2012_Code)
    :return: df, with non-sectors replaced with sectors
    """
    # from flowsa.flowbyfunctions import aggregator

    # drop NoneType
    df = replace_NoneType_with_empty_cells(df_load).reset_index(drop=True)

    # load the mastercroswalk and subset by sectorsourcename,
    # save values to list
    cw_load = load_crosswalk('sector_timeseries')
    cw = cw_load[sectorsourcename].drop_duplicates().tolist()

    # load melted crosswalk
    cw_melt = melt_naics_crosswalk()
    # drop the count column
    cw_melt = cw_melt.drop(columns='naics_count')

    # determine which headers are in the df
    if 'SectorConsumedBy' in df:
        column_headers = ['SectorProducedBy', 'SectorConsumedBy']
    else:
        column_headers = ['ActivityProducedBy', 'ActivityConsumedBy']

    # check if there are any sectors that are not in the naics 2012 crosswalk
    non_naics = check_if_sectors_are_naics(df, cw, column_headers)

    # loop through the df headers and determine if value is
    # not in crosswalk list
    if len(non_naics) != 0:
        vLog.debug('Checking if sectors represent a different '
                   'NAICS year, if so, replace with %s', sectorsourcename)
        for c in column_headers:
            # merge df with the melted sector crosswalk
            df = df.merge(cw_melt, left_on=c, right_on='NAICS', how='left')
            # if there is a value in the sectorsourcename column,
            # use that value to replace sector in column c if value in
            # column c is in the non_naics list
            df[c] = np.where(
                (df[c] == df['NAICS']) & (df[c].isin(non_naics)),
                df[sectorsourcename], df[c])
            # multiply the FlowAmount col by allocation_ratio
            df.loc[df[c] == df[sectorsourcename],
                   'FlowAmount'] = df['FlowAmount'] * df['allocation_ratio']
            # drop columns
            df = df.drop(
                columns=[sectorsourcename, 'NAICS', 'allocation_ratio'])
        vLog.debug('Replaced NAICS with %s', sectorsourcename)

        # check if there are any sectors that are not in
        # the naics 2012 crosswalk
        vLog.debug('Check again for non NAICS 2012 Codes')
        nonsectors = check_if_sectors_are_naics(df, cw, column_headers)
        if len(nonsectors) != 0:
            vLog.debug('Dropping non-NAICS from dataframe')
            for c in column_headers:
                # drop rows where column value is in the nonnaics list
                df = df[~df[c].isin(nonsectors)]
        # aggregate data
        possible_column_headers = \
            ('FlowAmount', 'Spread', 'Min', 'Max', 'DataReliability',
             'TemporalCorrelation', 'GeographicalCorrelation',
             'TechnologicalCorrelation', 'DataCollection', 'Description')
        # list of column headers to group aggregation by
        groupby_cols = [e for e in df.columns.values.tolist()
                        if e not in possible_column_headers]
        df = aggregator(df, groupby_cols)

    # drop rows where both SectorConsumedBy and SectorProducedBy NoneType
    if 'SectorConsumedBy' in df:
        df_drop = df[(df['SectorConsumedBy'].isnull()) &
                     (df['SectorProducedBy'].isnull())]
        if len(df_drop) != 0:
            activities_dropped = pd.unique(
                df_drop[['ActivityConsumedBy',
                         'ActivityProducedBy']].values.ravel('K'))
            activities_dropped = list(filter(
                lambda x: x is not None, activities_dropped))
            vLog.debug('Dropping rows where the Activity columns contain %s',
                       ', '.join(activities_dropped))
        df = df[~((df['SectorConsumedBy'].isnull()) &
                  (df['SectorProducedBy'].isnull()))].reset_index(drop=True)
    else:
        df = df[~((df['ActivityConsumedBy'].isnull()) &
                  (df['ActivityProducedBy'].isnull()))].reset_index(drop=True)

    df = replace_strings_with_NoneType(df)

    return df


def compare_FBS_results(fbs1_load, fbs2_load, ignore_metasources=False):
    """
    Compare a parquet on Data Commons to a parquet stored locally
    :param fbs1_load: df, fbs format
    :param fbs2_load: df, fbs format
    :param ignore_metasources: bool, True to compare fbs without
    matching metasources
    :return: df, comparison of the two dfs
    """
    import flowsa

    # load first file (must be saved locally)
    df1 = flowsa.getFlowBySector(fbs1_load).rename(
        columns={'FlowAmount': 'FlowAmount_fbs1'})
    df1 = replace_strings_with_NoneType(df1)
    # load second file (must be saved locally)
    df2 = flowsa.getFlowBySector(fbs2_load).rename(
        columns={'FlowAmount': 'FlowAmount_fbs2'})
    df2 = replace_strings_with_NoneType(df2)
    # compare df
    merge_cols = ['Flowable', 'Class', 'SectorProducedBy', 'SectorConsumedBy',
                  'SectorSourceName', 'Context', 'Location', 'LocationSystem',
                  'Unit', 'FlowType', 'Year', 'MetaSources']
    if ignore_metasources:
        merge_cols.remove('MetaSources')
    # check units
    compare_df_units(df1, df2)
    df_m = pd.merge(df1[merge_cols + ['FlowAmount_fbs1']],
                    df2[merge_cols + ['FlowAmount_fbs2']],
                    how='outer')
    df_m = df_m.assign(
        FlowAmount_diff=df_m['FlowAmount_fbs2'] - df_m['FlowAmount_fbs1'])
    df_m = df_m.assign(
        Percent_Diff=(df_m['FlowAmount_diff']/df_m['FlowAmount_fbs1']) * 100)
    df_m = df_m[df_m['FlowAmount_diff'].apply(
        lambda x: round(abs(x), 2) != 0)].reset_index(drop=True)
    # if no differences, print, if differences, provide df subset
    if len(df_m) == 0:
        vLog.debug('No differences between dataframes')
    else:
        vLog.debug('Differences exist between dataframes')
        df_m = df_m.sort_values(['Location', 'SectorProducedBy',
                                 'SectorConsumedBy', 'Flowable',
                                 'Context', ]).reset_index(drop=True)

    return df_m


def compare_geographic_totals(
        df_subset, df_load, sourcename, attr, activity_set, activity_names):
    """
    Check for any data loss between the geoscale used and published
    national data
    :param df_subset: df, after subset by geography
    :param df_load: df, loaded data, including published national data
    :param sourcename: str, source name
    :param attr: dictionary, attributes
    :param activity_set: str, activity set
    :param activity_names: list of names in the activity set by which
           to subset national level data
    :return: df, comparing published national level data to df subset
    """

    # subset df_load to national level
    nat = df_load[df_load['Location'] == US_FIPS].reset_index(
        drop=True).rename(columns={'FlowAmount': 'FlowAmount_nat'})
    # if df len is not 0, continue with comparison
    if len(nat) != 0:
        # subset national level data by activity set names
        nat = nat[(nat[fba_activity_fields[0]].isin(activity_names)) |
                  (nat[fba_activity_fields[1]].isin(activity_names)
                   )].reset_index(drop=True)
        nat = replace_strings_with_NoneType(nat)
        # drop the geoscale in df_subset and sum
        sub = df_subset.assign(Location=US_FIPS)
        # depending on the datasource, might need to rename some
        # strings for national comparison
        sub = rename_column_values_for_comparison(sub, sourcename)
        sub2 = aggregator(sub, fba_default_grouping_fields).rename(
            columns={'FlowAmount': 'FlowAmount_sub'})

        # compare df
        merge_cols = ['Class', 'SourceName', 'FlowName', 'Unit',
                      'FlowType', 'ActivityProducedBy', 'ActivityConsumedBy',
                      'Compartment', 'Location', 'LocationSystem', 'Year']
        # comapare units
        compare_df_units(nat, sub2)
        df_m = pd.merge(nat[merge_cols + ['FlowAmount_nat']],
                        sub2[merge_cols + ['FlowAmount_sub']],
                        how='outer')
        df_m = df_m.assign(
            FlowAmount_diff=df_m['FlowAmount_nat'] - df_m['FlowAmount_sub'])
        df_m = df_m.assign(Percent_Diff=(abs(df_m['FlowAmount_diff'] /
                                             df_m['FlowAmount_nat']) * 100))
        df_m = df_m[df_m['FlowAmount_diff'] != 0].reset_index(drop=True)
        # subset the merged df to what to include in the validation df
        # include data where percent difference is > 1 or where value is nan
        df_m_sub = df_m[(df_m['Percent_Diff'] > 1) |
                        (df_m['Percent_Diff'].isna())].reset_index(drop=True)

        if len(df_m_sub) == 0:
            vLog.info('No data loss greater than 1%% between national '
                      'level data and %s subset',
                      attr['allocation_from_scale'])
        else:
            vLog.info('There are data differences between published national'
                      ' values and %s subset, saving to validation log',
                      attr['allocation_from_scale'])

            vLogDetailed.info(
                'Comparison of National FlowAmounts to aggregated data '
                'subset for %s: \n {}'.format(
                    df_m_sub.to_string()), activity_set)


def rename_column_values_for_comparison(df, sourcename):
    """
    To compare some datasets at different geographic scales,
    must rename FlowName and Compartments to those available at national level
    :param df: df with FlowName and Compartment columns
    :param sourcename: string, datasource name
    :return:
    """

    # at the national level, only have information for 'FlowName' = 'total'
    # and 'Compartment' = 'total'. At state/county level, have information
    # for fresh/saline and ground/surface water. Therefore, to compare
    # subset data to national level, rename to match national values.
    if sourcename == 'USGS_NWIS_WU':
        df['FlowName'] = np.where(
            df['ActivityConsumedBy'] != 'Livestock', 'total', df['FlowName'])
        df['Compartment'] = df['Compartment'].str.replace(
            'ground', 'total', regex=True)
        df['Compartment'] = df['Compartment'].str.replace(
            'surface', 'total', regex=True)

    return df


def compare_df_units(df1_load, df2_load):
    """
    Determine what units are in each df prior to merge
    :param df1_load:
    :param df2_load:
    :return:
    """
    df1 = df1_load['Unit'].drop_duplicates().tolist()
    df2 = df2_load['Unit'].drop_duplicates().tolist()

    # identify differnces between unit lists
    list_comp = list(set(df1) ^ set(df2))
    # if list is not empty, print warning that units are different
    if list_comp:
        log.info('Merging df with %s and df with %s units', df1, df2)
