# validation.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to check data is loaded and transformed correctly
"""

import pandas as pd
import numpy as np
import flowsa
from flowsa.flowby import FlowBySector
from flowsa.flowbyfunctions import aggregator, create_geoscale_list,\
    subset_df_by_geoscale, sector_aggregation, collapse_fbs_sectors,\
    subset_df_by_sector_lengths
from flowsa.flowsa_log import log, vlog
from flowsa.dataclean import replace_strings_with_NoneType, \
    replace_NoneType_with_empty_cells
from flowsa.common import sector_level_key, \
    fba_activity_fields, check_activities_sector_like
from flowsa.location import US_FIPS, fips_number_key
from flowsa.schema import dq_fields


def check_if_data_exists_at_geoscale(df_load, geoscale):
    """
    Check if an activity or a sector exists at the specified geoscale
    :param df_load: df with activity columns
    :param geoscale: national, state, or county
    """

    df_load = replace_NoneType_with_empty_cells(df_load)

    # filter by geoscale depends on Location System
    fips_list = create_geoscale_list(df_load, geoscale)
    fips = pd.DataFrame(fips_list, columns=['FIPS'])

    activities = df_load[['ActivityProducedBy', 'ActivityConsumedBy']]\
        .drop_duplicates().reset_index(drop=True)
    # add tmp column and merge
    fips['tmp'] = 1
    activities['tmp'] = 1
    activities = activities.merge(fips, on='tmp').drop(columns='tmp')

    # merge activities with df and determine which FIPS are missing for each
    # activity
    df = df_load[df_load['Location'].isin(fips_list)]
    # if activities are defined, subset df
    # df = df[df['']]

    dfm = df.merge(activities,
                   left_on=['ActivityProducedBy', 'ActivityConsumedBy',
                            'Location'],
                   right_on=['ActivityProducedBy', 'ActivityConsumedBy',
                             'FIPS'],
                   how='outer')
    # subset into df where values for state and where states do not have data
    df1 = dfm[~dfm['FlowAmount'].isna()]
    df2 = dfm[dfm['FlowAmount'].isna()]
    df2 = df2[['ActivityProducedBy', 'ActivityConsumedBy',
               'FIPS']].reset_index(drop=True)

    # define source name and year
    sn = df_load['SourceName'][0]
    y = df_load['Year'][0]

    if len(df1) == 0:
        vlog.info(f'No flows found for activities in {sn} {y} at the '
                  f'{geoscale} scale')
    if len(df2) > 0:
        # if len(df2) > 1:
        df2 = df2.groupby(
            ['ActivityProducedBy', 'ActivityConsumedBy'], dropna=False).agg(
            lambda col: ','.join(col)).reset_index()
        vlog.info("There are %s, activity combos that do not have "
                  "data in %s %s: \n {}".format(df2.to_string()),
                  geoscale, sn, y)


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
    for df in df_list:
        replace_NoneType_with_empty_cells(df)
    df = df_list[0].merge(df_list[1], how='outer')

    # determine if any new data is negative
    dfn = df[df['FlowAmount_Modified'] < 0].reset_index(drop=True)
    if len(dfn) > 0:
        vlog.info('There are negative FlowAmounts in new dataframe, '
                  'see Validation Log')
        vlog.info('Negative FlowAmounts in new dataframe: '
                  '\n {}'.format(dfn.to_string()))

    # Because code will sometimes change terminology, aggregate
    # data by context and flowable to compare df differences
    # subset df
    flowcols = ['FlowName', 'Compartment']
    if 'Flowable' in df.columns:
        flowcols = ['Flowable', 'Context']
    dfs_cols = flowcols + ['ActivityProducedBy', 'ActivityConsumedBy',
              'FlowAmount_Original', 'FlowAmount_Modified', 'Unit',
              'geoscale']
    dfs = df[dfs_cols]
    agg_cols = flowcols + ['ActivityProducedBy', 'ActivityConsumedBy',
                           'Unit', 'geoscale']
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
        vlog.info('No FlowAmount differences')
    else:
        # subset df and aggregate, also print out the total
        # aggregate diff at the geoscale
        dfagg3 = replace_strings_with_NoneType(dfagg).drop(
            columns=['ActivityProducedBy', 'ActivityConsumedBy',
                     'FlowAmount_Difference', 'Percent_Difference'])
        dfagg4 = dfagg3.groupby(flowcols + ['Unit', 'geoscale'],
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
        vlog.info('Total FlowAmount differences between dataframes: '
                  '\n {}'.format(dfagg5.to_string(), index=False))

        # save detail output in log file
        vlog.info('Total FlowAmount differences by Activity Columns: '
                  '\n {}'.format(dfagg2.to_string(), index=False))


def compare_activity_to_sector_flowamounts(fba_load, fbs_load,
                                           activity_set, config, v, attr, **_):
    """
    Function to compare the loaded flowbyactivity with the final flowbysector
    by activityname (if exists) to target sector level
    output, checking for data loss
    :param fba_load: df, FBA loaded and mapped using FEDEFL
    :param fbs_load: df, final FBS df
    :param activity_set: str, activity set
    :param config: dictionary, method yaml
    :return: printout data differences between loaded FBA and FBS output,
             save results as csv in local directory
    """
    data_format = v.get('data_format')
    if (data_format == 'FBA') & (check_activities_sector_like(fba_load)):
        vlog.debug('Not comparing loaded FlowByActivity to FlowBySector '
                   'ratios for a dataset with sector-like activities because '
                   'if there are modifications to flowamounts for a sector, '
                   'then the ratios will be different')
    else:
        # subset fba df
        col_subset = ['Class', 'MetaSources', 'Flowable', 'Unit', 'FlowType',
                      'ActivityProducedBy', 'ActivityConsumedBy',
                      'SectorProducedBy', 'SectorConsumedBy', 'Context',
                      'Location', 'LocationSystem', 'Year', 'FlowAmount']
        fba = fba_load[fba_load.columns.intersection(
            col_subset)].reset_index(drop=True)
        # todo: modify so location subset is based on target geoscale
        fba.loc[:, 'Location'] = US_FIPS
        group_cols = [e for e in fba.columns if e in
                      ['ActivityProducedBy', 'ActivityConsumedBy',
                       'SectorProducedBy', 'SectorConsumedBy', 'Flowable',
                       'Unit', 'FlowType', 'Context', 'Location',
                       'LocationSystem', 'Year']]
        fba_agg = aggregator(fba, group_cols)
        fba_agg.rename(columns={'FlowAmount': 'FBA_amount'}, inplace=True)

        # subset fbs df
        col_subset_2 = ['Class', 'SectorSourceName', 'Flowable', 'Unit',
                        'FlowType', 'SectorProducedBy',
                        'ActivityProducedBy', 'ActivityConsumedBy',
                        'Context', 'Location', 'LocationSystem', 'Year',
                        'FlowAmount']
        if attr.get('allocation_merge_columns') is not None:
            col_subset_2 = col_subset_2 + attr.get('allocation_merge_columns')
        fbs = fbs_load[fbs_load.columns.intersection(
            col_subset_2)].reset_index(drop=True)

        fbs = replace_NoneType_with_empty_cells(fbs)

        # determine which
        for i in ['Produced', 'Consumed']:
            try:
                fbs[f'{i}Length'] = fbs[f'Sector{i}By'].str.len()
            except KeyError:
                pass
        max_list = [e for e in fbs.columns if e in ['ProducedLength',
                                                    'ConsumedLength']]
        fbs['SectorLength'] = fbs[max_list].max(axis=1)

        fbs.loc[:, 'Location'] = US_FIPS
        group_cols = ['Flowable', 'Unit', 'FlowType', 'Context', 'Location',
                      'LocationSystem', 'Year', 'SectorLength']
        if v.get('data_format') == 'FBA':
            group_cols = ['ActivityProducedBy', 'ActivityConsumedBy'] + \
                         group_cols
        else:
            group_cols = attr.get('allocation_merge_columns') + group_cols
        fbs_agg = aggregator(fbs, group_cols)
        fbs_agg.rename(columns={'FlowAmount': 'FBS_amount'}, inplace=True)

        # merge compare 1 and compare 2
        if v.get('data_format') == 'FBA':
            merge_cols = ['ActivityProducedBy', 'ActivityConsumedBy',
                          'Flowable', 'Unit', 'FlowType', 'Context',
                          'Location', 'LocationSystem', 'Year']
        else:
            merge_cols = attr.get('allocation_merge_columns') + \
                         ['Flowable', 'Unit', 'FlowType', 'Context',
                          'Location', 'LocationSystem', 'Year']
        df_merge = fba_agg.merge(fbs_agg, left_on=merge_cols,
                                 right_on=merge_cols, how='left')
        df_merge['Ratio'] = df_merge['FBS_amount'] / df_merge['FBA_amount']

        # reorder
        order_cols = ['SectorProducedBy', 'SectorConsumedBy',
                      'ActivityProducedBy', 'ActivityConsumedBy',
                      'Flowable', 'Unit', 'FlowType', 'Context', 'Location',
                      'LocationSystem', 'Year', 'SectorLength',
                      'FBA_amount', 'FBS_amount', 'Ratio']
        df_merge = df_merge[df_merge.columns.intersection(
            order_cols)].reset_index(drop=True)

        # keep only rows of specified sector length
        # todo: update to also keep sub target list
        comparison = df_merge[
            df_merge['SectorLength'] == sector_level_key[
                config['target_sector_level']]].reset_index(drop=True)

        tolerance = 0.01
        comparison2 = comparison[(comparison['Ratio'] < 1 - tolerance) |
                                 (comparison['Ratio'] > 1 + tolerance)]

        if len(comparison2) > 0:
            vlog.info('There are %s combinations of flowable/context/sector '
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
            vlog.info('Ratios for %s all round to 1', activity_set)
        else:
            vlog.info('Comparison of FlowByActivity load to '
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

    vlog.info('Comparing Flow-By-Activity subset by activity and geography to '
              'the subset Flow-By-Sector FlowAmount total.')

    # determine from scale
    if fips_number_key[source_attr['geoscale_to_use']] < \
            fips_number_key[activity_attr['allocation_from_scale']]:
        from_scale = source_attr['geoscale_to_use']
    else:
        from_scale = activity_attr['allocation_from_scale']

    if source_attr['data_format'] == 'FBA':
        # extract relevant geoscale data or aggregate existing data
        fba = subset_df_by_geoscale(fba_load, from_scale,
                                    method['target_geoscale'])
        if check_activities_sector_like(fba_load):
            # if activities are sector-like, run sector aggregation and then
            # subset df to only keep NAICS2
            fba = fba[['Class', 'SourceName', 'FlowAmount', 'Unit', 'Context',
                       'ActivityProducedBy', 'ActivityConsumedBy', 'Location',
                       'LocationSystem']]
            # rename the activity cols to sector cols for purposes of aggregation
            fba = fba.rename(columns={'ActivityProducedBy': 'SectorProducedBy',
                                      'ActivityConsumedBy': 'SectorConsumedBy'})
            fba = sector_aggregation(fba)
            # subset fba to only include NAICS2
            fba = replace_NoneType_with_empty_cells(fba)
            fba = subset_df_by_sector_lengths(fba, [2])
    else:
        fba = sector_aggregation(fba_load)
        fba = subset_df_by_sector_lengths(fba, [2])
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
        df_merge['FBS_amount'] = df_merge['FBS_amount'].fillna(0)
        df_merge['FlowAmount_difference'] = \
            df_merge['FBA_amount'] - df_merge['FBS_amount']
        df_merge['Percent_difference'] = \
            (df_merge['FlowAmount_difference']/df_merge['FBA_amount']) * 100
        # cases where flow amount diff is 0 but because fba amount is 0,
        # percent diff is null. Fill those cases with 0s
        df_merge['Percent_difference'] = np.where(
            (df_merge['FlowAmount_difference'] == 0) &
            (df_merge['FBA_amount'] == 0), 0, df_merge['Percent_difference'])
        # reorder
        df_merge = df_merge[['Class', 'Context', 'Location', 'LocationSystem',
                             'FBA_amount', 'FBA_unit', 'FBS_amount',
                             'FBS_unit', 'FlowAmount_difference',
                             'Percent_difference']]
        df_merge = replace_NoneType_with_empty_cells(df_merge)

        # list of contexts and locations
        context_list = df_merge[['Class', 'Context',
                                 'Location']].values.tolist()

        # loop through the contexts and print results of comparison
        vlog.info(f'Comparing FBA {activity_set} {source_attr["geoscale_to_use"]} '
                  'subset to FBS results. Details in Validation Log')
        for i, j, k in context_list:
            df_merge_subset = \
                df_merge[(df_merge['Class'] == i) &
                         (df_merge['Context'] == j) &
                         (df_merge['Location'] == k)].reset_index(drop=True)
            diff_per = df_merge_subset['Percent_difference'][0]
            if np.isnan(diff_per):
                vlog.info(f'FlowBySector FlowAmount for {source_name} '
                          f'{activity_set} {i} does not exist in the FBS')
                continue
            # make reporting more manageable
            if abs(diff_per) > 0.01:
                diff_per = round(diff_per, 2)
            else:
                diff_per = round(diff_per, 6)

            # diff_units = df_merge_subset['FBS_unit'][0]
            if diff_per > 0:
                vlog.info(f'FlowBySector FlowAmount for {source_name} '
                          f'{activity_set} {i} {j} at {k} is '
                          f'{str(abs(diff_per))}% less than the '
                          f'FlowByActivity FlowAmount')
            elif diff_per < 0:
                vlog.info(f'FlowBySector FlowAmount for {source_name} '
                          f'{activity_set} {i} {j} at {k} is '
                          f'{str(abs(diff_per))}% more than the '
                          f'FlowByActivity FlowAmount')
            elif diff_per == 0:
                vlog.info(f'FlowBySector FlowAmount for {source_name} '
                          f'{activity_set} {i} {j} at {k} is equal to the '
                          f'FlowByActivity FlowAmount')

        # subset the df to include in the validation log
        # only print rows where the percent difference does not round to 0
        df_v = df_merge[df_merge['Percent_difference'].apply(
            lambda x: round(x, 3) != 0)].reset_index(drop=True)

        # log output
        log.info('Save the comparison of FlowByActivity load to FlowBySector '
                 f'total FlowAmounts for {activity_set} in validation log file')
        # if df not empty, print, if empty, print string
        if df_v.empty:
            vlog.info(f'Percent difference between loaded FBA and '
                      f'output FBS for {activity_set} all round to 0')
        else:
            vlog.info('Comparison of FBA load to FBS total '
                      'FlowAmounts for %s: '
                      '\n {}'.format(df_v.to_string()), activity_set)
    except:
        vlog.info('Error occurred when comparing total FlowAmounts '
                  'for FlowByActivity and FlowBySector')


def compare_summation_at_sector_lengths_between_two_dfs(df1, df2):
    """
    Check summed 'FlowAmount' values at each sector length
    :param df1: df, first df of values with sector columns
    :param df2: df, second df of values with sector columns
    :return: df, comparison of sector summation results by region and
    printout if any child naics sum greater than parent naics
    """
    from flowsa.flowbyfunctions import assign_columns_of_sector_levels

    agg_cols = ['Class', 'SourceName', 'FlowName', 'Unit', 'FlowType',
                'Compartment', 'Location', 'Year', 'SectorProducedByLength',
                'SectorConsumedByLength']

    df_list = []
    for df in [df1, df2]:
        df = replace_NoneType_with_empty_cells(df)
        df = assign_columns_of_sector_levels(df)
        # sum flowamounts by sector length
        dfsum = df.groupby(agg_cols).agg({'FlowAmount': 'sum'}).reset_index()
        df_list.append(dfsum)

    df_list[0] = df_list[0].rename(columns={'FlowAmount': 'df1'})
    df_list[1] = df_list[1].rename(columns={'FlowAmount': 'df2'})
    dfm = df_list[0].merge(df_list[1], how='outer')
    dfm = dfm.fillna(0)
    dfm['flowIncrease_df1_to_df2_perc'] = (dfm['df2'] - dfm['df1'])/dfm[
        'df1'] * 100
    # dfm2 = dfm[dfm['flowIncrease_df1_to_df2'] != 0]
    # drop cases where sector length is 0 because not included in naics cw
    dfm2 = dfm[~((dfm['SectorProducedByLength'] == 0) & (dfm[
        'SectorConsumedByLength'] == 0))]
    # sort df
    dfm2 = dfm2.sort_values(['Location', 'SectorProducedByLength',
                             'SectorConsumedByLength']).reset_index(drop=True)

    dfm3 = dfm2[dfm2['flowIncrease_df1_to_df2_perc'] < 0]

    if len(dfm3) > 0:
        log.info('See validation log for cases where the second dataframe '
                 'has flow amounts greater than the first dataframe at the '
                 'same location/sector lengths.')
        vlog.info('The second dataframe has flow amounts greater than '
                  'the first dataframe at the same sector lengths: '
                  '\n {}'.format(dfm3.to_string()))
    else:
        vlog.info('The second dataframe does not have flow amounts '
                  'greater than the first dataframe at any sector '
                  'length')


def compare_child_to_parent_sectors_flowamounts(df_load):
    """
    Sum child sectors up to one sector and compare to parent sector values
    :param df_load: df, contains sector columns
    :return: comparison of flow values
    """
    from flowsa.flowbyfunctions import assign_sector_match_column

    merge_cols = [e for e in df_load.columns if e in [
        'Class', 'SourceName', 'MetaSources', 'FlowName', 'Unit',
        'FlowType', 'Flowable', 'ActivityProducedBy', 'ActivityConsumedBy',
        'Compartment', 'Context', 'Location', 'Year', 'Description']]
    # determine if activities are sector-like
    sector_like_activities = check_activities_sector_like(df_load)
    # if activities are sector like, drop columns from merge group
    if sector_like_activities:
        merge_cols = [e for e in merge_cols if e not in (
            'ActivityProducedBy', 'ActivityConsumedBy')]

    agg_cols = merge_cols + ['SectorProducedMatch', 'SectorConsumedMatch']
    dfagg = pd.DataFrame()
    for i in range(3, 7):
        df = subset_df_by_sector_lengths(df_load, [i])
        for s in ['Produced', 'Consumed']:
            df = assign_sector_match_column(df, f'Sector{s}By', i, i-1).rename(
                columns={'sector_group': f'Sector{s}Match'})
            df = df.fillna('')
        df2 = df.groupby(agg_cols).agg(
            {'FlowAmount': 'sum'}).rename(columns={
            'FlowAmount': 'ChildNAICSSum'}).reset_index()
        dfagg = pd.concat([dfagg, df2], ignore_index=True)

    # merge new df with summed child naics to original df
    drop_cols = [e for e in df_load.columns if e in
                 ['MeasureofSpread', 'Spread', 'DistributionType', 'Min',
                  'Max', 'DataReliability', 'DataCollection', 'Description',
                  'SectorProducedMatch', 'SectorConsumedMatch']]
    dfm = df_load.merge(dfagg, how='left', left_on=merge_cols + [
        'SectorProducedBy', 'SectorConsumedBy'], right_on=agg_cols).drop(
        columns=drop_cols)
    dfm = dfm.assign(FlowDiff=dfm['ChildNAICSSum'] - dfm['FlowAmount'])
    dfm['PercentDiff'] = (dfm['FlowDiff'] / dfm['FlowAmount']) * 100

    cols_subset = [e for e in dfm.columns if e in [
        'Class', 'SourceName', 'MetaSources', 'Flowable', 'FlowName',
        'Unit', 'FlowType', 'ActivityProducedBy', 'ActivityConsumedBy',
        'Context', 'Location', 'Year', 'SectorProducedBy',
        'SectorConsumedBy', 'FlowAmount', 'ChildNAICSSum', 'PercentDiff']]
    dfm = dfm[cols_subset]

    # subset df where child sectors sum to be greater than parent sectors
    tolerance = 1
    dfm2 = dfm[(dfm['PercentDiff'] > tolerance) |
               (dfm['PercentDiff'] < - tolerance)].reset_index(drop=True)

    if len(dfm2) > 0:
        log.info(f'See validation log for cases where child sectors sum to be '
                 f'different than parent sectors by at least {tolerance}%.')
        vlog.info('There are cases where child sectors sum to be '
                  'different than parent sectors by at least %s%%: '
                  '\n {}'.format(dfm2.to_string()), tolerance)
    else:
        vlog.info(f'No child sectors sum to be different than parent '
                  f'sectors by at least {tolerance}%.')


def check_for_nonetypes_in_sector_col(df):
    """
    Check for NoneType in columns where datatype = string
    :param df: df with columns where datatype = object
    :return: warning message if there are NoneTypes
    """
    # if datatypes are strings, return warning message
    if df['Sector'].isnull().any():
        vlog.warning("There are NoneType values in the 'Sector' column")
    return df


def check_for_negative_flowamounts(df):
    """
    Check for negative FlowAmounts in a dataframe 'FlowAmount' column
    :param df: df, requires 'FlowAmount' column
    :return: df, unchanged
    """
    # return a warning if there are negative flowamount values
    if (df['FlowAmount'].values < 0).any():
        vlog.warning('There are negative FlowAmounts')

    return df


def compare_FBS_results(fbs1, fbs2, ignore_metasources=False,
                        compare_to_remote=False):
    """
    Compare a parquet on Data Commons to a parquet stored locally
    :param fbs1: str, name of method 1
    :param fbs2: str, name of method 2
    :param ignore_metasources: bool, True to compare fbs without
    matching metasources
    :param compare_to_remote: bool, True to download fbs1 from remote and
    compare to fbs2 generated here
    :return: df, comparison of the two dfs
    """
    import flowsa

    # load first file
    df1 = flowsa.getFlowBySector(fbs1,
                                 download_FBS_if_missing=compare_to_remote
                                 ).rename(columns={'FlowAmount': 'FlowAmount_fbs1'})
    df1 = replace_strings_with_NoneType(df1)
    # load second file
    if compare_to_remote:
        # Generate the FBS locally and then immediately load
        df2 = (FlowBySector.generateFlowBySector(
                method=fbs2, download_sources_ok=True)
               .rename(columns={'FlowAmount': 'FlowAmount_fbs2'}))
    else:
        df2 = flowsa.getFlowBySector(fbs2).rename(
            columns={'FlowAmount': 'FlowAmount_fbs2'})
    df2 = replace_strings_with_NoneType(df2)
    # compare df
    merge_cols = [c for c in df2.select_dtypes(include=[
        'object', 'int']).columns if c not in dq_fields]
    if ignore_metasources:
        for e in ['MetaSources', 'AttributionSources']:
            try:
                merge_cols.remove(e)
            except ValueError:
                pass
    # todo: remove merge_col edit once the added columns from DataVis branch
    #  are pulled into master 12/1/22
    # ignore additional columns on merge if they do not exist in first
    # dataframe (version on Data commons if comparing to remote)
    merge_cols = [e for e in merge_cols if e in df1.columns]

    # aggregate dfs before merge - might have duplicate sectors due to
    # dropping metasources/attribution sources
    df1 = (df1.groupby(merge_cols, dropna=False)
           .agg({'FlowAmount_fbs1': 'sum'}).reset_index())
    df2 = (df2.groupby(merge_cols, dropna=False)
           .agg({'FlowAmount_fbs2': 'sum'}).reset_index())
    # check units
    compare_df_units(df1, df2)
    df_m = pd.DataFrame(
        pd.merge(df1[merge_cols + ['FlowAmount_fbs1']],
                 df2[merge_cols + ['FlowAmount_fbs2']],
                 how='outer'))
    df_m = df_m.assign(FlowAmount_diff=df_m['FlowAmount_fbs2']
                       .fillna(0) - df_m['FlowAmount_fbs1'].fillna(0))
    df_m = df_m.assign(
        Percent_Diff=(df_m['FlowAmount_diff']/df_m['FlowAmount_fbs1']) * 100)
    df_m = df_m[df_m['FlowAmount_diff'].apply(
        lambda x: round(abs(x), 2) != 0)].reset_index(drop=True)
    # if no differences, print, if differences, provide df subset
    if len(df_m) == 0:
        vlog.debug('No differences between dataframes')
    else:
        vlog.debug('Differences exist between dataframes')
        df_m = df_m.sort_values(['Location', 'SectorProducedBy',
                                 'SectorConsumedBy', 'Flowable',
                                 'Context', ]).reset_index(drop=True)

    return df_m


def compare_geographic_totals(
    df_subset, df_load, sourcename, attr, activity_set, activity_names,
    df_type='FBA', subnational_geoscale=None
):
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
    :param type: str, 'FBA' or 'FBS'
    :param subnational_geoscale: geoscale being compared against the
        national geoscale. Only necessary if df_subset is a FlowBy object
        rather than a DataFrame.
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

        # compare df
        merge_cols = ['Class', 'SourceName', 'Unit', 'FlowType',
                      'ActivityProducedBy', 'ActivityConsumedBy',
                      'Location', 'LocationSystem', 'Year']

        if df_type == 'FBA':
            merge_cols.extend(['FlowName', 'Compartment'])
        else:
             merge_cols.extend(['Flowable', 'Context'])

        sub2 = aggregator(sub, merge_cols).rename(
            columns={'FlowAmount': 'FlowAmount_sub'})

        # compare units
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

        subnational_geoscale = (subnational_geoscale
                                or attr['allocation_from_scale'])
        if len(df_m_sub) == 0:
            vlog.info(f'No data loss greater than 1%% between national '
                      f'level data and {subnational_geoscale} subset')
        else:
            vlog.info(f'There are data differences between published national '
                      f'values and {subnational_geoscale} subset, '
                      f'saving to validation log')

            vlog.info(
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
        df['Flowable'] = np.where(
            df['ActivityConsumedBy'] != 'Livestock', 'Water', df['Flowable'])
        df['Context'] = np.where(df['Context'].str.contains('resource/water/'),
                                 'resource/water', df['Context'])

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


def calculate_industry_coefficients(fbs_load, year,region,
                                    io_level, impacts=False):
    """
    Generates sector coefficients (flow/$) for all sectors for all locations.

    :param fbs_load: flow by sector method
    :param year: year for industry output dataset
    :param region: str, 'state' or 'national'
    :param io_level: str, 'summary' or 'detail'
    :param impacts: bool or str, True to apply and aggregate on impacts using TRACI,
        False to compare flow/contexts, str to pass alternate method
    """
    from flowsa.sectormapping import map_to_BEA_sectors,\
        get_BEA_industry_output

    fbs = collapse_fbs_sectors(fbs_load)

    fbs = map_to_BEA_sectors(fbs, region, io_level, year)

    inventory = not(impacts)
    if impacts:
        if isinstance(impacts, bool):
            impacts = 'TRACI2.1'
        try:
            import lciafmt
            fbs_summary = (lciafmt.apply_lcia_method(fbs, impacts)
                           .rename(columns={'FlowAmount': 'InvAmount',
                                            'Impact': 'FlowAmount'}))
            groupby_cols = ['Location', 'Sector',
                            'Indicator', 'Indicator unit']
            sort_by_cols = ['Indicator', 'Sector', 'Location']
        except ImportError:
            log.warning('lciafmt not installed')
            inventory = True
        except AttributeError:
            log.warning('check lciafmt branch')
            inventory = True

    if inventory:
        fbs_summary = fbs.copy()
        groupby_cols = ['Location', 'Sector',
                        'Flowable', 'Context', 'Unit']
        sort_by_cols = ['Context', 'Flowable',
                        'Sector', 'Location']

    # Update location if needed prior to aggregation
    if region == 'national':
        fbs_summary["Location"] = US_FIPS

    fbs_summary = (fbs_summary.groupby(groupby_cols)
                   .agg({'FlowAmount': 'sum'}).
                   reset_index())

    bea = get_BEA_industry_output(region, io_level, year)

    # Add sector output and assign coefficients
    fbs_summary = fbs_summary.merge(bea.rename(
        columns={'BEA': 'Sector'}), how = 'left',
        on=['Sector','Location'])
    fbs_summary['Coefficient'] = (fbs_summary['FlowAmount'] /
                                      fbs_summary['Output'])
    fbs_summary = fbs_summary.sort_values(by=sort_by_cols)

    return fbs_summary


if __name__ == "__main__":
    df1 = calculate_industry_coefficients(
            flowsa.getFlowBySector('Water_national_2015_m1'), 2015,
            "national", "summary", False)
    df2 = calculate_industry_coefficients(
            flowsa.getFlowBySector('GRDREL_national_2017'), 2017,
            "national", "summary", True)
    df3 = calculate_industry_coefficients(
            flowsa.getFlowBySector('GRDREL_national_2017'), 2017,
            "national", "detail", True)
    df4 = calculate_industry_coefficients(
            flowsa.getFlowBySector('GRDREL_state_2017'), 2017,
            "national", "detail", True)
    try:
        df5 = calculate_industry_coefficients(
                flowsa.getFlowBySector('GRDREL_state_2017'), 2017,
                "state", "detail", True)
    except TypeError:
        df5 = None
