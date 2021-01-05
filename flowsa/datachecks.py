"""
Functions to check data is loaded correctly
"""

import pandas as pd
from flowsa.flowbyfunctions import fba_fill_na_dict, harmonize_units, fba_activity_fields, filter_by_geoscale, \
    fba_default_grouping_fields, fbs_default_grouping_fields, aggregator, sector_aggregation, fbs_fill_na_dict, \
    fbs_activity_fields, clean_df, create_geoscale_list, sector_disaggregation, replace_strings_with_NoneType, \
    replace_NoneType_with_empty_cells
from flowsa.common import US_FIPS, sector_level_key, flow_by_sector_fields, load_sector_length_crosswalk_w_nonnaics, \
    load_sector_crosswalk, sector_source_name, log, fips_number_key, outputpath


def check_flow_by_fields(flowby_df, flowbyfields):
    """
    Add in missing fields to have a complete and ordered
    :param flowby_df: Either flowbyactivity or flowbysector df
    :param flowbyfields: Either flow_by_activity_fields or flow_by_sector_fields
    :return:
    """
    for k, v in flowbyfields.items():
        try:
            log.debug("fba activity " + k + " data type is " + str(flowby_df[k].values.dtype))
            log.debug("standard " + k + " data type is " + str(v[0]['dtype']))
        except:
            log.debug("Failed to find field ", k, " in fba")


def check_if_activities_match_sectors(fba):
    """
    Checks if activities in flowbyactivity that appear to be like sectors are actually sectors
    :param fba: a flow by activity dataset
    :return: A list of activities not marching the default sector list or text indicating 100% match
    """
    # Get list of activities in a flowbyactivity file
    activities = []
    for f in fba_activity_fields:
        activities.extend(fba[f])
    #activities.remove("None")

    # Get list of module default sectors
    flowsa_sector_list = list(load_sector_crosswalk()[sector_source_name])
    activities_missing_sectors = set(activities) - set(flowsa_sector_list)

    if len(activities_missing_sectors) > 0:
        log.info(str(len(
            activities_missing_sectors)) + " activities not matching sectors in default " + sector_source_name + " list.")
        return activities_missing_sectors
    else:
        log.info("All activities match sectors in " + sector_source_name + " list.")
        return None


def check_if_data_exists_at_geoscale(df, geoscale, activitynames='All'):
    """
    Check if an activity or a sector exists at the specified geoscale
    :param df: flowbyactivity dataframe
    :param activitynames: Either an activity name (ex. 'Domestic') or a sector (ex. '1124')
    :param geoscale: national, state, or county
    :return:
    """

    # if any activity name is specified, check if activity data exists at the specified geoscale
    activity_list = []
    if activitynames != 'All':
        if isinstance(activitynames, str) == True:
            activity_list.append(activitynames)
        else:
            activity_list = activitynames
        # check for specified activity name
        df = df[(df[fba_activity_fields[0]].isin(activity_list)) |
                (df[fba_activity_fields[1]].isin(activity_list))].reset_index(drop=True)
    else:
        activity_list.append('activities')

    # filter by geoscale depends on Location System
    fips = create_geoscale_list(df, geoscale)

    df = df[df['Location'].isin(fips)]

    if len(df) == 0:
        log.info(
            "No flows found for " + ', '.join(activity_list) + " at the " + geoscale + " scale.")
        exists = "No"
    else:
        log.info("Flows found for " + ', '.join(activity_list) + " at the " + geoscale + " scale.")
        exists = "Yes"

    return exists


def check_if_data_exists_at_less_aggregated_geoscale(df, geoscale, activityname):
    """
    In the event data does not exist at specified geoscale, check if data exists at less aggregated level
    :param df: Either flowbyactivity or flowbysector dataframe
    :param data_to_check: Either an activity name (ex. 'Domestic') or a sector (ex. '1124')
    :param geoscale: national, state, or county
    :param flowbytype: 'fba' for flowbyactivity, 'fbs' for flowbysector
    :return:
    """

    if geoscale == 'national':
        df = df[(df[fba_activity_fields[0]] == activityname) | (
                df[fba_activity_fields[1]] == activityname)]
        fips = create_geoscale_list(df, 'state')
        df = df[df['Location'].isin(fips)]
        if len(df) == 0:
            log.info("No flows found for " + activityname + "  at the state scale.")
            fips = create_geoscale_list(df, 'county')
            df = df[df['Location'].isin(fips)]
            if len(df) == 0:
                log.info("No flows found for " + activityname + "  at the county scale.")
            else:
                log.info("Flowbyactivity data exists for " + activityname + " at the county level")
                new_geoscale_to_use = 'county'
                return new_geoscale_to_use
        else:
            log.info("Flowbyactivity data exists for " + activityname + " at the state level")
            new_geoscale_to_use = 'state'
            return new_geoscale_to_use
    if geoscale == 'state':
        df = df[(df[fba_activity_fields[0]] == activityname) | (
                df[fba_activity_fields[1]] == activityname)]
        fips = create_geoscale_list(df, 'county')
        df = df[df['Location'].isin(fips)]
        if len(df) == 0:
            log.info("No flows found for " + activityname + "  at the county scale.")
        else:
            log.info("Flowbyactivity data exists for " + activityname + " at the county level")
            new_geoscale_to_use = 'county'
            return new_geoscale_to_use


def check_if_location_systems_match(df1, df2):
    """
    Check if two dataframes share the same location system
    :param df1: fba or fbs df
    :param df2: fba or fbs df
    :return:
    """

    if df1["LocationSystem"].all() == df2["LocationSystem"].all():
        log.info("LocationSystems match")
    else:
        log.warning("LocationSystems do not match, might lose county level data")


def check_if_data_exists_for_same_geoscales(fba_wsec_walloc, source,
                                            activity):  # fba_w_aggregated_sectors
    """
    Determine if data exists at the same scales for datasource and allocation source
    :param source_fba:
    :param allocation_fba:
    :return:
    """
    # todo: modify so only returns warning if no value for entire location, not just no value for one of the possible sectors

    from flowsa.mapping import get_activitytosector_mapping

    # create list of highest sector level for which there should be data
    mapping = get_activitytosector_mapping(source)
    # filter by activity of interest
    mapping = mapping.loc[mapping['Activity'].isin(activity)]
    # add sectors to list
    sectors_list = pd.unique(mapping['Sector']).tolist()

    # subset fba w sectors and with merged allocation table so only have rows with aggregated sector list
    df_subset = fba_wsec_walloc.loc[
        (fba_wsec_walloc[fbs_activity_fields[0]].isin(sectors_list)) |
        (fba_wsec_walloc[fbs_activity_fields[1]].isin(sectors_list))].reset_index(drop=True)
    # only interested in total flows
    # df_subset = df_subset.loc[df_subset['FlowName'] == 'total'].reset_index(drop=True)
    # df_subset = df_subset.loc[df_subset['Compartment'] == 'total'].reset_index(drop=True)

    # create subset of fba where the allocation data is missing
    missing_alloc = df_subset.loc[df_subset['FlowAmountRatio'].isna()].reset_index(drop=True)
    # drop any rows where source flow value = 0
    missing_alloc = missing_alloc.loc[missing_alloc['FlowAmount'] != 0].reset_index(drop=True)
    # create list of locations with missing alllocation data
    states_missing_data = pd.unique(missing_alloc['Location']).tolist()

    if len(missing_alloc) == 0:
        log.info("All aggregated sector flows have allocation flow ratio data")
    else:
        log.warning("Missing allocation flow ratio data for " + ', '.join(states_missing_data))

    return None


def check_if_losing_sector_data(df, target_sector_level):
    """
    Determine rows of data that will be lost if subset data at target sector level
    In some instances, not all
    :param fbs:
    :return:
    """

    # exclude nonsectors
    df = replace_NoneType_with_empty_cells(df)

    rows_lost = pd.DataFrame()
    for i in range(2, sector_level_key[target_sector_level]):
        # create df of i length
        df_x1 = df.loc[(df[fbs_activity_fields[0]].apply(lambda x: len(x) == i)) &
                       (df[fbs_activity_fields[1]] == '')]
        df_x2 = df.loc[(df[fbs_activity_fields[0]] == '') &
                       (df[fbs_activity_fields[1]].apply(lambda x: len(x) == i))]
        df_x3 = df.loc[(df[fbs_activity_fields[0]].apply(lambda x: len(x) == i)) &
                       (df[fbs_activity_fields[1]].apply(lambda x: len(x) == i))]
        df_x = pd.concat([df_x1, df_x2, df_x3], ignore_index=True, sort=False)

        # create df of i + 1 length
        df_y1 = df.loc[df[fbs_activity_fields[0]].apply(lambda x: len(x) == i + 1) |
                       df[fbs_activity_fields[1]].apply(lambda x: len(x) == i + 1)]
        df_y2 = df.loc[df[fbs_activity_fields[0]].apply(lambda x: len(x) == i + 1) &
                       df[fbs_activity_fields[1]].apply(lambda x: len(x) == i + 1)]
        df_y = pd.concat([df_y1, df_y2], ignore_index=True, sort=False)

        # create temp sector columns in df y, that are i digits in length
        df_y.loc[:, 'spb_tmp'] = df_y[fbs_activity_fields[0]].apply(lambda x: x[0:i])
        df_y.loc[:, 'scb_tmp'] = df_y[fbs_activity_fields[1]].apply(lambda x: x[0:i])
        # don't modify household sector lengths
        df_y = df_y.replace({'F0': 'F010',
                             'F01': 'F010'})

        # merge the two dfs
        df_m = pd.merge(df_x,
                        df_y[['Class', 'Context', 'FlowType', 'Flowable', 'Location', 'LocationSystem', 'Unit',
                              'Year', 'spb_tmp', 'scb_tmp']],
                        how='left',
                        left_on=['Class', 'Context', 'FlowType', 'Flowable', 'Location', 'LocationSystem', 'Unit',
                                 'Year', 'SectorProducedBy', 'SectorConsumedBy'],
                        right_on=['Class', 'Context', 'FlowType', 'Flowable', 'Location', 'LocationSystem', 'Unit',
                                  'Year', 'spb_tmp', 'scb_tmp'])

        # extract the rows that are not disaggregated to more specific naics
        rl = df_m[(df_m['scb_tmp'].isnull()) & (df_m['spb_tmp'].isnull())]
        # clean df
        rl = clean_df(rl, flow_by_sector_fields, fbs_fill_na_dict)
        rl_list = rl[['SectorProducedBy', 'SectorConsumedBy']].drop_duplicates().values.tolist()

        # match sectors with target sector length sectors

        # import cw and subset to current sector length and target sector length
        cw_load = load_sector_length_crosswalk_w_nonnaics()
        nlength = list(sector_level_key.keys())[list(sector_level_key.values()).index(i)]
        cw = cw_load[[nlength, target_sector_level]].drop_duplicates()
        # add column with counts
        cw['sector_count'] = cw.groupby(nlength)[nlength].transform('count')

        # merge df & conditionally replace sector produced/consumed columns
        rl_m = pd.merge(rl, cw, how='left', left_on=[fbs_activity_fields[0]], right_on=[nlength])
        rl_m.loc[rl_m[fbs_activity_fields[0]] != '', fbs_activity_fields[0]] = rl_m[target_sector_level]
        rl_m = rl_m.drop(columns=[nlength, target_sector_level])

        rl_m2 = pd.merge(rl_m, cw, how='left', left_on=[fbs_activity_fields[1]], right_on=[nlength])
        rl_m2.loc[rl_m2[fbs_activity_fields[1]] != '', fbs_activity_fields[1]] = rl_m2[target_sector_level]
        rl_m2 = rl_m2.drop(columns=[nlength, target_sector_level])

        # create one sector count column
        rl_m2['sector_count_x'] = rl_m2['sector_count_x'].fillna(rl_m2['sector_count_y'])
        rl_m3 = rl_m2.rename(columns={'sector_count_x': 'sector_count'})
        rl_m3 = rl_m3.drop(columns=['sector_count_y'])

        # calculate new flow amounts, based on sector count, allocating equally to the new sector length codes
        rl_m3['FlowAmount'] = rl_m3['FlowAmount'] / rl_m3['sector_count']
        rl_m3 = rl_m3.drop(columns=['sector_count'])

        # append to df
        if len(rl) != 0:
            log.warning('Data found at ' + str(i) + ' digit NAICS not represented in current '
                                                    'data subset: {}'.format(' '.join(map(str, rl_list))))
            rows_lost = rows_lost.append(rl_m3, ignore_index=True, sort=True)

    if len(rows_lost) == 0:
        log.info('Data exists at ' + target_sector_level)
    else:
        log.info('Allocating FlowAmounts equally to each ' + target_sector_level +
                 ' associated with the sectors previously dropped')

    # add rows of missing data to the fbs sector subset
    df_w_lost_data = pd.concat([df, rows_lost], ignore_index=True, sort=True)
    df_w_lost_data = replace_strings_with_NoneType(df_w_lost_data)

    return df_w_lost_data


def check_allocation_ratios(flow_alloc_df, activity_set, source_name, method_name):
    """
    Check for issues with the flow allocation ratios
    :param df:
    :return:
    """

    # create column of sector lengths
    flow_alloc_df.loc[:, 'slength'] = flow_alloc_df['Sector'].apply(lambda x: len(x))
    # subset df
    flow_alloc_df2 = flow_alloc_df[['FBA_Activity', 'Location', 'slength', 'FlowAmountRatio']]
    # sum the flow amount ratios by location and sector length
    flow_alloc_df3 = flow_alloc_df2.groupby(['FBA_Activity', 'Location', 'slength'],
                                            as_index=False)[["FlowAmountRatio"]].agg("sum")
    # not interested in sector length > 6
    flow_alloc_df4 = flow_alloc_df3[flow_alloc_df3['slength'] <= 6]

    ua_count1 = len(flow_alloc_df4[flow_alloc_df4['FlowAmountRatio'] < 1])
    log.info('There are ' + str(ua_count1) +
             ' instances at a sector length of 6 or less where the allocation ratio for a location and sector length is < 1')
    ua_count2 = len(flow_alloc_df4[flow_alloc_df4['FlowAmountRatio'] < 0.99])
    log.info('There are ' + str(ua_count2) +
             ' instances at a sector length of 6 or less where the allocation ratio for a location and sector length is < 0.99')
    ua_count3 = len(flow_alloc_df4[flow_alloc_df4['FlowAmountRatio'] > 1])
    log.info('There are ' + str(ua_count3) +
             ' instances at a sector length of 6 or less where the allocation ratio for a location and sector length is > 1')
    ua_count4 = len(flow_alloc_df4[flow_alloc_df4['FlowAmountRatio'] > 1.01])
    log.info('There are ' + str(ua_count4) +
             ' instances at a sector length of 6 or less where the allocation ratio for a location and sector length is > 1.01')

    # save csv to output folder
    log.info('Save the summary table of flow allocation ratios for each sector length for ' +
             activity_set + ' in output folder')
    # output data for all sector lengths
    flow_alloc_df3.to_csv(outputpath + "FlowBySectorMethodAnalysis/" + method_name + '_' + source_name +
                          "_allocation_ratios_" + activity_set + ".csv", index=False)

    return None


def check_for_differences_between_fba_load_and_fbs_output(fba_load, fbs_load, activity_set, source_name, method_name):
    """
    Function to compare the loaded flowbyactivity with the final flowbysector output, checking for data loss
    :param df:
    :return:
    """

    from flowsa.flowbyfunctions import replace_strings_with_NoneType, replace_NoneType_with_empty_cells

    # subset fba df
    fba = fba_load[['Class', 'SourceName', 'Flowable', 'Unit', 'FlowType', 'ActivityProducedBy',
                    'ActivityConsumedBy', 'Context', 'Location', 'LocationSystem', 'Year',
                    'FlowAmount']].drop_duplicates().reset_index(drop=True)
    fba.loc[:, 'Location'] = US_FIPS
    group_cols = ['ActivityProducedBy', 'ActivityConsumedBy', 'Flowable', 'Unit', 'FlowType', 'Context',
                  'Location', 'LocationSystem', 'Year']
    fba_agg = aggregator(fba, group_cols)
    fba_agg.rename(columns={'FlowAmount': 'FBA_amount'}, inplace=True)

    # subset fbs df
    fbs = fbs_load[['Class', 'SectorSourceName', 'Flowable', 'Unit', 'FlowType', 'SectorProducedBy', 'SectorConsumedBy',
                    'ActivityProducedBy', 'ActivityConsumedBy', 'Context', 'Location', 'LocationSystem', 'Year',
                    'FlowAmount']].drop_duplicates().reset_index(drop=True)

    fbs = replace_NoneType_with_empty_cells(fbs)

    fbs['ProducedLength'] = fbs['SectorProducedBy'].apply(lambda x: len(x))
    fbs['ConsumedLength'] = fbs['SectorConsumedBy'].apply(lambda x: len(x))
    fbs['SectorLength'] = fbs[['ProducedLength', 'ConsumedLength']].max(axis=1)
    fbs.loc[:, 'Location'] = US_FIPS
    group_cols = ['ActivityProducedBy', 'ActivityConsumedBy', 'Flowable', 'Unit', 'FlowType', 'Context',
                  'Location', 'LocationSystem', 'Year', 'SectorLength']
    fbs_agg = aggregator(fbs, group_cols)
    fbs_agg.rename(columns={'FlowAmount': 'FBS_amount'}, inplace=True)

    # merge compare 1 and compare 2
    df_merge = fba_agg.merge(fbs_agg,
                               left_on=['ActivityProducedBy', 'ActivityConsumedBy', 'Flowable', 'Unit',
                                        'FlowType', 'Context', 'Location','LocationSystem', 'Year'],
                               right_on=['ActivityProducedBy', 'ActivityConsumedBy', 'Flowable', 'Unit',
                                         'FlowType', 'Context', 'Location', 'LocationSystem', 'Year'],
                               how='left')
    df_merge['Ratio'] = df_merge['FBS_amount'] / df_merge['FBA_amount']

    # reorder
    df_merge = df_merge[['ActivityProducedBy', 'ActivityConsumedBy', 'Flowable', 'Unit', 'FlowType', 'Context',
                             'Location', 'LocationSystem', 'Year', 'SectorLength', 'FBA_amount', 'FBS_amount', 'Ratio']]

    # only report difference at sector length <= 6
    comparison = df_merge[df_merge['SectorLength'] <= 6]

    # todo: address the duplicated rows/data that occur for non-naics household sector length

    ua_count1 = len(comparison[comparison['Ratio'] < 0.95])
    log.info('There are ' + str(ua_count1) +
             ' combinations of flowable/context/sector length where the flowbyactivity to flowbysector ratio is < 0.95')
    ua_count2 = len(comparison[comparison['Ratio'] < 0.99])
    log.info('There are ' + str(ua_count2) +
             ' combinations of flowable/context/sector length where the flowbyactivity to flowbysector ratio is < 0.99')
    oa_count1 = len(comparison[comparison['Ratio'] > 1])
    log.info('There are ' + str(oa_count1) +
             ' combinations of flowable/context/sector length where the flowbyactivity to flowbysector ratio is > 1.0')
    oa_count2 = len(comparison[comparison['Ratio'] > 1.01])
    log.info('There are ' + str(oa_count2) +
             ' combinations of flowable/context/sector length where the flowbyactivity to flowbysector ratio is > 1.01')

    # save csv to output folder
    log.info('Save the comparision of FlowByActivity load to FlowBySector ratios for ' +
              activity_set + ' in output folder')
    # output data at all sector lengths
    df_merge.to_csv(outputpath + "FlowBySectorMethodAnalysis/" + method_name + '_' + source_name +
                                "_FBA_load_to_FBS_comparision_" + activity_set + ".csv", index=False)

    return None


def compare_fba_load_and_fbs_output_totals(fba_load, fbs_load, activity_set, source_name, method_name, attr, method):
    """
    Function to compare the loaded flowbyactivity total with the final flowbysector output total
    :param df:
    :return:
    """

    from flowsa.flowbyfunctions import harmonize_units, subset_df_by_geoscale

    log.info('Comparing loaded FlowByActivity FlowAmount total to subset FlowBySector FlowAmount total')

    # harmonize units
    fba = harmonize_units(fba_load)
    # subset/agg dfs
    col_subset = ['Class', 'FlowAmount', 'Unit', 'Location', 'LocationSystem']
    group_cols = ['Class', 'Unit', 'Location', 'LocationSystem']
    # fba
    # extract relevant geoscale data or aggregate existing data
    fba = subset_df_by_geoscale(fba, attr['allocation_from_scale'], method['target_geoscale'])
    fba = fba[col_subset]
    fba_agg = aggregator(fba, group_cols)
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
        df_merge['FlowAmount_difference'] = df_merge['FBA_amount'] - df_merge['FBS_amount']
        df_merge['Percent_difference'] = (df_merge['FlowAmount_difference']/df_merge['FBA_amount']) * 100

        # reorder
        df_merge = df_merge[['Class', 'Location', 'LocationSystem', 'FBA_amount', 'FBA_unit',
                             'FBS_amount', 'FBS_unit', 'FlowAmount_difference', 'Percent_difference']]

        diff_per = df_merge['Percent_difference'][0]
        # make reporting more manageable
        if abs(diff_per) > 0.001:
            diff_per = round(diff_per, 2)
        else:
            diff_per = round(diff_per, 6)

        diff_units = df_merge['FBS_unit'][0]
        if diff_per > 0:
            log.info('The total FlowBySector FlowAmount for ' + source_name + ' ' + activity_set +
                     ' is ' + str(abs(diff_per)) + '% less than the total FlowByActivity FlowAmount')
        else:
            log.info('The total FlowBySector FlowAmount for ' + source_name + ' ' + activity_set +
                     ' is ' + str(abs(diff_per)) + '% more than the total FlowByActivity FlowAmount')

        # save csv to output folder
        log.info('Save the comparision of FlowByActivity load to FlowBySector total FlowAmounts for ' +
                  activity_set + ' in output folder')
        # output data at all sector lengths
        df_merge.to_csv(outputpath + "FlowBySectorMethodAnalysis/" + method_name + '_' + source_name +
                                    "_FBA_total_to_FBS_total_FlowAmount_comparision_" + activity_set + ".csv", index=False)

    except:
        log.info('Error occured when comparing total FlowAmounts for FlowByActivity and FlowBySector')

    return None


def check_summation_at_sector_lengths(df):

    # columns to keep
    df_cols = [e for e in df.columns if e not in ('MeasureofSpread', 'Spread', 'DistributionType', 'Min', 'Max',
                                                  'DataReliability', 'DataCollection', 'FlowType', 'Compartment',
                                                  'Description', 'Activity')]
    # subset df
    df2 = df[df_cols]

    # rename columns and clean up df
    df2 = df2[~df2['Sector'].isnull()]

    df2 = df2.assign(slength=df2['Sector'].apply(lambda x: len(x)))

    # sum flowamounts by sector length
    denom_df = df2.copy()
    denom_df.loc[:, 'Denominator'] = denom_df.groupby(['Location', 'slength'])['FlowAmount'].transform('sum')

    summed_df = denom_df.drop(columns=['Sector', 'FlowAmount']).drop_duplicates().reset_index(drop=True)

    # max value
    maxv = max(summed_df['Denominator'].apply(lambda x: x))

    # percent of total accounted for
    summed_df = summed_df.assign(percentOfTot=summed_df['Denominator']/maxv)

    summed_df = summed_df.sort_values(['slength']).reset_index(drop=True)

    return summed_df


def check_for_nonetypes(df):
    """
    Check for NoneType in columns where datatype = string
    :param df: df with columns where datatype = object
    :return: warning message if there are NoneTypes
    """
    # if datatypes are strings, return warning message
    for y in df.columns:
        if df[y].dtype == object:
            if df[y].isnull().any():
                log.warning('There are NoneType values in ' + y)
    return df


def check_for_negative_flowamounts(df):

    if (df['FlowAmount'].values < 0).any():
        log.warning('There are negative FlowAmounts')

    return df


# def check_if_data_exists_at_geoscale(df, provided_from_scale):
#     """
#     Check if an activity or a sector exists at the specified geoscale
#     :param df: flowbyactivity dataframe
#     :param activitynames: Either an activity name (ex. 'Domestic') or a sector (ex. '1124')
#     :param geoscale: national, state, or county
#     :return:
#     """
#     from flowsa.flowbyfunctions import unique_activity_names, dataframe_difference
#
#     # test
#     # df = flows_subset.copy()
#     # provided_from_scale = v['geoscale_to_use']
#
#     # determine the unique combinations of activityproduced/consumedby
#     unique_activities = unique_activity_names(df)
#     # filter by geoscale
#     fips = create_geoscale_list(df, provided_from_scale)
#     df_sub = df[df['Location'].isin(fips)]
#     # determine unique activities after subsetting by geoscale
#     unique_activities_sub = unique_activity_names(df_sub)
#
#     # return df of the difference between unique_activities and unique_activities2
#     df_missing = dataframe_difference(unique_activities, unique_activities_sub, which='left_only')
#     # return df of the similarities between unique_activities and unique_activities2
#     df_existing = dataframe_difference(unique_activities, unique_activities_sub, which='both')
#     df_existing = df_existing.drop(columns='_merge')
#     df_existing['activity_from_scale'] = provided_from_scale
#
#     # for loop through geoscales until find data for each activity combo
#     if provided_from_scale == 'national':
#         geoscales = ['state', 'county']
#     elif provided_from_scale == 'state':
#         geoscales = ['county']
#     elif provided_from_scale == 'county':
#         log.info('No data - skipping')
#
#     if len(df_missing) > 0:
#         for i in geoscales:
#             # test
#             # i = 'state'
#             # filter by geoscale
#             fips_i = create_geoscale_list(df, i)
#             df_i = df[df['Location'].isin(fips_i)]
#
#             # determine unique activities after subsetting by geoscale
#             unique_activities_i = unique_activity_names(df_i)
#
#             # return df of the difference between unique_activities subset and unique_activities for geoscale
#             df_missing_i = dataframe_difference(unique_activities_sub, unique_activities_i, which='right_only')
#             df_missing_i = df_missing_i.drop(columns='_merge')
#             df_missing_i['activity_from_scale'] = i
#             # return df of the similarities between unique_activities and unique_activities2
#             df_existing_i = dataframe_difference(unique_activities_sub, unique_activities_i, which='both')
#
#             # append unique activities and df with defined activity_from_scale
#             unique_activities_sub = unique_activities_sub.append(df_missing_i[[fba_activity_fields[0],
#                                                                                fba_activity_fields[1]]])
#             df_existing = df_existing.append(df_missing_i)
#             df_missing = dataframe_difference(df_missing[[fba_activity_fields[0],fba_activity_fields[1]]],
#                                               df_existing_i[[fba_activity_fields[0],fba_activity_fields[1]]],
#                                               which=None)
#
#     return df_existing



# from flowsa.flowbyfunctions import unique_activity_names, dataframe_difference
#
# # test
# df = flows_subset.copy()
# geoscale = v['geoscale_to_use']
#
# # determine the unique combinations of activityproduced/consumedby
# unique_activities = unique_activity_names(df)
#
# # filter by geoscale
# fips = create_geoscale_list(df, geoscale)
# df2 = df[df['Location'].isin(fips)]
#
# # determine unique activities after subsetting by geoscale
# unique_activities2 = unique_activity_names(df2)
#
# # return df of the difference between unique_activities and unique_activities2
# df_diff = dataframe_difference(unique_activities, unique_activities2, which='left_only')
# # create a list of the activities lost by subsetting df
# rl_list = df_diff[[fba_activity_fields[0], fba_activity_fields[1]]].drop_duplicates().values.tolist()
# # add column stating activity combos do not exist
# df_diff['exists'] = 'No'
#
# # return df of the similarities between unique_activities and unique_activities2
# df_sim = dataframe_difference(unique_activities, unique_activities2, which='both')
# # create a list of the activities lost by subsetting df
# sim_list = df_sim[[fba_activity_fields[0], fba_activity_fields[1]]].drop_duplicates().values.tolist()
# # add column stating activity combos do not exist
# df_sim['exists'] = 'Yes'
#
# if (len(df_diff) == 0) & (len(df_sim) != 0):
#     log.info("Flows found for activities {}".format(' '.join(map(str, sim_list))) + ' at the ' + geoscale + " scale.")
# else:
#     log.info("No flows found for activities {}".format(' '.join(map(str, rl_list))) +
#              ' at the ' + geoscale + " scale.")
#     if len(df_sim) != 0:
#         log.info("Flows found for activities {}".format(' '.join(map(str, sim_list))) + ' at the ' + geoscale + " scale.")
#
# # concat the df of differences and similarities and turn into a dictionary
# df_comb = pd.concat([df_diff, df_sim])
# df_comb = df_comb.drop(columns='_merge')

# return df_comb


# def check_if_data_exists_at_less_aggregated_geoscale(df_to_check, activiites_to_check, provided_from_scale):
#     """
#     In the event data does not exist at specified geoscale, check if data exists at less aggregated level
#
#     :param df: Either flowbyactivity or flowbysector dataframe
#     :param data_to_check: Either an activity name (ex. 'Domestic') or a sector (ex. '1124')
#     :param geoscale: national, state, or county
#     :param flowbytype: 'fba' for flowbyactivity, 'fbs' for flowbysector
#     :return:
#     """
#     # test
#     # df_to_check = df.copy()
#     # activiites_to_check = missing_data.copy()
#
#     # ensure only have the activity combos want to check
#     df = pd.merge(activiites_to_check, df_to_check)
#
#     # fips_number_key
#     # for loop through geoscales until find data for each activity combo
#     if provided_from_scale == 'national':
#         geoscales = ['state', 'county']
#     elif provided_from_scale == 'state':
#         geoscales = ['county']
#     else:
#         log.info("No data - skipping")
#
#     for i in geoscales:
#         fips = create_geoscale_list(df, i)
#         df = df[df['Location'].isin(fips)]
#         if len(df) == 0:
#             log.info("No flows found at the" + i + " geoscale.")
#             fips = create_geoscale_list(df, 'county')
#             df2 = df[df['Location'].isin(fips)]
#             if len(df2) == 0:
#                 log.info("No flows found at the " + i + " scale.")
#             else:
#                 log.info("Flowbyactivity data exists at the " + i + " scale.")
#                 new_geoscale_to_use = 'county'
#                 return new_geoscale_to_use




# def check_if_data_exists_at_less_aggregated_geoscale(df, geoscale, activityname):
#     """
#     In the event data does not exist at specified geoscale, check if data exists at less aggregated level
#
#     :param df: Either flowbyactivity or flowbysector dataframe
#     :param data_to_check: Either an activity name (ex. 'Domestic') or a sector (ex. '1124')
#     :param geoscale: national, state, or county
#     :param flowbytype: 'fba' for flowbyactivity, 'fbs' for flowbysector
#     :return:
#     """
#
#     if geoscale == 'national':
#         df = df[(df[fba_activity_fields[0]] == activityname) | (
#              df[fba_activity_fields[1]] == activityname)]
#         fips = create_geoscale_list(df, 'state')
#         df = df[df['Location'].isin(fips)]
#         if len(df) == 0:
#             log.info("No flows found for " + activityname + "  at the state scale.")
#             fips = create_geoscale_list(df, 'county')
#             df = df[df['Location'].isin(fips)]
#             if len(df) == 0:
#                 log.info("No flows found for " + activityname + "  at the county scale.")
#             else:
#                 log.info("Flowbyactivity data exists for " + activityname + " at the county level")
#                 new_geoscale_to_use = 'county'
#                 return new_geoscale_to_use
#         else:
#             log.info("Flowbyactivity data exists for " + activityname + " at the state level")
#             new_geoscale_to_use = 'state'
#             return new_geoscale_to_use
#     if geoscale == 'state':
#         df = df[(df[fba_activity_fields[0]] == activityname) | (
#              df[fba_activity_fields[1]] == activityname)]
#         fips = create_geoscale_list(df, 'county')
#         df = df[df['Location'].isin(fips)]
#         if len(df) == 0:
#             log.info("No flows found for " + activityname + "  at the county scale.")
#         else:
#             log.info("Flowbyactivity data exists for " + activityname + " at the county level")
#             new_geoscale_to_use = 'county'
#             return new_geoscale_to_use



# def geoscale_summation(flowclass, years, datasource):
#
#     # test
#     # flowclass = ['Water']
#     # years = [2015]
#     # datasource = 'USGS_NWIS_WU'
#
#     # load parquet file checking aggregation
#     flows = flowsa.getFlowByActivity(flowclass=flowclass,
#                                      years=years,
#                                      datasource=datasource)
#     # fill null values
#     flows = flows.fillna(value=fba_fill_na_dict)
#
#
#     return None


# def geoscale_flow_comparison(flowclass, years, datasource, activitynames=['all'], to_scale='national'):
#     """ Aggregates county data to state and national, and state data to national level, allowing for comparisons
#         in flow totals for a given flowclass and industry. First assigns all flownames to NAICS and standardizes units.
#
#         Assigned to NAICS rather than using FlowNames for aggregation to negate any changes in flownames across
#         time/geoscale
#     """
#
#     # test
#     flowclass = ['Land']
#     years = ['2018']
#     datasource = 'USDA_IWMS'
#
#     flows = flowsa.getFlowByActivity(flowclass=flowclass, years=years, datasource=datasource)
#
#
#     # convert units
#     flows = harmonize_units(flows)
#
#     # if activityname set to default, then compare aggregation for all activities. If looking at particular activity,
#     # filter that activity out
#     if activitynames == ['all']:
#         flow_subset = flows.copy()
#     else:
#         flow_subset = flows[(flows[fba_activity_fields[0]].isin(activitynames)) |
#                             (flows[fba_activity_fields[1]].isin(activitynames))]
#
#     # Reset index values after subset
#     flow_subset = flow_subset.reset_index()
#
#     # pull naics crosswalk
#     mapping = get_activitytosector_mapping(flow_subset['SourceName'].all())
#
#     # assign naics to activities
#     # usgs datasource is not easily assigned to naics for checking totals, so instead standardize activity names
#     if datasource == 'USGS_NWIS_WU':
#         flow_subset = standardize_usgs_nwis_names(flow_subset)
#     else:
#         flow_subset = pd.merge(flow_subset, mapping[['Activity', 'Sector']], left_on='ActivityProducedBy',
#                                right_on='Activity', how='left').rename({'Sector': 'SectorProducedBy'}, axis=1)
#         flow_subset = pd.merge(flow_subset, mapping[['Activity', 'Sector']], left_on='ActivityConsumedBy',
#                                right_on='Activity', how='left').rename({'Sector': 'SectorConsumedBy'}, axis=1)
#     flow_subset = flow_subset.drop(columns=['ActivityProducedBy', 'ActivityConsumedBy', 'Activity_x', 'Activity_y',
#                                             'Description'], errors='ignore')
#     flow_subset['SectorProducedBy'] = flow_subset['SectorProducedBy'].replace({np.nan: None})
#     flow_subset['SectorConsumedBy'] = flow_subset['SectorConsumedBy'].replace({np.nan: None})
#
#     # create list of geoscales for aggregation
#     if to_scale == 'national':
#         geoscales = ['national', 'state', 'county']
#     elif to_scale == 'state':
#         geoscales = ['state', 'county']
#
#     # create empty df list
#     flow_dfs = []
#     for i in geoscales:
#         # test
#         #i = 'state'
#         # filter by geoscale
#         fba_from_scale = filter_by_geoscale(flow_subset, i)
#
#         if fba_from_scale is not None:
#
#             # remove/add column names as a column
#             group_cols = fba_default_grouping_fields.copy()
#             for j in ['Location', 'ActivityProducedBy', 'ActivityConsumedBy']:
#                 group_cols.remove(j)
#             for j in ['SectorProducedBy', 'SectorConsumedBy']:
#                 group_cols.append(j)
#
#             # county sums to state and national, state sums to national
#             if to_scale == 'state':
#                 fba_from_scale['Location'] = fba_from_scale['Location'].apply(lambda x: x[0:2])
#             elif to_scale == 'national':
#                 fba_from_scale['Location'] = US_FIPS
#
#             # aggregate
#             fba_from_scale = fba_from_scale.fillna(0)
#             fba_agg = aggregator(fba_from_scale, group_cols)
#
#             # rename flowamount column, based on geoscale
#             fba_agg = fba_agg.rename(columns={"FlowAmount": "FlowAmount_" + i})
#
#             # drop fields irrelevant to aggregated flow comparision
#             drop_fields = flows[['MeasureofSpread', 'Spread', 'DistributionType', 'DataReliability','DataCollection']]
#             fba_agg = fba_agg.drop(columns=drop_fields)
#
#             # reset index
#             fba_agg = fba_agg.reset_index(drop=True)
#
#             flow_dfs.append(fba_agg)
#
#     # merge list of dfs by column
#     flow_comparison = reduce(lambda left, right: pd.merge(left, right, on=['Class', 'SourceName', 'FlowName', 'Unit',
#                                                                            'SectorProducedBy', 'SectorConsumedBy',
#                                                                            'Compartment', 'Location',
#                                                                            'LocationSystem', 'Year'], how='outer'), flow_dfs)
#
#     # sort df
#     flow_comparison = flow_comparison.sort_values(['Year', 'Location', 'SectorProducedBy', 'SectorConsumedBy',
#                                                    'FlowName', 'Compartment'])
#
#     return flow_comparison



# def sector_flow_comparision(fbs_df):
#     """
#     Function that sums a flowbysector df to 2 digit sectors, from sectors of various lengths. Allows for comparision of
#     sector totals
#
#     :param fbs: A flowbysector df
#     :return:
#     """
#     # testing purposes
#     #fbs_df = flowsa.getFlowBySector(methodname='Water_national_2015_m1', activity="Industrial")
#
#     # grouping columns
#     group_cols = fbs_default_grouping_fields.copy()
#
#     # run  sector aggregation to sum flow amounts to each sector length
#     fbs_agg = sector_aggregation(fbs_df, group_cols)
#     # add missing naics5/6 when only one naics5/6 associated with a naics4
#     fbs_agg = sector_disaggregation(fbs_agg)
#
#     # subset df into four df based on values in sector columns
#     # df 1 where sector produced by = none
#     df1 = fbs_agg.loc[fbs_agg['SectorProducedBy'].isnull()]
#     # df 2 where sector consumed by = none
#     df2 = fbs_agg.loc[fbs_agg['SectorConsumedBy'].isnull()]
#     # df 3 where sector produced by = 221320 (public supply)
#     df3 = fbs_agg.loc[
#         (fbs_agg['SectorProducedBy'].isnull()) & (fbs_agg['SectorConsumedBy'] == '221310')]
#     # df 3 where sector consumed by = 221320 (public supply)
#     df4 = fbs_agg.loc[
#         (fbs_agg['SectorProducedBy'] == '221310') & (fbs_agg['SectorConsumedBy'].isnull())]
#
#     sector_dfs = []
#     for df in (df1, df2, df3, df4):
#         # if the dataframe is not empty, run through sector aggregation code
#         if len(df) != 0:
#             # assign the sector column for aggregation
#             if (df['SectorProducedBy'].all() == 'None') | (
#                     (df['SectorProducedBy'].all() == '221310') & (df['SectorConsumedBy'].all() is None)):
#                 sector = 'SectorConsumedBy'
#             elif (df['SectorConsumedBy'].all() == 'None') | (
#                     (df['SectorConsumedBy'].all() == '221310') & (df['SectorProducedBy'].all() is None)):
#                 sector = 'SectorProducedBy'
#
#             # find max length of sector column
#             df.loc[:, 'SectorLength'] = df[sector].apply(lambda x: len(x))
#
#             # reassign sector consumed/produced by to help wth grouping
#             # assign the sector column for aggregation
#             if df['SectorProducedBy'].all() == 'None':
#                 df.loc[:, 'SectorConsumedBy'] = 'All'
#             elif (df['SectorProducedBy'].all() == '221310') & (df['SectorConsumedBy'].all() != 'None'):
#                 df.loc[:, 'SectorConsumedBy'] = 'All'
#             elif df['SectorConsumedBy'].all() == 'None':
#                 df.loc[:, 'SectorProducedBy'] = 'All'
#             elif (df['SectorConsumedBy'].all() == '221310') & (df['SectorProducedBy'].all() != 'None'):
#                 df.loc[:, 'SectorProducedBy'] = 'All'
#
#             # append to df
#             sector_dfs.append(df)
#
#     # concat and sort df
#     df_agg = pd.concat(sector_dfs, sort=False)
#
#     # sum df based on sector length
#     grouping = fbs_default_grouping_fields.copy()
#     grouping.append('SectorLength')
#     sector_comparison = df_agg.groupby(grouping, as_index=False)[["FlowAmount"]].agg("sum")
#
#     # drop columns not needed for comparison
#     sector_comparison = sector_comparison.drop(columns=['DistributionType', 'MeasureofSpread'])
#
#     # sort df
#     sector_comparison = sector_comparison.sort_values(['Flowable', 'Context', 'FlowType', 'SectorLength'])
#
#     return sector_comparison


# def fba_to_fbs_summation_check(fba_source, fbs_methodname):
#     """
#     Temporary code - need to update
#     :param fba_source:
#     :param fbs_methodname:
#     :return:
#     """
#
#     import flowsa
#
#     # testing
#     # df = fbss.copy()
#     # test1 = df[df['SectorConsumedBy'].isnull()]
#     # test2 = df[df['SectorProducedBy'].isnull()]
#     # test3 = df[df['SectorConsumedBy'] == 'None']
#     # test4 = df[df['SectorProducedBy'] == 'None']
#
#
#
#     fbs_methodname = 'Water_total_2015'
#
#     fbs = flowsa.getFlowBySector(fbs_methodname)
#     fbs_c = flowsa.getFlowBySector_collapsed(fbs_methodname)
#
#     return None


# def flowname_summation_check(df, flownames_to_sum, flownames_published):
#     """
#     Check if rows with specified flowname values sum to another specified flowname (do fresh + saline = total?)
#
#     :param df: fba or fbs
#     :param flownames_to_sum: list of strings
#     :param flownames_published: string
#     :return:
#     """
#
#     return None
