"""
Functions to check data is loaded correctly
"""

import pandas as pd
from flowsa.flowbyfunctions import fba_fill_na_dict, harmonize_units, fba_activity_fields, filter_by_geoscale, \
    fba_default_grouping_fields, fbs_default_grouping_fields, aggregator, sector_aggregation, fbs_fill_na_dict, \
    fbs_activity_fields, clean_df, create_geoscale_list, sector_disaggregation, replace_strings_with_NoneType, \
    replace_NoneType_with_empty_cells
from flowsa.common import US_FIPS, sector_level_key, flow_by_sector_fields, load_sector_length_crosswalk, \
    load_sector_crosswalk, sector_source_name, log, fips_number_key, outputpath, activity_fields


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
            "No flows found for " + ', '.join(activity_list) + " at the " + geoscale + " scale")
        exists = "No"
    else:
        log.info("Flows found for " + ', '.join(activity_list) + " at the " + geoscale + " scale")
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
            log.info("No flows found for " + activityname + "  at the state scale")
            fips = create_geoscale_list(df, 'county')
            df = df[df['Location'].isin(fips)]
            if len(df) == 0:
                log.info("No flows found for " + activityname + "  at the county scale")
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
            log.info("No flows found for " + activityname + "  at the county scale")
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
        cw_load = load_sector_length_crosswalk()
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
    fba = fba_load[['Class', 'MetaSources', 'Flowable', 'Unit', 'FlowType', 'ActivityProducedBy',
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
    log.info('Save the comparison of FlowByActivity load to FlowBySector ratios for ' +
              activity_set + ' in output folder')
    # output data at all sector lengths
    df_merge.to_csv(outputpath + "FlowBySectorMethodAnalysis/" + method_name + '_' + source_name +
                                "_FBA_load_to_FBS_comparison_" + activity_set + ".csv", index=False)

    return None


def compare_fba_load_and_fbs_output_totals(fba_load, fbs_load, activity_set, source_name, method_name, attr, method, mapping_files):
    """
    Function to compare the loaded flowbyactivity total with the final flowbysector output total
    :param df:
    :return:
    """

    from flowsa.flowbyfunctions import subset_df_by_geoscale, sector_aggregation
    from flowsa.common import load_source_catalog
    from flowsa.mapping import map_elementary_flows


    log.info('Comparing loaded FlowByActivity FlowAmount total to subset FlowBySector FlowAmount total')

    # load source catalog
    cat = load_source_catalog()
    src_info = cat[source_name]

    # extract relevant geoscale data or aggregate existing data
    fba = subset_df_by_geoscale(fba_load, attr['allocation_from_scale'], method['target_geoscale'])
    # map loaded fba
    fba = map_elementary_flows(fba, mapping_files, keep_unmapped_rows=True)
    if src_info['sector-like_activities']:
        # if activities are sector-like, run sector aggregation and then subset df to only keep NAICS2
        fba = fba[['Class', 'FlowAmount', 'Unit', 'Context', 'ActivityProducedBy', 'ActivityConsumedBy', 'Location', 'LocationSystem']]
        # rename the activity cols to sector cols for purposes of aggregation
        fba = fba.rename(columns={'ActivityProducedBy': 'SectorProducedBy',
                                    'ActivityConsumedBy': 'SectorConsumedBy'})
        group_cols_agg = ['Class', 'Context', 'Unit', 'Location', 'LocationSystem', 'SectorProducedBy', 'SectorConsumedBy']
        fba = sector_aggregation(fba, group_cols_agg)
        # subset fba to only include NAICS2
        fba = replace_NoneType_with_empty_cells(fba)
        fba = fba[fba['SectorConsumedBy'].apply(lambda x: len(x) == 2) |
                  fba['SectorProducedBy'].apply(lambda x: len(x) == 2)]
    # subset/agg dfs
    col_subset = ['Class', 'FlowAmount', 'Unit', 'Context', 'Location', 'LocationSystem']
    group_cols = ['Class', 'Unit', 'Context', 'Location', 'LocationSystem']
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
        df_merge['FlowAmount_difference'] = df_merge['FBA_amount'] - df_merge['FBS_amount']
        df_merge['Percent_difference'] = (df_merge['FlowAmount_difference']/df_merge['FBA_amount']) * 100

        # reorder
        df_merge = df_merge[['Class', 'Context', 'Location', 'LocationSystem', 'FBA_amount', 'FBA_unit',
                             'FBS_amount', 'FBS_unit', 'FlowAmount_difference', 'Percent_difference']]
        df_merge = replace_NoneType_with_empty_cells(df_merge)

        # list of contexts
        context_list = df_merge['Context'].to_list()

        # loop through the contexts and print results of comparison
        for i in context_list:
            df_merge_subset = df_merge[df_merge['Context'] == i].reset_index(drop=True)
            diff_per = df_merge_subset['Percent_difference'][0]
            # make reporting more manageable
            if abs(diff_per) > 0.001:
                diff_per = round(diff_per, 2)
            else:
                diff_per = round(diff_per, 6)

            diff_units = df_merge_subset['FBS_unit'][0]
            if diff_per > 0:
                log.info('The total FlowBySector FlowAmount for ' + source_name + ' ' + activity_set +
                         ' ' + i + ' is ' + str(abs(diff_per)) + '% less than the total FlowByActivity FlowAmount')
            else:
                log.info('The total FlowBySector FlowAmount for ' + source_name + ' ' + activity_set +
                         ' ' + i + ' is ' + str(abs(diff_per)) + '% more than the total FlowByActivity FlowAmount')

        # save csv to output folder
        log.info('Save the comparison of FlowByActivity load to FlowBySector total FlowAmounts for ' +
                  activity_set + ' in output folder')
        # output data at all sector lengths
        df_merge.to_csv(outputpath + "FlowBySectorMethodAnalysis/" + method_name + '_' + source_name +
                        "_FBA_total_to_FBS_total_FlowAmount_comparison_" + activity_set + ".csv", index=False)

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


def check_for_nonetypes_in_sector_col(df):
    """
    Check for NoneType in columns where datatype = string
    :param df: df with columns where datatype = object
    :return: warning message if there are NoneTypes
    """
    # if datatypes are strings, return warning message
    if df['Sector'].isnull().any():
        log.warning("There are NoneType values in the 'Sector' column")
    return df


def check_for_negative_flowamounts(df):

    if (df['FlowAmount'].values < 0).any():
        log.warning('There are negative FlowAmounts')

    return df


def check_if_sectors_are_naics(df, crosswalk_list, column_headers):
    """
    Check if activity-like sectors are in fact sectors. Also works for the Sector column
    :return:
    """

    # create a df of non-sectors to export
    non_sectors_df = []
    # create a df of just the non-sectors column
    non_sectors_list = []
    # loop through the df headers and determine if value is not in crosswalk list
    for c in column_headers:
        # create df where sectors do not exist in master crosswalk
        non_sectors = df[~df[c].isin(crosswalk_list)]
        # drop rows where c is empty
        non_sectors = non_sectors[non_sectors[c] != '']
        # subset to just the sector column
        if len(non_sectors) != 0:
            sectors = non_sectors[[c]].rename(columns={c: 'NonSectors'})
            non_sectors_df.append(non_sectors)
            non_sectors_list.append(sectors)

    if len(non_sectors_df) != 0:
        # concat the df and the df of sectors
        # ns_df = pd.concat(non_sectors_df, sort=False, ignore_index=True)
        ns_list = pd.concat(non_sectors_list, sort=False, ignore_index=True)
        # print the NonSectors
        non_sectors = ns_list['NonSectors'].drop_duplicates().tolist()
        log.info('There are sectors that are not NAICS 2012 Codes')
        print(non_sectors)
    else:
        log.info('All sectors are NAICS 2012 Codes')

    return non_sectors_df


def melt_naics_crosswalk():
    """
    Create a melt version of the naics 07 to 17 crosswalk to map naics to naics 2012
    :return:
    """

    # load the mastercroswalk and subset by sectorsourcename, save values to list
    cw_load = load_sector_crosswalk()

    # create melt table of possible 2007 and 2017 naics that can be mapped to 2012
    cw_melt = cw_load.melt(id_vars='NAICS_2012_Code', var_name='NAICS_year', value_name='NAICS')
    # drop the naics year because not relevant for replacement purposes
    cw_replacement = cw_melt.dropna(how='any')
    cw_replacement = cw_replacement[['NAICS_2012_Code', 'NAICS']].drop_duplicates()
    # drop rows where contents are equal
    cw_replacement = cw_replacement[cw_replacement['NAICS_2012_Code'] != cw_replacement['NAICS']]
    # drop rows where length > 6
    cw_replacement = cw_replacement[cw_replacement['NAICS_2012_Code'].apply(lambda x: len(x) < 7)].reset_index(drop=True)
    # order by naics 2012
    cw_replacement = cw_replacement.sort_values(['NAICS', 'NAICS_2012_Code']).reset_index(drop=True)


    # create allocation ratios by determining number of NAICS 2012 to other naics when not a 1:1 ratio
    cw_replacement_2 = cw_replacement.assign(naics_count=cw_replacement.groupby(['NAICS'])['NAICS_2012_Code'].transform('count'))
    cw_replacement_2 = cw_replacement_2.assign(allocation_ratio=1/cw_replacement_2['naics_count'])

    return cw_replacement_2



def replace_naics_w_naics_2012(df, sectorsourcename):
    """
    Check if activity-like sectors are in fact sectors. Also works for the Sector column
    :return:
    """
    # test
    # df = mapping.copy()
    # drop NoneType
    df = replace_NoneType_with_empty_cells(df)

    # load the mastercroswalk and subset by sectorsourcename, save values to list
    cw_load = load_sector_crosswalk()
    cw = cw_load[sectorsourcename].drop_duplicates().tolist()

    # load melted crosswalk
    cw_melt = melt_naics_crosswalk()
    # drop the count column
    cw_melt = cw_melt.drop(columns='naics_count')

    # determine which headers are in the df
    possible_column_headers = ['Sector', 'SectorProducedBy', 'SectorConsumedBy']
    # # list of column headers that do exist in the df being aggregated
    column_headers = [e for e in possible_column_headers if e in df.columns.values.tolist()]

    # check if there are any sectors that are not in the naics 2012 crosswalk
    non_naics2012 = check_if_sectors_are_naics(df, cw, column_headers)

    # loop through the df headers and determine if value is not in crosswalk list
    if len(non_naics2012) != 0:
        log.info('Checking if sectors represent a different NAICS year, if so, replace with NAICS 2012')
        for c in column_headers:
            # merge df with the melted sector crosswalk
            df = df.merge(cw_melt, left_on=c, right_on='NAICS', how='left')
            # if there is a value in the 'NAICS_2012_Code' column, use that value to replace sector in column c
            df.loc[df[c] == df['NAICS'], c] = df['NAICS_2012_Code']
            # multiply the FlowAmount col by allocation_ratio
            df.loc[df[c] == df['NAICS_2012_Code'], 'FlowAmount'] = df['FlowAmount'] * df['allocation_ratio']
            # drop columns
            df = df.drop(columns=['NAICS_2012_Code', 'NAICS', 'allocation_ratio'])
        log.info('Replaced NAICS with NAICS 2012 Codes')

        # check if there are any sectors that are not in the naics 2012 crosswalk
        log.info('Check again for non NAICS 2012 Codes')
        check_if_sectors_are_naics(df, cw, column_headers)

    else:
        log.info('No sectors require substitution')

    return df


def compare_remote_to_local_FBS_parquet(DataCommonsParquetName, LocalParquetName):
    """
    Compare a parquet on Data Commons to a parquet stored locally
    :param DataCommonsParquetName:
    :param LocalParquetName:
    :param FileFormat: Either 'FlowByActivity' or 'FlowBySector'
    :return:
    """
    import flowsa
    from flowsa.flowbyfunctions import dataframe_difference

    # test
    # DataCommonsParquetName = 'Water_national_2015_m1'
    # LocalParquetName = 'Water_national_2015_m1'

    # load remote file
    df_remote = flowsa.getFlowBySector(DataCommonsParquetName, file_location='remote')
    # load local file
    df_local = flowsa.getFlowBySector((LocalParquetName))
    # compare df
    df_diff = dataframe_difference(df_remote, df_local)
    # if no differences, print, if differences, provide df subset
    if len(df_diff) == 0:
        log.info('No differences between dataframes')
    else:
        log.info('Differences exist between dataframes')
        df_diff = df_diff.sort_values(['Location', 'SectorProducedBy', 'SectorConsumedBy', 'Flowable',
                                       'Context', ]).reset_index(drop=True)

    return df_diff
