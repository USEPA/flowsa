# allocation.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Methods of allocating datasets
"""
import pandas as pd
from flowsa.settings import log
from flowsa.common import fbs_activity_fields, sector_level_key, load_crosswalk
from flowsa.settings import vLogDetailed
from flowsa.dataclean import replace_NoneType_with_empty_cells, \
    replace_strings_with_NoneType
from flowsa.flowbyfunctions import sector_aggregation, \
    sector_disaggregation, subset_and_merge_df_by_sector_lengths


def allocate_by_sector(df_w_sectors, attr, allocation_method,
                       group_cols, **kwargs):
    """
    Create an allocation ratio for df
    :param df_w_sectors: df with column of sectors
    :param attr: dictionary, attributes of activity set
    :param allocation_method: currently written for 'proportional'
         and 'proportional-flagged'
    :param group_cols: columns on which to base aggregation and disaggregation
    :return: df with FlowAmountRatio for each sector
    """

    # first determine if there is a special case with how
    # the allocation ratios are created
    if allocation_method == 'proportional-flagged':
        # if the allocation method is flagged, subset sectors that are
        # flagged/notflagged, where nonflagged sectors have flowamountratio=1
        if kwargs != {}:
            if 'flowSubsetMapped' in kwargs:
                fsm = kwargs['flowSubsetMapped']
                flagged = fsm[fsm['disaggregate_flag'] == 1]
                if flagged['SectorProducedBy'].isna().all():
                    sector_col = 'SectorConsumedBy'
                else:
                    sector_col = 'SectorProducedBy'
                flagged_names = flagged[sector_col].tolist()

                nonflagged = fsm[fsm['disaggregate_flag'] == 0]
                nonflagged_names = nonflagged[sector_col].tolist()

                # subset the original df so rows of data that run through the
                # proportional allocation process are
                # sectors included in the flagged list
                df_w_sectors_nonflagged = df_w_sectors.loc[
                    (df_w_sectors[fbs_activity_fields[0]].isin(
                        nonflagged_names)) |
                    (df_w_sectors[fbs_activity_fields[1]].isin(
                        nonflagged_names))].reset_index(drop=True)
                df_w_sectors_nonflagged = \
                    df_w_sectors_nonflagged.assign(FlowAmountRatio=1)

                df_w_sectors = \
                    df_w_sectors.loc[(df_w_sectors[fbs_activity_fields[0]]
                                      .isin(flagged_names)) |
                                     (df_w_sectors[fbs_activity_fields[1]]
                                      .isin(flagged_names)
                                      )].reset_index(drop=True)
            else:
                log.error('The proportional-flagged allocation '
                          'method requires a column "disaggregate_flag" '
                          'in the flow_subset_mapped df')

    # run sector aggregation fxn to determine total flowamount
    # for each level of sector
    if len(df_w_sectors) == 0:
        return df_w_sectors_nonflagged
    else:
        df1 = sector_aggregation(df_w_sectors)
        # run sector disaggregation to capture one-to-one
        # naics4/5/6 relationships
        df2 = sector_disaggregation(df1)

        # if statements for method of allocation
        # either 'proportional' or 'proportional-flagged'
        allocation_df = []
        if allocation_method in ('proportional', 'proportional-flagged'):
            allocation_df = proportional_allocation(df2, attr)
        else:
            log.error('Must create function for specified '
                      'method of allocation')

        if allocation_method == 'proportional-flagged':
            # drop rows where values are not in flagged names
            allocation_df =\
                allocation_df.loc[(allocation_df[fbs_activity_fields[0]]
                                   .isin(flagged_names)) |
                                  (allocation_df[fbs_activity_fields[1]]
                                   .isin(flagged_names)
                                   )].reset_index(drop=True)
            # concat the flagged and nonflagged dfs
            allocation_df = \
                pd.concat([allocation_df, df_w_sectors_nonflagged],
                          ignore_index=True).sort_values(['SectorProducedBy',
                                                          'SectorConsumedBy'])

        return allocation_df


def proportional_allocation(df, attr):
    """
    Creates a proportional allocation based on all the most
    aggregated sectors within a location
    Ensure that sectors are at 2 digit level - can run sector_aggregation()
    prior to using this function
    :param df: df, includes sector columns
    :param attr: dictionary, attributes for an activity set
    :return: df, with 'FlowAmountRatio' column
    """

    # tmp drop NoneType
    df = replace_NoneType_with_empty_cells(df)

    # determine if any additional columns beyond location and sector by which
    # to base allocation ratios
    if 'allocation_merge_columns' in attr:
        groupby_cols = ['Location'] + attr['allocation_merge_columns']
        denom_subset_cols = ['Location', 'LocationSystem', 'Year',
                             'Denominator'] + attr['allocation_merge_columns']
    else:
        groupby_cols = ['Location']
        denom_subset_cols = ['Location', 'LocationSystem', 'Year',
                             'Denominator']

    cw_load = load_crosswalk('sector_length')
    cw = cw_load['NAICS_2'].drop_duplicates()
    denom_df = df.loc[(df['SectorProducedBy'].isin(cw)) |
                      (df['SectorConsumedBy'].isin(cw))]

    # generate denominator based on identified groupby cols
    denom_df = denom_df.assign(Denominator=denom_df.groupby(
        groupby_cols)['FlowAmount'].transform('sum'))
    # subset select columns by which to generate ratios
    denom_df_2 = denom_df[denom_subset_cols].drop_duplicates()
    # merge the denominator column with fba_w_sector df
    allocation_df = df.merge(denom_df_2, how='left')
    # calculate ratio
    allocation_df.loc[:, 'FlowAmountRatio'] = \
        allocation_df['FlowAmount'] / allocation_df['Denominator']
    allocation_df = allocation_df.drop(columns=['Denominator']).reset_index()

    # add nonetypes
    allocation_df = replace_strings_with_NoneType(allocation_df)

    return allocation_df


def proportional_allocation_by_location_and_activity(df_load, sectorcolumn):
    """
    Creates a proportional allocation within each aggregated
    sector within a location
    :param df_load: df with sector columns
    :param sectorcolumn: str, sector column for which to create
         allocation ratios
    :return: df, with 'FlowAmountRatio' and 'HelperFlow' columns
    """

    # tmp replace NoneTypes with empty cells
    df = replace_NoneType_with_empty_cells(df_load).reset_index(drop=True)

    # want to create denominator based on shortest length naics for each
    # activity/location
    grouping_cols = [e for e in ['FlowName', 'Location', 'Activity',
                                 'ActivityConsumedBy', 'ActivityProducedBy',
                                 'Class', 'SourceName', 'Unit', 'FlowType',
                                 'Compartment', 'Year']
                     if e in df.columns.values.tolist()]
    activity_cols = [e for e in ['Activity', 'ActivityConsumedBy',
                                 'ActivityProducedBy']
                     if e in df.columns.values.tolist()]
    # trim whitespace
    df[sectorcolumn] = df[sectorcolumn].str.strip()
    # to create the denominator dataframe first add a column that captures
    # the sector length
    denom_df = df.assign(sLen=df[sectorcolumn].str.len())
    denom_df = denom_df[denom_df['sLen'] == denom_df.groupby(activity_cols)[
        'sLen'].transform(min)].drop(columns='sLen')
    denom_df.loc[:, 'Denominator'] = \
        denom_df.groupby(grouping_cols)['HelperFlow'].transform('sum')

    # list of column headers, that if exist in df, should be aggregated
    # using the weighted avg fxn
    possible_column_headers = ('Location', 'LocationSystem', 'Year',
                               'Activity', 'ActivityConsumedBy',
                               'ActivityProducedBy')
    # list of column headers that do exist in the df being aggregated
    column_headers = [e for e in possible_column_headers
                      if e in denom_df.columns.values.tolist()]
    merge_headers = column_headers.copy()
    column_headers.append('Denominator')
    # create subset of denominator values based on Locations and Activities
    denom_df_2 = \
        denom_df[column_headers].drop_duplicates().reset_index(drop=True)
    # merge the denominator column with fba_w_sector df
    allocation_df = df.merge(denom_df_2,
                             how='left',
                             left_on=merge_headers,
                             right_on=merge_headers)
    # calculate ratio
    allocation_df.loc[:, 'FlowAmountRatio'] = \
        allocation_df['HelperFlow'] / allocation_df['Denominator']
    allocation_df = allocation_df.drop(
        columns=['Denominator']).reset_index(drop=True)
    # where parent NAICS are not found in the allocation dataset, make sure
    # those child NAICS are not dropped
    allocation_df['FlowAmountRatio'] = \
        allocation_df['FlowAmountRatio'].fillna(1)
    # fill empty cols with NoneType
    allocation_df = replace_strings_with_NoneType(allocation_df)
    # fill na values with 0
    allocation_df['HelperFlow'] = allocation_df['HelperFlow'].fillna(0)

    return allocation_df


def equally_allocate_parent_to_child_naics(df_load, method):
    """
    Determine rows of data that will be lost if subset data at
    target sector level.
    Equally allocate parent NAICS to child NAICS where child NAICS missing
    :param df_load: df, FBS format
    :param target_sector_level: str, target NAICS level for FBS output
    :return: df, with all child NAICS at target sector level
    """
    # determine which sector level to use, use the least aggregated level
    sector_level = method.get('target_sector_level')
    # if secondary sector levels are identified, set the sector level to the
    # least aggregated
    sector_level_list = [sector_level]
    if 'target_subset_sector_level' in method:
        sector_level_dict = method.get('target_subset_sector_level')
        for k, v in sector_level_dict.items():
            sector_level_list = sector_level_list + [k]
        sector_subset_dict = dict((k, sector_level_key[k]) for k in
                                  sector_level_list if k in sector_level_key)
        sector_level = max(sector_subset_dict, key=sector_subset_dict.get)

    # exclude nonsectors
    df = replace_NoneType_with_empty_cells(df_load)

    rows_lost = pd.DataFrame()
    for i in range(2, sector_level_key[sector_level]):
        dfm = subset_and_merge_df_by_sector_lengths(df_load, i, i+1)

        # extract the rows that are not disaggregated to more
        # specific naics
        rl = dfm.query('_merge=="left_only"').drop(
            columns=['_merge', 'SPB_tmp', 'SCB_tmp'])
        rl_list = rl[['SectorProducedBy', 'SectorConsumedBy']]\
            .drop_duplicates().values.tolist()

        # match sectors with target sector length sectors
        # import cw and subset to current sector length
        # and target sector length
        cw_load = load_crosswalk('sector_length')
        nlength = list(sector_level_key.keys()
                       )[list(sector_level_key.values()).index(i)]
        cw = cw_load[[nlength, sector_level]].drop_duplicates()
        # add column with counts
        cw['sector_count'] = \
            cw.groupby(nlength)[nlength].transform('count')

        # merge df & conditionally replace sector produced/consumed columns
        # merge dfs assigning sector length
        sectype_list = ['Produced', 'Consumed']
        for s in sectype_list:
            rl = rl.merge(cw, how='left', left_on=[f'Sector{s}By'],
                          right_on=nlength).rename(
                columns={'sector_count': f'{s}Count'})
            rl[f'Sector{s}By'] = rl[sector_level]
            rl = rl.drop(columns=[sector_level, nlength])

        # create one sector count column, using max value
        rl['sector_count'] = rl[['ProducedCount', 'ConsumedCount']].max(axis=1)
        rl = rl.drop(columns=['ProducedCount', 'ConsumedCount'])

        # calculate new flow amounts, based on sector count,
        # allocating equally to the new sector length codes
        rl['FlowAmount'] = rl['FlowAmount'] / rl['sector_count']
        rl = rl.drop(columns=['sector_count'])

        # append to df
        if len(rl) != 0:
            vLogDetailed.warning('Data found at %s digit NAICS not '
                                 'represented in current data subset: '
                                 '{}'.format(' '.join(map(str, rl_list))),
                                 str(i))
            rows_lost = pd.concat([rows_lost, rl], ignore_index=True)

    if len(rows_lost) != 0:
        vLogDetailed.info('Allocating FlowAmounts equally to '
                          'each %s associated with the sectors previously '
                          'dropped', sector_level)

    # add rows of missing data to the fbs sector subset
    df_w_lost_data = pd.concat([df, rows_lost], ignore_index=True, sort=True)
    df_w_lost_data = replace_strings_with_NoneType(df_w_lost_data)

    return df_w_lost_data


def equal_allocation(fba_load):
    """
    Allocate an Activity in a FBA equally to all mapped sectors.
    Function only works if all mapped sectors are the same length

    :param fba_load: df, FBA with activity columns mapped to sectors
    :return: df, with FlowAmount equally allocated to all mapped sectors
    """
    # create groupby cols by which to determine allocation
    fba_cols = fba_load.select_dtypes([object]).columns.to_list()
    groupcols = [e for e in fba_cols if e not in
                 ['SectorProducedBy', 'SectorConsumedBy', 'Description']]
    # create counts of rows
    df_count = fba_load.groupby(
        groupcols, as_index=False, dropna=False).size().astype(str)
    df_count = replace_strings_with_NoneType(df_count)

    # merge dfs
    dfm = fba_load.merge(df_count, how='left')
    # calc new flowamounts
    dfm['FlowAmount'] = dfm['FlowAmount'] / dfm['size'].astype(int)
    dfm = dfm.drop(columns='size')

    return dfm
