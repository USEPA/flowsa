# allocation.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Methods of allocating datasets
"""
import pandas as pd
from flowsa import log
from flowsa.common import fbs_activity_fields
from flowsa.dataclean import replace_NoneType_with_empty_cells, replace_strings_with_NoneType
from flowsa.flowbyfunctions import sector_aggregation, sector_disaggregation


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
        return df_w_sectors_nonflagged
    else:
        df1 = sector_aggregation(df_w_sectors, group_cols)
        # run sector disaggregation to capture one-to-one naics4/5/6 relationships
        df2 = sector_disaggregation(df1)

        # if statements for method of allocation
        # either 'proportional' or 'proportional-flagged'
        allocation_df = []
        if allocation_method in ('proportional', 'proportional-flagged'):
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
