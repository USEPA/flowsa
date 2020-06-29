"""
Functions to check data is loaded correctly
"""

import flowsa
from functools import reduce
import numpy as np
import pandas as pd
from flowsa.mapping import get_activitytosector_mapping
from flowsa.flowbyfunctions import fba_fill_na_dict, convert_unit, fba_activity_fields, filter_by_geoscale, \
    fba_default_grouping_fields, fbs_default_grouping_fields, aggregator, sector_aggregation
from flowsa.common import US_FIPS
from flowsa.USGS_NWIS_WU import standardize_usgs_nwis_names


def geoscale_flow_comparison(flowclass, years, datasource, activitynames=['all'], to_scale='national'):
    """ Aggregates county data to state and national, and state data to national level, allowing for comparisons
        in flow totals for a given flowclass and industry. First assigns all flownames to NAICS and standardizes units.

        Assigned to NAICS rather than using FlowNames for aggregation to negate any changes in flownames across
        time/geoscale
    """

    # load parquet file checking aggregation
    flows = flowsa.getFlowByActivity(flowclass=flowclass,
                                     years=years,
                                     datasource=datasource)
    # fill null values
    flows = flows.fillna(value=fba_fill_na_dict)
    # convert units
    flows = convert_unit(flows)

    # if activityname set to default, then compare aggregation for all activities. If looking at particular activity,
    # filter that activity out
    if activitynames == ['all']:
        flow_subset = flows.copy()
    else:
        flow_subset = flows[(flows[fba_activity_fields[0]].isin(activitynames)) |
                            (flows[fba_activity_fields[1]].isin(activitynames))]

    # Reset index values after subset
    flow_subset = flow_subset.reset_index()

    # pull naics crosswalk
    mapping = get_activitytosector_mapping(flow_subset['SourceName'].all())

    # assign naics to activities
    # usgs datasource is not easily assigned to naics for checking totals, so instead standardize activity names
    if datasource == 'USGS_NWIS_WU':
        flow_subset = standardize_usgs_nwis_names(flow_subset)
    else:
        flow_subset = pd.merge(flow_subset, mapping[['Activity', 'Sector']], left_on='ActivityProducedBy',
                               right_on='Activity', how='left').rename({'Sector': 'SectorProducedBy'}, axis=1)
        flow_subset = pd.merge(flow_subset, mapping[['Activity', 'Sector']], left_on='ActivityConsumedBy',
                               right_on='Activity', how='left').rename({'Sector': 'SectorConsumedBy'}, axis=1)
    flow_subset = flow_subset.drop(columns=['ActivityProducedBy', 'ActivityConsumedBy', 'Activity_x', 'Activity_y',
                                            'Description'], errors='ignore')
    flow_subset['SectorProducedBy'] = flow_subset['SectorProducedBy'].replace({np.nan: None}).astype(str)
    flow_subset['SectorConsumedBy'] = flow_subset['SectorConsumedBy'].replace({np.nan: None}).astype(str)

    # create list of geoscales for aggregation
    if to_scale == 'national':
        geoscales = ['national', 'state', 'county']
    elif to_scale == 'state':
        geoscales = ['state', 'county']

    # create empty df list
    flow_dfs = []
    for i in geoscales:
        try:
            # filter by geoscale
            fba_from_scale = filter_by_geoscale(flow_subset, i)

            # remove/add column names as a column
            group_cols = fba_default_grouping_fields.copy()
            for j in ['Location', 'ActivityProducedBy', 'ActivityConsumedBy']:
                group_cols.remove(j)
            for j in ['SectorProducedBy', 'SectorConsumedBy']:
                group_cols.append(j)

            # county sums to state and national, state sums to national
            if to_scale == 'state':
                fba_from_scale['Location'] = fba_from_scale['Location'].apply(lambda x: str(x[0:2]))
            elif to_scale == 'national':
                fba_from_scale['Location'] = US_FIPS

            # aggregate
            fba_agg = aggregator(fba_from_scale, group_cols)

            # rename flowamount column, based on geoscale
            fba_agg = fba_agg.rename(columns={"FlowAmount": "FlowAmount_" + i})

            # drop fields irrelevant to aggregated flow comparision
            drop_fields = flows[['MeasureofSpread', 'Spread', 'DistributionType', 'DataReliability','DataCollection']]
            fba_agg = fba_agg.drop(columns=drop_fields)

            # reset index
            fba_agg = fba_agg.reset_index(drop=True)

            flow_dfs.append(fba_agg)
        except:
            pass

    # merge list of dfs by column
    flow_comparison = reduce(lambda left, right: pd.merge(left, right, on=['Class', 'SourceName', 'FlowName', 'Unit',
                                                                           'SectorProducedBy', 'SectorConsumedBy',
                                                                           'Compartment', 'Location',
                                                                           'LocationSystem', 'Year'], how='outer'), flow_dfs)

    # sort df
    flow_comparison = flow_comparison.sort_values(['Year', 'Location', 'SectorProducedBy', 'SectorConsumedBy',
                                                   'FlowName', 'Compartment'])

    return flow_comparison


def sector_flow_comparision(fbs_df):
    """
    Function that sums a flowbysector df to 2 digit sectors, from sectors of various lengths. Allows for comparision of
    sector totals

    :param fbs: A flowbysector df
    :return:
    """
    # testing purposes
    #fbs_df = flowsa.getFlowBySector(methodname='Water_national_2015_m1', activity="Industrial")

    # grouping columns
    group_cols = fbs_default_grouping_fields.copy()

    # run  sector aggregation to sum flow amounts to each sector length
    fbs_agg = sector_aggregation(fbs_df, group_cols)

    # subset df into four df based on values in sector columns
    # df 1 where sector produced by = none
    df1 = fbs_agg.loc[fbs_agg['SectorProducedBy'] == 'None']
    # df 2 where sector consumed by = none
    df2 = fbs_agg.loc[fbs_agg['SectorConsumedBy'] == 'None']
    # df 3 where sector produced by = 221320 (public supply)
    df3 = fbs_agg.loc[
        (fbs_agg['SectorProducedBy'] != 'None') & (fbs_agg['SectorConsumedBy'] == '221310')]
    # df 3 where sector consumed by = 221320 (public supply)
    df4 = fbs_agg.loc[
        (fbs_agg['SectorProducedBy'] == '221310') & (fbs_agg['SectorConsumedBy'] != 'None')]

    sector_dfs = []
    for df in (df1, df2, df3, df4):
        # if the dataframe is not empty, run through sector aggregation code
        if len(df) != 0:
            # assign the sector column for aggregation
            if (df['SectorProducedBy'].all() == 'None') or (
                    (df['SectorProducedBy'].all() == '221310') & (df['SectorConsumedBy'].all() != 'None')):
                sector = 'SectorConsumedBy'
            elif (df['SectorConsumedBy'].all() == 'None') or (
                    (df['SectorConsumedBy'].all() == '221310') & (df['SectorProducedBy'].all() != 'None')):
                sector = 'SectorProducedBy'

            # find max length of sector column
            df['SectorLength'] = df[sector].apply(lambda x: len(x))

            # reassign sector consumed/produced by to help wth grouping
            # assign the sector column for aggregation
            if df['SectorProducedBy'].all() == 'None':
                df['SectorConsumedBy'] = 'All'
            elif (df['SectorProducedBy'].all() == '221310') & (df['SectorConsumedBy'].all() != 'None'):
                df['SectorConsumedBy'] = 'All'
            elif df['SectorConsumedBy'].all() == 'None':
                df['SectorProducedBy'] = 'All'
            elif (df['SectorConsumedBy'].all() == '221310') & (df['SectorProducedBy'].all() != 'None'):
                df['SectorProducedBy'] = 'All'

            # append to df
            sector_dfs.append(df)

    # concat and sort df
    df_agg = pd.concat(sector_dfs, sort=True)

    # sum df based on sector length
    grouping = fbs_default_grouping_fields.copy()
    grouping.append('SectorLength')
    sector_comparison = df_agg.groupby(grouping, as_index=False)[["FlowAmount"]].agg("sum")

    # drop columns not needed for comparison
    sector_comparison = sector_comparison.drop(columns=['DistributionType', 'MeasureofSpread'])

    # sort df
    sector_comparison = sector_comparison.sort_values(['Flowable', 'Context', 'FlowType', 'SectorLength'])

    return sector_comparison
