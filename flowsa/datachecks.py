"""
Functions to check data is loaded correctly
"""

import flowsa
from functools import reduce
import numpy as np
import pandas as pd
from flowsa.mapping import get_activitytosector_mapping
from flowsa.flowbyactivity import fba_fill_na_dict, convert_unit, fba_activity_fields, filter_by_geoscale, \
    fba_default_grouping_fields, aggregator
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
                fba_from_scale['to_Location'] = fba_from_scale['Location'].apply(lambda x: str(x[0:2]))
            elif to_scale == 'national':
                fba_from_scale['to_Location'] = US_FIPS
            group_cols.append('to_Location')

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
                                                                           'Compartment', 'to_Location',
                                                                           'LocationSystem', 'Year'], how='outer'), flow_dfs)

    # sort df
    flow_comparison = flow_comparison.sort_values(['Year', 'to_Location', 'SectorProducedBy', 'SectorConsumedBy',
                                                   'FlowName', 'Compartment'])

    return flow_comparison