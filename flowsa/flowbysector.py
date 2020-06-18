# flowbysector.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Produces a FlowBySector data frame based on a method file for the given class
"""
import flowsa
import yaml
import numpy as np
import pandas as pd
from flowsa.common import log, flowbyactivitymethodpath, datapath, flow_by_sector_fields, \
    generalize_activity_field_names, get_flow_by_groupby_cols, create_fill_na_dict, fbsoutputpath
from flowsa.mapping import add_sectors_to_flowbyactivity
from flowsa.flowbyactivity import fba_activity_fields, agg_by_geoscale, \
    fba_fill_na_dict, convert_unit, activity_fields, fba_default_grouping_fields, \
    get_fba_allocation_subset, add_missing_flow_by_fields, fbs_activity_fields, aggregator


fbs_fill_na_dict = create_fill_na_dict(flow_by_sector_fields)

fbs_default_grouping_fields = get_flow_by_groupby_cols(flow_by_sector_fields)


def load_method(method_name):
    """
    Loads a flowbysector method from a YAML
    :param method_name:
    :return:
    """
    sfile = flowbyactivitymethodpath+method_name+'.yaml'
    try:
        with open(sfile, 'r') as f:
            method = yaml.safe_load(f)
    except IOError:
        log.error("File not found.")
    return method


def allocate_by_sector(fba_w_sectors, allocation_method):

    # drop any columns that contain a "-" in sector column
    fba_w_sectors = fba_w_sectors[~fba_w_sectors['Sector'].str.contains('-', regex=True)]

    # run sector aggregation fxn to determine total flowamount for each level of sector
    fba_w_sectors = sector_aggregation(fba_w_sectors)

    # if statements for method of allocation
    if allocation_method == 'proportional':
        # denomenator summed from highest level of sector grouped by location
        denom_df = fba_w_sectors.loc[fba_w_sectors['Sector'].apply(lambda x: len(x) == 2)]
        denom_df['Denominator'] = denom_df['FlowAmount'].groupby(denom_df['Location']).transform('sum')
        denom_df = denom_df[['Location', 'LocationSystem', 'Year', 'Denominator']].drop_duplicates()
        # merge the denominator column with fba_w_sector df
        allocation_df = fba_w_sectors.merge(denom_df, how='left')
        # calculate ratio
        allocation_df['FlowAmountRatio'] = allocation_df['FlowAmount'] / allocation_df['Denominator']
        allocation_df = allocation_df.drop(columns=['Denominator']).reset_index()

        return allocation_df


def sector_aggregation(fbs_df):
    """
    Function that checks if a sector aggregation exists, and if not, sums the less aggregated sector
    :param fbs_df: flow by sector dataframe
    :return:
    """

    # group by columns
    group_cols= fba_default_grouping_fields
    group_cols = [e for e in group_cols if e not in ('ActivityProducedBy', 'ActivityConsumedBy')]
    group_cols.append('Sector')

    # find the longest length naics (will be 6 or 8), needs to be integer for for loop
    length = max(fbs_df['Sector'].apply(lambda x: len(x)).unique())
    # for loop in reverse order longest length naics minus 1 to 2
    # appends missing naics levels to df
    for i in range(length-1, 1, -1):
        # subset df to sectors with length = i and length = i + 1
        df_subset = fbs_df.loc[fbs_df['Sector'].apply(lambda x: i + 2 > len(x) >= i)]
        # create a list of i digit sectors in df subset
        sector_list = df_subset['Sector'].apply(lambda x: str(x[0:i])).unique().tolist()
        # create a list of sectors that are exactly i digits long
        existing_sectors = df_subset['Sector'].loc[df_subset['Sector'].apply(lambda x: len(x) == i)].unique().tolist()
        # list of sectors of length i that are not in sector list
        missing_sectors = np.setdiff1d(sector_list, existing_sectors).tolist()
        # add start of symbol to missing list
        missing_sectors = ["^" + e for e in missing_sectors]
        if len(missing_sectors) != 0:
            # new df of sectors that start with missing sectors. drop the last digit of the sector and sum flow amounts
            agg_sectors = df_subset.loc[df_subset['Sector'].str.contains('|'.join(missing_sectors))]
            # only keep data with length greater than i
            agg_sectors = agg_sectors.loc[agg_sectors['Sector'].apply(lambda x: len(x) > i)]
            agg_sectors['Sector'] = agg_sectors['Sector'].apply(lambda x: str(x[0:i]))
            agg_sectors = agg_sectors.fillna(0).reset_index()
            # aggregate the new sector flow amounts
            agg_sectors = aggregator(agg_sectors, group_cols)
            agg_sectors = agg_sectors.fillna(0).reset_index(drop=True)
            # append to df
            fbs_df = fbs_df.append(agg_sectors, sort=True)

    # sort df
    fbs_df = fbs_df.sort_values(['Location', 'Sector'])

    return fbs_df


def store_flowbysector(fbs_df, parquet_name):
    """Prints the data frame into a parquet file."""
    f = fbsoutputpath + parquet_name + '.parquet'
    try:
        fbs_df.to_parquet(f, engine="pyarrow")
    except:
        log.error('Failed to save '+parquet_name + ' file.')


def main(method_name):
    """
    Creates a flowbysector dataset
    :param method_name: Name of method corresponding to flowbysector method yaml name
    :return: flowbysector
    """

    # call on method
    method = load_method(method_name)
    # create dictionary of water data and allocation datasets
    fbas = method['flowbyactivity_sources']
    for k,v in fbas.items():
        # pull water data for allocation
        flows = flowsa.getFlowByActivity(flowclass=[v['class']],
                                         years=[v['year']],
                                         datasource=k)
        # drop description field
        flows = flows.drop(columns='Description')
        # fill null values
        flows = flows.fillna(value=fba_fill_na_dict)
        # convert unit
        flows = convert_unit(flows)

        # create dictionary of allocation datasets for different usgs activities
        activities = v['activity_sets']
        for aset,attr in activities.items():
            # subset by named activities
            names = [attr['names']]
            # subset usgs data by activity
            flow_subset = flows[(flows[fba_activity_fields[0]].isin(names)) |
                          (flows[fba_activity_fields[1]].isin(names))]
            # Reset index values after subset
            flow_subset = flow_subset.reset_index()

            # aggregate geographically to the scale of the allocation dataset
            from_scale = v['geoscale_to_use']
            to_scale = attr['allocation_from_scale']
            # aggregate usgs activity to target scale
            flow_subset = agg_by_geoscale(flow_subset, from_scale, to_scale, fba_default_grouping_fields)
            # rename location column and pad zeros if necessary
            flow_subset = flow_subset.rename(columns={'to_Location': 'Location'})
            flow_subset['Location'] = flow_subset['Location'].apply(lambda x: x.ljust(3 + len(x), '0') if len(x) < 5
                                                                    else x)

            # Add sectors to usgs activity, creating two versions of the flow subset
            # the first version "flow_subset" is the most disaggregated version of the Sectors (NAICS)
            # the second version, "flow_subset_agg" includes only the most aggregated level of sectors
            flow_subset_wsec = add_sectors_to_flowbyactivity(flow_subset, sectorsourcename=method['target_sector_source'])
            flow_subset_wsec_agg = add_sectors_to_flowbyactivity(flow_subset,
                                                                 sectorsourcename=method['target_sector_source'],
                                                                 levelofNAICSagg='agg')

            # if allocation method is "direct", then no need to create allocation ratios, else need to use allocation
            # dataframe to create sector allocation ratios
            if attr['allocation_method'] == 'direct':
                fbs = flow_subset_wsec_agg.copy()
            else:
                # determine appropriate allocation dataset
                fba_allocation = flowsa.getFlowByActivity(flowclass=[attr['allocation_source_class']],
                                                          datasource=attr['allocation_source'],
                                                          years=[attr['allocation_source_year']])

                # fill null values
                fba_allocation = fba_allocation.fillna(value=fba_fill_na_dict)
                # convert unit
                fba_allocation = convert_unit(fba_allocation)

                # assign naics to allocation dataset
                fba_allocation = add_sectors_to_flowbyactivity(fba_allocation,
                                                               sectorsourcename=method['target_sector_source'])
                # subset fba datsets to only keep the naics associated with usgs activity subset
                fba_allocation_subset = get_fba_allocation_subset(fba_allocation, k, names)
                # Reset index values after subset
                fba_allocation_subset = fba_allocation_subset.reset_index(drop=True)
                # generalize activity field names to enable link to water withdrawal table
                fba_allocation_subset = generalize_activity_field_names(fba_allocation_subset)
                # drop activity column
                fba_allocation_subset = fba_allocation_subset.drop(columns=['Activity', 'Description', 'Min', 'Max'])
                # create flow allocation ratios
                flow_allocation = allocate_by_sector(fba_allocation_subset, attr['allocation_method'])

                # create list of sectors in the flow allocation df, drop any rows of data in the flow df that \
                # aren't in list
                sector_list = flow_allocation['Sector'].unique().tolist()
                # subset fba allocation table to the values in the activity list, based on overlapping sectors
                flow_subset_wsec = flow_subset_wsec.loc[(flow_subset_wsec[fbs_activity_fields[0]].isin(sector_list)) |
                                              (flow_subset_wsec[fbs_activity_fields[1]].isin(sector_list))]

                # merge water withdrawal df w/flow allocation dataset
                # todo: modify to recalculate data quality scores

                fbs = flow_subset_wsec.merge(
                    flow_allocation[['Location', 'LocationSystem', 'Sector', 'FlowAmountRatio']],
                    left_on=['Location', 'LocationSystem', 'SectorProducedBy'],
                    right_on=['Location', 'LocationSystem', 'Sector'], how='left')

                fbs = fbs.merge(
                    flow_allocation[['Location', 'LocationSystem',  'Sector', 'FlowAmountRatio']],
                    left_on=['Location', 'LocationSystem', 'SectorConsumedBy'],
                    right_on = ['Location', 'LocationSystem', 'Sector'], how='left')

                # drop columns where both sector produced/consumed by in flow allocation dif is null
                fbs = fbs.dropna(subset=['Sector_x', 'Sector_y'], how='all').reset_index()

                # merge the flowamount columns
                fbs['FlowAmountRatio'] = fbs['FlowAmountRatio_x'].fillna(fbs['FlowAmountRatio_y'])
                fbs['FlowAmountRatio'] = fbs['FlowAmountRatio'].fillna(0)

                # calcuate flow amounts for each sector
                fbs['FlowAmount'] = fbs['FlowAmount'] * fbs['FlowAmountRatio']

                # drop columns
                fbs = fbs.drop(columns=['Sector_x', 'FlowAmountRatio_x', 'Sector_y', 'FlowAmountRatio_y',
                                          'FlowAmountRatio', 'ActivityProducedBy', 'ActivityConsumedBy'])

            # drop rows where flowamount = 0 (although this includes dropping suppressed data)
            fbs = fbs[fbs['FlowAmount'] != 0].reset_index(drop=True)

            # aggregate df geographically
            from_scale = attr['allocation_from_scale']
            to_scale = method['target_geoscale']
            # add missing data columns
            fbs = add_missing_flow_by_fields(fbs, flow_by_sector_fields)
            # fill null values
            fbs = fbs.fillna(value=fbs_fill_na_dict)
            # aggregate usgs activity to target scale
            fbs = agg_by_geoscale(fbs, from_scale, to_scale, fbs_default_grouping_fields)

            # save as parquet file
            parquet_name = method_name + '_' + attr['names']
            store_flowbysector(fbs, parquet_name)

            return fbs



