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
    generalize_activity_field_names, get_flow_by_groupby_cols, create_fill_na_dict, outputpath
from flowsa.mapping import add_sectors_to_flowbyactivity
from flowsa.flowbyactivity import fba_activity_fields, agg_by_geoscale, \
    fba_fill_na_dict, convert_unit, activity_fields, fba_default_grouping_fields, \
    get_fba_allocation_subset, add_missing_flow_by_fields


# todo: pull in the R code Mo created to prioritize NAICS codes, so no data is dropped


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
    # if statements for method of allocation

    # # drop coloumns
    # fba_w_sectors = fba_w_sectors.drop(columns=['ActivityProducedBy', 'ActivityConsumedBy', 'Description'])
    # #fba_w_sectors['FlowAmountRatio'] = None
    # # remove/add column names as a column
    # groupbycols = fba_default_grouping_fields.copy()
    # for j in ['ActivityProducedBy', 'ActivityConsumedBy']:
    #     groupbycols.remove(j)
    # # for j in ['SectorProducedBy', 'SectorConsumedBy']:
    # #     groupbycols.append(j)

    if allocation_method == 'proportional':
        fba_w_sectors['FlowAmountRatio'] = fba_w_sectors['FlowAmount'] / fba_w_sectors['FlowAmount'].groupby(
            fba_w_sectors['Location']).transform('sum')
        allocation = fba_w_sectors.copy()
    elif allocation_method == 'direct':
        fba_w_sectors['FlowAmountRatio'] = 1
        allocation = fba_w_sectors.copy()

    return allocation


def store_flowbysector(fbs_df, parquet_name):
    """Prints the data frame into a parquet file."""
    f = outputpath + parquet_name + '.parquet'
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
        print(k)
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
            # create flow allocation ratios
            flow_allocation = allocate_by_sector(fba_allocation_subset, attr['allocation_method'])

            # Add sectors to usgs activity and merge dataframes
            flow_subset = add_sectors_to_flowbyactivity(flow_subset, sectorsourcename=method['target_sector_source'])

            # merge water withdrawal df w/flow allocation datset, first on sectorproduced by, then on sectorconsumedby
            # todo: modify to recalculate data quality scores

            flow = flow_subset.merge(
                flow_allocation[['Location', 'LocationSystem', 'Year', 'Sector', 'FlowAmountRatio']],
                left_on=['Location', 'LocationSystem', 'Year', 'SectorProducedBy'],
                right_on=['Location', 'LocationSystem', 'Year', 'Sector'], how='left')

            flow = flow.merge(
                flow_allocation[['Location', 'LocationSystem', 'Year', 'Sector', 'FlowAmountRatio']],
                left_on=['Location', 'LocationSystem', 'Year', 'SectorConsumedBy'],
                right_on = ['Location', 'LocationSystem', 'Year', 'Sector'], how='left')

            # merge the flowamount columns
            flow['FlowAmountRatio'] = flow['FlowAmountRatio_x'].fillna(flow['FlowAmountRatio_y'])
            flow['FlowAmountRatio'] = flow['FlowAmountRatio'].fillna(0)

            # calcuate flow amounts for each sector
            flow['FlowAmount'] = flow['FlowAmount'] * flow['FlowAmountRatio']

            # drop columns
            flow = flow.drop(columns=['Sector_x', 'FlowAmountRatio_x', 'Sector_y', 'FlowAmountRatio_y',
                                      'FlowAmountRatio', 'ActivityProducedBy', 'ActivityConsumedBy'])

            # drop rows where flowamount = 0
            flow = flow[flow['FlowAmount'] != 0].reset_index(drop=True)

            # aggregate df geographically
            from_scale = attr['allocation_from_scale']
            to_scale = method['target_geoscale']
            # add missing data columns
            flow = add_missing_flow_by_fields(flow, flow_by_sector_fields)
            # fill null values
            flow = flow.fillna(value=fbs_fill_na_dict)
            # aggregate usgs activity to target scale
            flow_agg = agg_by_geoscale(flow, from_scale, to_scale, fbs_default_grouping_fields)

            # save as parquet file
            # parquet_name = 'FBS_' + str(k) + '_' + attr['names'] + '_' + str(v['year'])
            # store_flowbysector(flow_agg, parquet_name)

    return flow_agg



