# flowbysector.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Produces a FlowBySector data frame based on a method file for the given class

To run code, specify the "Run/Debug Configurations" Parameters to the "flowsa/data/flowbysectormethods" yaml file name
you want to use.

Example: "Parameters: --m Water_national_2015_m1"

Files necessary to run FBS:
a. a method yaml in "flowsa/data/flowbysectormethods"
b. crosswalk(s) for the main dataset you are allocating and any datasets used to allocate to sectors
c. a .py file in "flowsa/" for the main dataset you are allocating if you need functions to clean up the FBA
   before allocating to FBS

"""

import flowsa
import yaml
import argparse
import sys
import pandas as pd
from flowsa.common import log, flowbyactivitymethodpath, flow_by_sector_fields, load_household_sector_codes, \
    generalize_activity_field_names, fbsoutputpath, fips_number_key, load_sector_length_crosswalk, \
    flow_by_activity_fields
from flowsa.mapping import add_sectors_to_flowbyactivity, get_fba_allocation_subset, map_elementary_flows, \
    get_sector_list, add_non_naics_sectors
from flowsa.flowbyfunctions import fba_activity_fields, fbs_default_grouping_fields, agg_by_geoscale, \
    fba_fill_na_dict, fbs_fill_na_dict, harmonize_units, fba_default_grouping_fields, \
    add_missing_flow_by_fields, fbs_activity_fields, allocate_by_sector, allocation_helper, sector_aggregation, \
    filter_by_geoscale, aggregator, check_if_data_exists_at_geoscale, check_if_location_systems_match, \
    check_if_data_exists_at_less_aggregated_geoscale, check_if_data_exists_for_same_geoscales
from flowsa.USGS_NWIS_WU import usgs_fba_data_cleanup, usgs_fba_w_sectors_data_cleanup
from flowsa.datachecks import sector_flow_comparision


def parse_args():
    """Make year and source script parameters"""
    ap = argparse.ArgumentParser()
    ap.add_argument("-m", "--method", required=True, help="Method for flow by sector file. A valid method config file must exist with this name.")
    args = vars(ap.parse_args())
    return args


def load_method(method_name):
    """
    Loads a flowbysector method from a YAML
    :param method_name:
    :return:
    """
    sfile = flowbyactivitymethodpath + method_name + '.yaml'
    try:
        with open(sfile, 'r') as f:
            method = yaml.safe_load(f)
    except IOError:
        log.error("File not found.")
    return method


def store_flowbysector(fbs_df, parquet_name):
    """Prints the data frame into a parquet file."""
    f = fbsoutputpath + parquet_name + '.parquet'
    try:
        fbs_df.to_parquet(f)
    except:
        log.error('Failed to save ' + parquet_name + ' file.')


def main(method_name):
    """
    Creates a flowbysector dataset
    :param method_name: Name of method corresponding to flowbysector method yaml name
    :return: flowbysector
    """

    log.info("Initiating flowbysector creation for " + method_name)
    # call on method
    method = load_method(method_name)
    # create dictionary of data and allocation datasets
    fbas = method['flowbyactivity_sources']
    # Create empty list for storing fbs files
    fbss = []
    for k, v in fbas.items():
        # pull fba data for allocation
        log.info("Retrieving flowbyactivity for datasource " + k + " in year " + str(v['year']))
        flows = flowsa.getFlowByActivity(flowclass=[v['class']],
                                         years=[v['year']],
                                         datasource=k)

        # clean up fba, if specified in yaml
        if v["clean_fba_df_fxn"] != 'None':
            log.info("Cleaning up " + k + " FlowByActivity")
            flows = getattr(sys.modules[__name__], v["clean_fba_df_fxn"])(flows)

        # ensure datatypes correct
        flows = add_missing_flow_by_fields(flows, flow_by_activity_fields)

        # drop description field
        flows = flows.drop(columns='Description')
        # fill null values
        flows = flows.fillna(value=fba_fill_na_dict)

        # create dictionary of allocation datasets for different activities
        activities = v['activity_sets']
        # subset activity data and allocate to sector
        for aset, attr in activities.items():
            # subset by named activities
            names = attr['names']
            log.info("Preparing to handle subset of flownames " + ', '.join(map(str, names)) + " in " + k)

            # check if flowbyactivity data exists at specified geoscale to use
            flow_subset_list = []
            for n in names:
                # subset usgs data by activity
                flow_subset = flows[(flows[fba_activity_fields[0]] == n) |
                                    (flows[fba_activity_fields[1]] == n)].reset_index(drop=True)
                log.info("Checking if flowbyactivity data exists for " + n + " at the " +
                         v['geoscale_to_use'] + ' level')
                geocheck = check_if_data_exists_at_geoscale(flow_subset, v['geoscale_to_use'], activitynames=n)
                # aggregate geographically to the scale of the allocation dataset
                if geocheck == "Yes":
                    activity_from_scale = v['geoscale_to_use']
                else:
                    # if activity does not exist at specified geoscale, issue warning and use data at less aggregated
                    # geoscale, and sum to specified geoscale
                    log.info("Checking if flowbyactivity data exists for " + n + " at a less aggregated level")
                    activity_from_scale = check_if_data_exists_at_less_aggregated_geoscale(flow_subset,
                                                                                           v['geoscale_to_use'], n)

                activity_to_scale = attr['allocation_from_scale']
                # if df is less aggregated than allocation df, aggregate usgs activity to allocation geoscale
                if fips_number_key[activity_from_scale] > fips_number_key[activity_to_scale]:
                    log.info("Aggregating subset from " + activity_from_scale + " to " + activity_to_scale)
                    flow_subset = agg_by_geoscale(flow_subset, activity_from_scale, activity_to_scale,
                                                  fba_default_grouping_fields, n)
                # else, aggregate to geoscale want to use
                elif fips_number_key[activity_from_scale] > fips_number_key[v['geoscale_to_use']]:
                    log.info("Aggregating subset from " + activity_from_scale + " to " + v['geoscale_to_use'])
                    flow_subset = agg_by_geoscale(flow_subset, activity_from_scale, v['geoscale_to_use'],
                                                  fba_default_grouping_fields, n)
                # else, if usgs is more aggregated than allocation table, filter relevant rows
                else:
                    log.info("Subsetting " + activity_from_scale + " data")
                    flow_subset = filter_by_geoscale(flow_subset, activity_from_scale, n)

                flow_subset_list.append(flow_subset)
            flow_subset = pd.concat(flow_subset_list, sort=False).reset_index(drop=True)

            # location column pad zeros if necessary
            flow_subset.loc[:, 'Location'] = flow_subset['Location'].apply(lambda x: x.ljust(3 + len(x), '0') if len(x) < 5 else x)

            # Add sectors to df activity, depending on level of specified sector aggregation
            log.info("Adding sectors to " + k + " for " + ', '.join(map(str, names)))
            flow_subset_wsec = add_sectors_to_flowbyactivity(flow_subset,
                                                             sectorsourcename=method['target_sector_source'],
                                                             levelofSectoragg=attr['activity_sector_aggregation'])

            # clean up fba with sectors, if specified in yaml
            if v["clean_fba_w_sec_df_fxn"] != 'None':
                log.info("Cleaning up " + k + " FlowByActivity with sectors")
                flow_subset_wsec = getattr(sys.modules[__name__], v["clean_fba_w_sec_df_fxn"])(flow_subset_wsec, attr)

            # map df to elementary flows - commented out until mapping complete
            log.info("Mapping flows in " + k + ' to federal elementary flow list')
            flow_subset_wsec = map_elementary_flows(flow_subset_wsec, k)

            # if allocation method is "direct", then no need to create alloc ratios, else need to use allocation
            # dataframe to create sector allocation ratios
            if attr['allocation_method'] == 'direct':
                log.info('Directly assigning ' + ', '.join(map(str, names)) + ' to sectors')
                fbs = flow_subset_wsec.copy()

            else:
                # determine appropriate allocation dataset
                log.info("Loading allocation flowbyactivity " + attr['allocation_source'] + " for year " + str(attr['allocation_source_year']))
                fba_allocation = flowsa.getFlowByActivity(flowclass=[attr['allocation_source_class']],
                                                          datasource=attr['allocation_source'],
                                                          years=[attr['allocation_source_year']]).reset_index(drop=True)

                # ensure correct data types
                fba_allocation = add_missing_flow_by_fields(fba_allocation, flow_by_activity_fields)
                # drop description field
                fba_allocation = fba_allocation.drop(columns='Description')

                # fill null values
                fba_allocation = fba_allocation.fillna(value=fba_fill_na_dict)
                # harmonize units across dfs
                fba_allocation = harmonize_units(fba_allocation)

                # subset based on yaml settings
                if attr['allocation_flow'] != 'None':
                    fba_allocation = fba_allocation.loc[fba_allocation['FlowName'].isin(attr['allocation_flow'])]
                if attr['allocation_compartment'] != 'None':
                    fba_allocation = fba_allocation.loc[
                        fba_allocation['Compartment'].isin(attr['allocation_compartment'])]
                # reset index
                fba_allocation = fba_allocation.reset_index(drop=True)

                # check if allocation data exists at specified geoscale to use
                log.info("Checking if allocation data exists at the " + attr['allocation_from_scale'] + " level")
                check_if_data_exists_at_geoscale(fba_allocation, attr['allocation_from_scale'])

                # aggregate geographically to the scale of the flowbyactivty source, if necessary
                from_scale = attr['allocation_from_scale']
                to_scale = v['geoscale_to_use']
                # if allocation df is less aggregated than FBA df, aggregate allocation df to target scale
                if fips_number_key[from_scale] > fips_number_key[to_scale]:
                    fba_allocation = agg_by_geoscale(fba_allocation, from_scale, to_scale, fba_default_grouping_fields, names)
                # else, if usgs is more aggregated than allocation table, use usgs as both to and from scale
                else:
                    fba_allocation = filter_by_geoscale(fba_allocation, from_scale, names)

                # assign sector to allocation dataset
                log.info("Adding sectors to " + attr['allocation_source'])
                fba_allocation = add_sectors_to_flowbyactivity(fba_allocation,
                                                               sectorsourcename=method['target_sector_source'],
                                                               levelofSectoragg=attr['allocation_sector_aggregation'])
                # subset fba datsets to only keep the sectors associated with activity subset
                log.info("Subsetting " + attr['allocation_source'] + " for sectors in " + k)
                fba_allocation_subset = get_fba_allocation_subset(fba_allocation, k, names)

                # generalize activity field names to enable link to main fba source
                log.info("Generalizing activity columns in subset of " + attr['allocation_source'])
                fba_allocation_subset = generalize_activity_field_names(fba_allocation_subset)
                # drop columns
                fba_allocation_subset = fba_allocation_subset.drop(columns=['Activity'])

                # if there is an allocation helper dataset, modify allocation df
                if attr['allocation_helper'] == 'yes':
                    log.info("Using the specified allocation help for subset of " + attr['allocation_source'])
                    fba_allocation_subset = allocation_helper(fba_allocation_subset, method, attr)

                # create flow allocation ratios
                log.info("Creating allocation ratios for " + attr['allocation_source'])
                flow_allocation = allocate_by_sector(fba_allocation_subset, attr['allocation_method'])

                # create list of sectors in the flow allocation df, drop any rows of data in the flow df that \
                # aren't in list
                sector_list = flow_allocation['Sector'].unique().tolist()

                # subset fba allocation table to the values in the activity list, based on overlapping sectors
                flow_subset_wsec = flow_subset_wsec.loc[
                    (flow_subset_wsec[fbs_activity_fields[0]].isin(sector_list)) |
                    (flow_subset_wsec[fbs_activity_fields[1]].isin(sector_list))]

                # check if fba and allocation dfs have the same LocationSystem
                log.info("Checking if flowbyactivity and allocation dataframes use the same location systems")
                check_if_location_systems_match(flow_subset_wsec, flow_allocation)

                # merge fba df w/flow allocation dataset
                log.info("Merge " + k + " and subset of " + attr['allocation_source'])
                fbs = flow_subset_wsec.merge(
                    flow_allocation[['Location', 'Sector', 'FlowAmountRatio']],
                    left_on=['Location', 'SectorProducedBy'],
                    right_on=['Location', 'Sector'], how='left')

                fbs = fbs.merge(
                    flow_allocation[['Location', 'Sector', 'FlowAmountRatio']],
                    left_on=['Location', 'SectorConsumedBy'],
                    right_on=['Location', 'Sector'], how='left')

                # merge the flowamount columns
                fbs.loc[:, 'FlowAmountRatio'] = fbs['FlowAmountRatio_x'].fillna(fbs['FlowAmountRatio_y'])

                # check if fba and allocation dfs have data for same geoscales - comment back in after address the 'todo'
                # log.info("Checking if flowbyactivity and allocation dataframes have data at the same locations")
                # check_if_data_exists_for_same_geoscales(fbs, k, attr['names'])

                # drop rows where there is no allocation data
                fbs = fbs.dropna(subset=['Sector_x', 'Sector_y'], how='all').reset_index()

                # calculate flow amounts for each sector
                log.info("Calculating new flow amounts using flow ratios")
                fbs.loc[:, 'FlowAmount'] = fbs['FlowAmount'] * fbs['FlowAmountRatio']

                # drop columns
                log.info("Cleaning up new flow by sector")
                fbs = fbs.drop(columns=['Sector_x', 'FlowAmountRatio_x', 'Sector_y', 'FlowAmountRatio_y',
                                        'FlowAmountRatio', 'ActivityProducedBy', 'ActivityConsumedBy'])

            # drop rows where flowamount = 0 (although this includes dropping suppressed data)
            fbs = fbs[fbs['FlowAmount'] != 0].reset_index(drop=True)

            # add missing data columns
            fbs = add_missing_flow_by_fields(fbs, flow_by_sector_fields)
            # fill null values
            fbs = fbs.fillna(value=fbs_fill_na_dict)

            # aggregate df geographically, if necessary
            log.info("Aggregating flowbysector to " + method['target_geoscale'] + " level")
            if fips_number_key[v['geoscale_to_use']] < fips_number_key[attr['allocation_from_scale']]:
                from_scale = v['geoscale_to_use']
            else:
                from_scale = attr['allocation_from_scale']

            to_scale = method['target_geoscale']

            fbs = agg_by_geoscale(fbs, from_scale, to_scale, fbs_default_grouping_fields, names)

            # aggregate data to every sector level
            log.info("Aggregating flowbysector to all sector levels")
            fbs = sector_aggregation(fbs, fbs_default_grouping_fields)

            # test agg by sector
            # sector_agg_comparison = sector_flow_comparision(fbs)

            # return sector level specified in method yaml
            # load the crosswalk linking sector lengths
            sector_list = get_sector_list(method['target_sector_level'])
            # add any non-NAICS sectors used with NAICS
            sector_list = add_non_naics_sectors(sector_list, method['target_sector_level'])

            # subset df, necessary because not all of the sectors are NAICS and can get duplicate rows
            fbs = fbs.loc[(fbs[fbs_activity_fields[0]].isin(sector_list)) &
                          (fbs[fbs_activity_fields[1]].isin(sector_list))].reset_index(drop=True)

            log.info("Completed flowbysector for activity subset with flows " + ', '.join(map(str, names)))
            fbss.append(fbs)
    # create single df of all activities
    log.info("Concat data for all activities")
    fbss = pd.concat(fbss, ignore_index=True, sort=False)
    log.info("Clean final dataframe")
    # aggregate df as activities might have data for the same specified sector length
    fbss = aggregator(fbss, fbs_default_grouping_fields)
    # sort df
    log.info("Sort and store dataframe")
    # add missing fields, ensure correct data type, reorder columns
    fbss = add_missing_flow_by_fields(fbss, flow_by_sector_fields)
    fbss = fbss.sort_values(
        ['SectorProducedBy', 'SectorConsumedBy', 'Flowable', 'Context']).reset_index(drop=True)
    # save parquet file
    store_flowbysector(fbss, method_name)


if __name__ == '__main__':
    # assign arguments
    args = parse_args()
    main(args["method"])

