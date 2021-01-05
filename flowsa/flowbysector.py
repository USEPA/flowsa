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
from flowsa.common import log, flowbysectormethodpath, flow_by_sector_fields, \
    fbsoutputpath, fips_number_key, flow_by_activity_fields, load_source_catalog, \
    flowbysectoractivitysetspath, flow_by_sector_fields_w_activity
from flowsa.mapping import add_sectors_to_flowbyactivity, get_fba_allocation_subset, map_elementary_flows, \
    get_sector_list
from flowsa.flowbyfunctions import fba_activity_fields, fbs_default_grouping_fields, fba_mapped_default_grouping_fields, agg_by_geoscale, \
    fba_fill_na_dict, fbs_fill_na_dict, fba_default_grouping_fields, harmonize_units, \
    fbs_activity_fields, allocate_by_sector, allocation_helper, sector_aggregation, \
    filter_by_geoscale, aggregator, clean_df, subset_df_by_geoscale, \
    sector_disaggregation, return_activity_from_scale, fbs_grouping_fields_w_activities, collapse_activity_fields
from flowsa.datachecks import check_if_losing_sector_data, check_if_data_exists_at_geoscale, \
    check_if_data_exists_at_less_aggregated_geoscale, check_if_location_systems_match, \
    check_if_data_exists_for_same_geoscales, check_allocation_ratios,\
    check_for_differences_between_fba_load_and_fbs_output, compare_fba_load_and_fbs_output_totals

# import specific functions
from flowsa.BEA import subset_BEA_Use
from flowsa.Blackhurst_IO import convert_blackhurst_data_to_gal_per_year, convert_blackhurst_data_to_gal_per_employee
from flowsa.BLS_QCEW import clean_bls_qcew_fba, clean_bls_qcew_fba_for_employment_sat_table, \
    bls_clean_allocation_fba_w_sec
from flowsa.EIA_CBECS_Land import cbecs_land_fba_cleanup
from flowsa.EIA_MECS import mecs_energy_fba_cleanup, eia_mecs_energy_clean_allocation_fba_w_sec, \
    mecs_land_fba_cleanup, mecs_land_fba_cleanup_for_land_2012_fbs, mecs_land_clean_allocation_mapped_fba_w_sec
from flowsa.EPA_NEI import clean_NEI_fba, clean_NEI_fba_no_pesticides
from flowsa.StatCan_IWS_MI import convert_statcan_data_to_US_water_use, disaggregate_statcan_to_naics_6
from flowsa.stewicombo_to_sector import stewicombo_to_sector, stewi_to_sector
from flowsa.USDA_CoA_Cropland import disaggregate_coa_cropland_to_6_digit_naics, coa_irrigated_cropland_fba_cleanup
from flowsa.USDA_ERS_MLU import allocate_usda_ers_mlu_land_in_urban_areas, allocate_usda_ers_mlu_land_in_rural_transportation_areas
from flowsa.USDA_IWMS import disaggregate_iwms_to_6_digit_naics
from flowsa.USGS_NWIS_WU import usgs_fba_data_cleanup, usgs_fba_w_sectors_data_cleanup


def parse_args():
    """Make year and source script parameters"""
    ap = argparse.ArgumentParser()
    ap.add_argument("-m", "--method", required=True, help="Method for flow by sector file. "
                                                          "A valid method config file must exist with this name.")
    args = vars(ap.parse_args())
    return args


def load_method(method_name):
    """
    Loads a flowbysector method from a YAML
    :param method_name:
    :return:
    """
    sfile = flowbysectormethodpath + method_name + '.yaml'
    try:
        with open(sfile, 'r') as f:
            method = yaml.safe_load(f)
    except IOError:
        log.error("FlowBySector method file not found.")
    return method


def load_source_dataframe(k, v):
    """
    Load the source dataframe. Data can be a FlowbyActivity or FlowBySector parquet stored in flowsa, or a FlowBySector
    formatted dataframe from another package.
    :param k: The datasource name
    :param v: The datasource parameters
    :return:
    """
    if v['data_format'] == 'FBA':
        log.info("Retrieving flowbyactivity for datasource " + k + " in year " + str(v['year']))
        flows_df = flowsa.getFlowByActivity(flowclass=[v['class']], years=[v['year']], datasource=k)
    elif v['data_format'] == 'FBS':
        log.info("Retrieving flowbysector for datasource " + k)
        flows_df = flowsa.getFlowBySector(k)
    elif v['data_format'] == 'FBS_outside_flowsa':
        log.info("Retrieving flowbysector for datasource " + k)
        flows_df = getattr(sys.modules[__name__], v["FBS_datapull_fxn"])(*v['parameters'])
    else:
        log.error("Data format not specified in method file for datasource " + k)

    return flows_df


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
    fb = method['source_names']
    # Create empty list for storing fbs files
    fbs_list = []
    for k, v in fb.items():
        # pull fba data for allocation
        flows = load_source_dataframe(k, v)

        if v['data_format'] == 'FBA':
            # ensure correct datatypes and that all fields exist
            flows = clean_df(flows, flow_by_activity_fields, fba_fill_na_dict, drop_description=False)

            # clean up fba, if specified in yaml
            if v["clean_fba_df_fxn"] != 'None':
                log.info("Cleaning up " + k + " FlowByActivity")
                flows = getattr(sys.modules[__name__], v["clean_fba_df_fxn"])(flows)

            # if activity_sets are specified in a file, call them here
            if 'activity_set_file' in v:
                aset_names = pd.read_csv(flowbysectoractivitysetspath + v['activity_set_file'], dtype=str)

            # create dictionary of allocation datasets for different activities
            activities = v['activity_sets']
            # subset activity data and allocate to sector
            for aset, attr in activities.items():
                # subset by named activities
                if 'activity_set_file' in v:
                    names = aset_names[aset_names['activity_set'] == aset]['name']
                else:
                    names = attr['names']

                log.info("Preparing to handle subset of flownames " + ', '.join(map(str, names)) + " in " + k)
                # subset fba data by activity
                flows_subset = flows[(flows[fba_activity_fields[0]].isin(names)) |
                                     (flows[fba_activity_fields[1]].isin(names))].reset_index(drop=True)

                # extract relevant geoscale data or aggregate existing data
                log.info("Subsetting/aggregating dataframe to " + attr['allocation_from_scale'] + " geoscale")
                flows_subset_geo = subset_df_by_geoscale(flows_subset, v['geoscale_to_use'],
                                                         attr['allocation_from_scale'])

                # Add sectors to df activity, depending on level of specified sector aggregation
                log.info("Adding sectors to " + k)
                flow_subset_wsec = add_sectors_to_flowbyactivity(flows_subset_geo,
                                                                 sectorsourcename=method['target_sector_source'],
                                                                 allocationmethod=attr['allocation_method'])
                # clean up fba with sectors, if specified in yaml
                if v["clean_fba_w_sec_df_fxn"] != 'None':
                    log.info("Cleaning up " + k + " FlowByActivity with sectors")
                    flow_subset_wsec = getattr(sys.modules[__name__], v["clean_fba_w_sec_df_fxn"])(flow_subset_wsec,
                                                                                                   attr=attr)

                # map df to elementary flows
                log.info("Mapping flows in " + k + ' to federal elementary flow list')
                if 'fedefl_mapping' in v:
                    mapping_files = v['fedefl_mapping']
                else:
                    mapping_files = k

                flow_subset_mapped = map_elementary_flows(flow_subset_wsec, mapping_files)

                # clean up mapped fba with sectors, if specified in yaml
                if "clean_mapped_fba_w_sec_df_fxn" in v:
                    log.info("Cleaning up " + k + " FlowByActivity with sectors")
                    flow_subset_mapped = getattr(sys.modules[__name__],
                                                 v["clean_mapped_fba_w_sec_df_fxn"])(flow_subset_mapped, attr, method)

                # if allocation method is "direct", then no need to create alloc ratios, else need to use allocation
                # dataframe to create sector allocation ratios
                if attr['allocation_method'] == 'direct':
                    log.info('Directly assigning ' + ', '.join(map(str, names)) + ' to sectors')
                    fbs = flow_subset_mapped.copy()
                    # for each activity, if activities are not sector like, check that there is no data loss
                    if load_source_catalog()[k]['sector-like_activities'] is False:
                        activity_list = []
                        for n in names:
                            log.info('Checking for ' + n + ' at ' + method['target_sector_level'])
                            fbs_subset = fbs[((fbs[fba_activity_fields[0]] == n) &
                                             (fbs[fba_activity_fields[1]] == n)) |
                                             (fbs[fba_activity_fields[0]] == n) |
                                             (fbs[fba_activity_fields[1]] == n)].reset_index(drop=True)
                            fbs_subset = check_if_losing_sector_data(fbs_subset, method['target_sector_level'])
                            activity_list.append(fbs_subset)
                        fbs = pd.concat(activity_list, ignore_index=True)

                # if allocation method for an activity set requires a specific function due to the complicated nature
                # of the allocation, call on function here
                elif attr['allocation_method'] == 'allocation_function':
                    log.info('Calling on function specified in method yaml to allocate ' +
                             ', '.join(map(str, names)) + ' to sectors')
                    fbs = getattr(sys.modules[__name__], attr['allocation_source'])(flow_subset_mapped, attr, fbs_list)

                else:
                    # determine appropriate allocation dataset
                    log.info("Loading allocation flowbyactivity " + attr['allocation_source'] + " for year " +
                             str(attr['allocation_source_year']))
                    fba_allocation = flowsa.getFlowByActivity(flowclass=[attr['allocation_source_class']],
                                                              datasource=attr['allocation_source'],
                                                              years=[attr['allocation_source_year']]).reset_index(drop=True)

                    # clean df and harmonize unites
                    fba_allocation = clean_df(fba_allocation, flow_by_activity_fields, fba_fill_na_dict)
                    fba_allocation = harmonize_units(fba_allocation)

                    # subset based on yaml settings
                    if attr['allocation_flow'] != 'None':
                        fba_allocation = fba_allocation.loc[fba_allocation['FlowName'].isin(attr['allocation_flow'])]
                    if attr['allocation_compartment'] != 'None':
                        fba_allocation = fba_allocation.loc[
                            fba_allocation['Compartment'].isin(attr['allocation_compartment'])]

                    # cleanup the fba allocation df, if necessary
                    if 'clean_allocation_fba' in attr:
                        log.info("Cleaning " + attr['allocation_source'])
                        fba_allocation = getattr(sys.modules[__name__],
                                                 attr["clean_allocation_fba"])(fba_allocation, attr=attr)
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
                        fba_allocation = agg_by_geoscale(fba_allocation, from_scale, to_scale,
                                                         fba_default_grouping_fields)
                    # else, if fba is more aggregated than allocation table, use fba as both to and from scale
                    else:
                        fba_allocation = filter_by_geoscale(fba_allocation, from_scale)

                    # assign sector to allocation dataset
                    log.info("Adding sectors to " + attr['allocation_source'])
                    fba_allocation_wsec = add_sectors_to_flowbyactivity(fba_allocation,
                                                                        sectorsourcename=method['target_sector_source'])

                    # call on fxn to further clean up/disaggregate the fba allocation data, if exists
                    if 'clean_allocation_fba_w_sec' in attr:
                        log.info("Further disaggregating sectors in " + attr['allocation_source'])
                        fba_allocation_wsec = getattr(sys.modules[__name__],
                                                      attr["clean_allocation_fba_w_sec"])(fba_allocation_wsec, attr=attr, method=method)

                    # subset fba datasets to only keep the sectors associated with activity subset
                    log.info("Subsetting " + attr['allocation_source'] + " for sectors in " + k)
                    fba_allocation_subset = get_fba_allocation_subset(fba_allocation_wsec, k, names,
                                                                      flowSubsetMapped=flow_subset_mapped,
                                                                      allocMethod=attr['allocation_method'])

                    # if there is an allocation helper dataset, modify allocation df
                    if attr['allocation_helper'] == 'yes':
                        log.info("Using the specified allocation help for subset of " + attr['allocation_source'])
                        fba_allocation_subset = allocation_helper(fba_allocation_subset, attr, method, v)

                    # create flow allocation ratios for each activity
                    # if load_source_catalog()[k]['sector-like_activities']
                    flow_alloc_list = []
                    group_cols = fba_mapped_default_grouping_fields
                    group_cols = [e for e in group_cols if e not in ('ActivityProducedBy', 'ActivityConsumedBy')]
                    for n in names:
                        log.info("Creating allocation ratios for " + n)
                        fba_allocation_subset_2 = get_fba_allocation_subset(fba_allocation_subset, k, [n],
                                                                            flowSubsetMapped=flow_subset_mapped,
                                                                            allocMethod=attr['allocation_method'])
                        if len(fba_allocation_subset_2) == 0:
                            log.info("No data found to allocate " + n)
                        else:
                            flow_alloc = allocate_by_sector(fba_allocation_subset_2, k, attr['allocation_source'],
                                                            attr['allocation_method'], group_cols,
                                                            flowSubsetMapped=flow_subset_mapped)
                            flow_alloc = flow_alloc.assign(FBA_Activity=n)
                            flow_alloc_list.append(flow_alloc)
                    flow_allocation = pd.concat(flow_alloc_list, ignore_index=True)

                    # generalize activity field names to enable link to main fba source
                    log.info("Generalizing activity columns in subset of " + attr['allocation_source'])
                    flow_allocation = collapse_activity_fields(flow_allocation)

                    # check for issues with allocation ratios
                    check_allocation_ratios(flow_allocation, aset, k, method_name)

                    # create list of sectors in the flow allocation df, drop any rows of data in the flow df that \
                    # aren't in list
                    sector_list = flow_allocation['Sector'].unique().tolist()

                    # subset fba allocation table to the values in the activity list, based on overlapping sectors
                    flow_subset_mapped = flow_subset_mapped.loc[
                        (flow_subset_mapped[fbs_activity_fields[0]].isin(sector_list)) |
                        (flow_subset_mapped[fbs_activity_fields[1]].isin(sector_list))]

                    # check if fba and allocation dfs have the same LocationSystem
                    log.info("Checking if flowbyactivity and allocation dataframes use the same location systems")
                    check_if_location_systems_match(flow_subset_mapped, flow_allocation)

                    # merge fba df w/flow allocation dataset
                    log.info("Merge " + k + " and subset of " + attr['allocation_source'])
                    fbs = flow_subset_mapped.merge(
                        flow_allocation[['Location', 'Sector', 'FlowAmountRatio', 'FBA_Activity']],
                        left_on=['Location', 'SectorProducedBy', 'ActivityProducedBy'],
                        right_on=['Location', 'Sector', 'FBA_Activity'], how='left')

                    fbs = fbs.merge(
                        flow_allocation[['Location', 'Sector', 'FlowAmountRatio', 'FBA_Activity']],
                        left_on=['Location', 'SectorConsumedBy', 'ActivityConsumedBy'],
                        right_on=['Location', 'Sector', 'FBA_Activity'], how='left')

                    # merge the flowamount columns
                    fbs.loc[:, 'FlowAmountRatio'] = fbs['FlowAmountRatio_x'].fillna(fbs['FlowAmountRatio_y'])
                    # fill null rows with 0 because no allocation info
                    fbs['FlowAmountRatio'] = fbs['FlowAmountRatio'].fillna(0)

                    # check if fba and alloc dfs have data for same geoscales - comment back in after address the 'todo'
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
                                            'FlowAmountRatio', 'FBA_Activity_x', 'FBA_Activity_y'])

                # drop rows where flowamount = 0 (although this includes dropping suppressed data)
                fbs = fbs[fbs['FlowAmount'] != 0].reset_index(drop=True)

                # define grouping columns dependent on sectors being activity-like or not
                if load_source_catalog()[k]['sector-like_activities'] is False:
                    groupingcols = fbs_grouping_fields_w_activities
                    groupingdict = flow_by_sector_fields_w_activity
                else:
                    groupingcols = fbs_default_grouping_fields
                    groupingdict = flow_by_sector_fields

                # clean df
                fbs = clean_df(fbs, groupingdict, fbs_fill_na_dict)

                # aggregate df geographically, if necessary
                # todo: replace with fxn return_from_scale
                log.info("Aggregating flowbysector to " + method['target_geoscale'] + " level")
                if fips_number_key[v['geoscale_to_use']] < fips_number_key[attr['allocation_from_scale']]:
                    from_scale = v['geoscale_to_use']
                else:
                    from_scale = attr['allocation_from_scale']

                to_scale = method['target_geoscale']

                fbs_geo_agg = agg_by_geoscale(fbs, from_scale, to_scale, groupingcols)

                # aggregate data to every sector level
                log.info("Aggregating flowbysector to all sector levels")
                fbs_sec_agg = sector_aggregation(fbs_geo_agg, groupingcols)
                # add missing naics5/6 when only one naics5/6 associated with a naics4
                fbs_agg = sector_disaggregation(fbs_sec_agg, groupingdict)

                # check if any sector information is lost before reaching the target sector length, if so,
                # allocate values equally to disaggregated sectors
                log.info('Checking for data at ' + method['target_sector_level'])
                fbs_agg_2 = check_if_losing_sector_data(fbs_agg, method['target_sector_level'])

                # compare flowbysector with flowbyactivity
                # todo: modify fxn to work if activities are sector like in df being allocated
                if load_source_catalog()[k]['sector-like_activities'] is False:
                    check_for_differences_between_fba_load_and_fbs_output(flow_subset_mapped, fbs_agg_2, aset, k, method_name)

                # return sector level specified in method yaml
                # load the crosswalk linking sector lengths
                sector_list = get_sector_list(method['target_sector_level'])

                # subset df, necessary because not all of the sectors are NAICS and can get duplicate rows
                fbs_1 = fbs_agg_2.loc[(fbs_agg_2[fbs_activity_fields[0]].isin(sector_list)) &
                                      (fbs_agg_2[fbs_activity_fields[1]].isin(sector_list))].reset_index(drop=True)
                fbs_2 = fbs_agg_2.loc[(fbs_agg_2[fbs_activity_fields[0]].isin(sector_list)) &
                                      (fbs_agg_2[fbs_activity_fields[1]].isnull())].reset_index(drop=True)
                fbs_3 = fbs_agg_2.loc[(fbs_agg_2[fbs_activity_fields[0]].isnull()) &
                                      (fbs_agg_2[fbs_activity_fields[1]].isin(sector_list))].reset_index(drop=True)
                fbs_sector_subset = pd.concat([fbs_1, fbs_2, fbs_3])

                # drop activity columns
                fbs_sector_subset = fbs_sector_subset.drop(['ActivityProducedBy', 'ActivityConsumedBy'],
                                                           axis=1, errors='ignore')

                # save comparision of FBA total to FBS total for an activity set
                compare_fba_load_and_fbs_output_totals(flows_subset_geo, fbs_sector_subset, aset, k,
                                                       method_name, attr, method)

                log.info("Completed flowbysector for activity subset with flows " + ', '.join(map(str, names)))
                fbs_list.append(fbs_sector_subset)
        else:
            # if the loaded flow dt is already in FBS format, append directly to list of FBS
            log.info("Append " + k + " to FBS list")
            # ensure correct field datatypes and add any missing fields
            flows = clean_df(flows, flow_by_sector_fields, fbs_fill_na_dict)
            fbs_list.append(flows)
    # create single df of all activities
    log.info("Concat data for all activities")
    fbss = pd.concat(fbs_list, ignore_index=True, sort=False)
    log.info("Clean final dataframe")
    # aggregate df as activities might have data for the same specified sector length
    fbss = clean_df(fbss, flow_by_sector_fields, fbs_fill_na_dict)
    fbss = aggregator(fbss, fbs_default_grouping_fields)
    # sort df
    log.info("Sort and store dataframe")
    # add missing fields, ensure correct data type, reorder columns
    fbss = fbss.sort_values(
        ['SectorProducedBy', 'SectorConsumedBy', 'Flowable', 'Context']).reset_index(drop=True)
    # save parquet file
    store_flowbysector(fbss, method_name)


if __name__ == '__main__':
    # assign arguments
    args = parse_args()
    main(args["method"])

