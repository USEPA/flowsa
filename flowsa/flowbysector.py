# flowbysector.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Produces a FlowBySector data frame based on a method file for the given class

To run code, specify the "Run/Debug Configurations" Parameters to the
"flowsa/data/flowbysectormethods" yaml file name
you want to use.

Example: "Parameters: --m Water_national_2015_m1"

Files necessary to run FBS:
a. a method yaml in "flowsa/data/flowbysectormethods"
b. crosswalk(s) for the main dataset you are allocating and any datasets
used to allocate to sectors
c. a .py file in "flowsa/" for the main dataset you are allocating if
you need functions to clean up the FBA
   before allocating to FBS

"""

import argparse
import pandas as pd
import os
from esupy.processed_data_mgmt import write_df_to_file
import flowsa
from flowsa.allocation import equally_allocate_parent_to_child_naics
from flowsa.common import check_activities_sector_like, str2bool, \
    fba_activity_fields, rename_log_file, fba_fill_na_dict, fbs_fill_na_dict, \
    fbs_default_grouping_fields, fbs_grouping_fields_w_activities, \
    logoutputpath, load_yaml_dict
from flowsa.dataclean import clean_df, harmonize_FBS_columns, \
    reset_fbs_dq_scores
from flowsa.fbs_allocation import direct_allocation_method, \
    function_allocation_method, dataset_allocation_method
from flowsa.flowbyfunctions import agg_by_geoscale, sector_aggregation, \
    aggregator, subset_df_by_geoscale, sector_disaggregation, \
    update_geoscale, subset_df_by_sector_list
from flowsa.location import fips_number_key, merge_urb_cnty_pct
from flowsa.metadata import set_fb_meta, write_metadata
from flowsa.schema import flow_by_activity_fields, flow_by_sector_fields, \
    flow_by_sector_fields_w_activity
from flowsa.sectormapping import add_sectors_to_flowbyactivity, \
    map_fbs_flows, get_sector_list
from flowsa.settings import log, vLog, flowbysectoractivitysetspath, paths
from flowsa.validation import compare_activity_to_sector_flowamounts, \
    compare_fba_geo_subset_and_fbs_output_totals, compare_geographic_totals,\
    replace_naics_w_naics_from_another_year, check_for_negative_flowamounts, \
    compare_child_to_parent_sectors_flowamounts, \
    check_if_data_exists_at_geoscale, calculate_flowamount_diff_between_dfs


def parse_args():
    """
    Make method parameters
    :return: dictionary, 'method'
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("-m", "--method", required=True,
                    help="Method for flow by sector file. A valid method "
                         "config file must exist with this name.")
    ap.add_argument("-c", "--fbsconfigpath",
                    type=str2bool, required=False,
                    help="Option to specify where to find the FBS method "
                         "yaml.")
    ap.add_argument("-d", "--download_FBAs_if_missing",
                    type=str2bool, required=False,
                    help="Option to download any FBAs not saved locally "
                         "rather than generating the FBAs in FLOWSA.")
    args = vars(ap.parse_args())
    return args


def load_source_dataframe(method, sourcename, source_dict,
                          download_FBA_if_missing, fbsconfigpath=None):
    """
    Load the source dataframe. Data can be a FlowbyActivity or
    FlowBySector parquet stored in flowsa, or a FlowBySector
    formatted dataframe from another package.
    :param method: dictionary, FBS method
    :param sourcename: str, The datasource name
    :param source_dict: dictionary, The datasource parameters
    :param download_FBA_if_missing: Bool, if True will download FBAs from
       Data Commons. Default is False.
    :param fbsconfigpath, str, optional path to an FBS method outside flowsa
        repo
    :return: df of identified parquet
    """
    if source_dict['data_format'] == 'FBA':
        # if yaml specifies a geoscale to load, use parameter
        # to filter dataframe
        if 'source_fba_load_scale' in source_dict:
            geo_level = source_dict['source_fba_load_scale']
        else:
            geo_level = None
        vLog.info("Retrieving Flow-By-Activity for datasource %s in year %s",
                  sourcename, str(source_dict['year']))
        flows_df = flowsa.getFlowByActivity(
            datasource=sourcename,
            year=source_dict['year'],
            flowclass=source_dict['class'],
            geographic_level=geo_level,
            download_FBA_if_missing=download_FBA_if_missing)
    elif source_dict['data_format'] == 'FBS':
        vLog.info("Retrieving flowbysector for datasource %s", sourcename)
        flows_df = flowsa.getFlowBySector(sourcename)
    elif source_dict['data_format'] == 'FBS_outside_flowsa':
        vLog.info("Retrieving flowbysector for datasource %s", sourcename)
        flows_df = source_dict["FBS_datapull_fxn"](source_dict,
                                                   method,
                                                   fbsconfigpath)
    else:
        vLog.error("Data format not specified in method "
                   "file for datasource %s", sourcename)

    return flows_df


def main(**kwargs):
    """
    Creates a flowbysector dataset
    :param kwargs: dictionary of arguments, only argument is
        "method_name", the name of method corresponding to flowbysector
        method yaml name
    :return: parquet, FBS save to local folder
    """
    if len(kwargs) == 0:
        kwargs = parse_args()

    method_name = kwargs['method']
    fbsconfigpath = kwargs.get('fbsconfigpath')
    download_FBA_if_missing = kwargs.get('download_FBAs_if_missing')
    # assign arguments
    vLog.info("Initiating flowbysector creation for %s", method_name)
    # call on method
    method = load_yaml_dict(method_name, flowbytype='FBS',
                            filepath=fbsconfigpath)
    # create dictionary of data and allocation datasets
    fb = method['source_names']
    # Create empty list for storing fbs files
    fbs_list = []
    for k, v in fb.items():
        # pull fba data for allocation
        flows = load_source_dataframe(method, k, v, download_FBA_if_missing,
                                      fbsconfigpath)

        if v['data_format'] == 'FBA':
            # ensure correct datatypes and that all fields exist
            flows = clean_df(flows, flow_by_activity_fields,
                             fba_fill_na_dict, drop_description=False)

            # split data by urban and rural
            if v.get('apply_urban_rural'):
                vLog.info(f"Splitting {k} into urban and rural quantities "
                          "by FIPS.")
                flows = merge_urb_cnty_pct(flows)

            # clean up fba before mapping, if specified in yaml
            if "clean_fba_before_mapping_df_fxn" in v:
                vLog.info("Cleaning up %s FlowByActivity", k)
                flows = v["clean_fba_before_mapping_df_fxn"](flows)

            # map flows to federal flow list or material flow list
            flows_mapped, mapping_files = \
                map_fbs_flows(flows, k, v, keep_fba_columns=True)

            # clean up fba, if specified in yaml
            if "clean_fba_df_fxn" in v:
                vLog.info("Cleaning up %s FlowByActivity", k)
                flows_mapped = v["clean_fba_df_fxn"](flows_mapped)

            # master list of activity names read in from data source
            ml_act = []
            # create dictionary of allocation datasets for different activities
            activities = v['activity_sets']
            # subset activity data and allocate to sector
            for aset, attr in activities.items():
                # subset by named activities
                names = attr['names']

                # to avoid double counting data from the same source, in
                # the event there are values in both the APB and ACB
                # columns, if an activity has already been read in and
                # allocated, remove that activity from the mapped flows
                # regardless of what activity set the data was read in
                flows_mapped = flows_mapped[
                    ~((flows_mapped[fba_activity_fields[0]].isin(ml_act)) |
                      (flows_mapped[fba_activity_fields[1]].isin(ml_act))
                      )].reset_index(drop=True)
                ml_act.extend(names)

                vLog.info("Preparing to handle %s in %s", aset, k)
                # subset fba data by activity
                flows_subset = flows_mapped[
                    (flows_mapped[fba_activity_fields[0]].isin(names)) |
                    (flows_mapped[fba_activity_fields[1]].isin(names)
                     )].reset_index(drop=True)

                # subset by flowname if exists
                if 'source_flows' in attr:
                    flows_subset = flows_subset[flows_subset['FlowName']
                                                .isin(attr['source_flows'])]
                if len(flows_subset) == 0:
                    log.warning(f"no data found for flows in {aset}")
                    continue
                if len(flows_subset[flows_subset['FlowAmount'] != 0]) == 0:
                    log.warning(f"all flow data for {aset} is 0")
                    continue
                # if activities are sector-like, check sectors are valid
                if check_activities_sector_like(flows_subset):
                    flows_subset2 = replace_naics_w_naics_from_another_year(
                        flows_subset, method['target_sector_source'])

                    # check impact on df FlowAmounts
                    vLog.info('Calculate FlowAmount difference caused by '
                              'replacing NAICS Codes with %s, saving '
                              'difference in Validation log',
                              method['target_sector_source'],)
                    calculate_flowamount_diff_between_dfs(
                        flows_subset, flows_subset2)
                else:
                    flows_subset2 = flows_subset.copy()

                # extract relevant geoscale data or aggregate existing data
                flows_subset_geo = subset_df_by_geoscale(
                    flows_subset2, v['geoscale_to_use'],
                    attr['allocation_from_scale'])
                # if loading data subnational geoscale, check for data loss
                if attr['allocation_from_scale'] != 'national':
                    compare_geographic_totals(
                        flows_subset_geo, flows_mapped, k, attr, aset, names)

                # Add sectors to df activity, depending on level
                # of specified sector aggregation
                log.info("Adding sectors to %s", k)
                flows_subset_wsec = add_sectors_to_flowbyactivity(
                    flows_subset_geo, v.get('activity_to_sector_mapping'),
                    sectorsourcename=method['target_sector_source'],
                    allocationmethod=attr['allocation_method'],
                    fbsconfigpath=fbsconfigpath)
                # clean up fba with sectors, if specified in yaml
                if "clean_fba_w_sec_df_fxn" in v:
                    vLog.info("Cleaning up %s FlowByActivity with sectors", k)
                    flows_subset_wsec = v["clean_fba_w_sec_df_fxn"](
                        flows_subset_wsec,
                        attr=attr,
                        method=method
                    )
                # check for activities at geoscale - return any missing
                # locations for an activity
                check_if_data_exists_at_geoscale(flows_subset_geo,
                                                 attr['allocation_from_scale'])

                # rename SourceName to MetaSources and drop columns
                flows_mapped_wsec = flows_subset_wsec.\
                    rename(columns={'SourceName': 'MetaSources'}).\
                    drop(columns=['FlowName', 'Compartment'])

                # if allocation method is "direct", then no need
                # to create alloc ratios, else need to use allocation
                # dataframe to create sector allocation ratios
                if attr['allocation_method'] == 'direct':
                    fbs = direct_allocation_method(
                        flows_mapped_wsec, k, names, method)
                # if allocation method for an activity set requires a specific
                # function due to the complicated nature
                # of the allocation, call on function here
                elif attr['allocation_method'] == 'allocation_function':
                    fbs = function_allocation_method(
                        flows_mapped_wsec, k, names, attr, fbs_list)
                else:
                    fbs = dataset_allocation_method(
                        flows_mapped_wsec, attr, names, method, k, v, aset,
                        download_FBA_if_missing, fbsconfigpath)

                # drop rows where flowamount = 0
                # (although this includes dropping suppressed data)
                fbs = fbs[fbs['FlowAmount'] != 0].reset_index(drop=True)

                # define grouping columns dependent on sectors
                # being activity-like or not
                if check_activities_sector_like(fbs) is False:
                    groupingcols = fbs_grouping_fields_w_activities
                    groupingdict = flow_by_sector_fields_w_activity
                else:
                    groupingcols = fbs_default_grouping_fields
                    groupingdict = flow_by_sector_fields

                # clean df
                fbs = clean_df(fbs, groupingdict, fbs_fill_na_dict)

                # aggregate df geographically, if necessary
                log.info("Aggregating flowbysector to %s level",
                         method['target_geoscale'])
                # determine from scale
                if fips_number_key[v['geoscale_to_use']] <\
                        fips_number_key[attr['allocation_from_scale']]:
                    from_scale = v['geoscale_to_use']
                else:
                    from_scale = attr['allocation_from_scale']

                fbs_geo_agg = agg_by_geoscale(
                    fbs, from_scale, method['target_geoscale'], groupingcols)

                # aggregate data to every sector level
                log.info("Aggregating flowbysector to all sector levels")
                fbs_sec_agg = sector_aggregation(fbs_geo_agg)
                # add missing naics5/6 when only one naics5/6
                # associated with a naics4
                fbs_agg = sector_disaggregation(fbs_sec_agg)

                # check if any sector information is lost before reaching
                # the target sector length, if so,
                # allocate values equally to disaggregated sectors
                vLog.info('Searching for and allocating FlowAmounts for any '
                          'parent NAICS dropped while subsetting the '
                          'dataframe')
                fbs_agg_2 = equally_allocate_parent_to_child_naics(
                    fbs_agg, method)

                # compare child sectors to parent sectors flow amounts
                compare_child_to_parent_sectors_flowamounts(fbs)

                # compare flowbysector with flowbyactivity
                compare_activity_to_sector_flowamounts(
                    flows_mapped_wsec, fbs_agg_2, aset, method)

                # return sector level specified in method yaml
                # load the crosswalk linking sector lengths
                secondary_sector_level = \
                    method.get('target_subset_sector_level')
                sector_list = get_sector_list(
                    method['target_sector_level'],
                    secondary_sector_level_dict=secondary_sector_level)

                # subset df, necessary because not all of the sectors are
                # NAICS and can get duplicate rows
                fbs_sector_subset = subset_df_by_sector_list(
                    fbs_agg_2, sector_list)

                # drop activity columns
                fbs_sector_subset = fbs_sector_subset.drop(
                    ['ActivityProducedBy', 'ActivityConsumedBy'], axis=1,
                    errors='ignore')

                # save comparison of FBA total to FBS total for an activity set
                compare_fba_geo_subset_and_fbs_output_totals(
                    flows_subset_geo, fbs_sector_subset, aset, k, v, attr,
                    method)

                log.info("Completed flowbysector for %s", aset)
                fbs_list.append(fbs_sector_subset)
        else:
            if 'clean_fbs_df_fxn' in v:
                flows = v["clean_fbs_df_fxn"](flows, method)
            flows = update_geoscale(flows, method['target_geoscale'])
            # if the loaded flow dt is already in FBS format,
            # append directly to list of FBS
            log.info("Append %s to FBS list", k)
            # ensure correct field datatypes and add any missing fields
            flows = clean_df(flows, flow_by_sector_fields, fbs_fill_na_dict)
            fbs_list.append(flows)
    # create single df of all activities
    log.info("Concat data for all activities")
    fbss = pd.concat(fbs_list, ignore_index=True, sort=False)
    log.info("Clean final dataframe")
    # add missing fields, ensure correct data type,
    # add missing columns, reorder columns
    fbss = clean_df(fbss, flow_by_sector_fields, fbs_fill_na_dict)
    # prior to aggregating, replace MetaSources string with all sources
    # that share context/flowable/sector values
    fbss = harmonize_FBS_columns(fbss)
    # aggregate df as activities might have data for
    # the same specified sector length
    fbss = aggregator(fbss, fbs_default_grouping_fields)
    # sort df
    log.info("Sort and store dataframe")
    # ensure correct data types/order of columns
    fbss = clean_df(fbss, flow_by_sector_fields, fbs_fill_na_dict)
    fbss = fbss.sort_values(['SectorProducedBy', 'SectorConsumedBy',
                             'Flowable', 'Context']).reset_index(drop=True)
    # check for negative flow amounts
    check_for_negative_flowamounts(fbss)
    # tmp reset data quality scores
    fbss = reset_fbs_dq_scores(fbss)
    # save parquet file
    meta = set_fb_meta(method_name, "FlowBySector")
    write_df_to_file(fbss, paths, meta)
    write_metadata(method_name, method, meta, "FlowBySector")
    # rename the log file saved to local directory
    rename_log_file(method_name, meta)
    log.info('See the Validation log for detailed assessment of '
             'model results in %s', logoutputpath)


if __name__ == '__main__':
    main()
