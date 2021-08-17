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
import yaml
import pandas as pd
from esupy.processed_data_mgmt import write_df_to_file
import flowsa
from flowsa.common import log, flowbysectormethodpath, flow_by_sector_fields, \
    fips_number_key, flow_by_activity_fields, load_source_catalog, \
    flowbysectoractivitysetspath, flow_by_sector_fields_w_activity, \
    paths, fba_activity_fields, rename_log_file, \
    fbs_activity_fields, fba_fill_na_dict, fbs_fill_na_dict, fbs_default_grouping_fields, \
    fbs_grouping_fields_w_activities
from flowsa.metadata import set_fb_meta, write_metadata
from flowsa.fbs_allocation import direct_allocation_method, function_allocation_method, \
    dataset_allocation_method
from flowsa.mapping import add_sectors_to_flowbyactivity, map_fbs_flows, \
    get_sector_list
from flowsa.flowbyfunctions import agg_by_geoscale, sector_aggregation, \
    aggregator, subset_df_by_geoscale, sector_disaggregation, dynamically_import_fxn
from flowsa.dataclean import clean_df, harmonize_FBS_columns, reset_fbs_dq_scores
from flowsa.validation import check_if_losing_sector_data,\
    check_for_differences_between_fba_load_and_fbs_output, \
    compare_fba_load_and_fbs_output_totals, compare_geographic_totals,\
    replace_naics_w_naics_from_another_year


def parse_args():
    """
    Make year and source script parameters
    :return: dictionary, 'method'
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("-m", "--method",
                    required=True, help="Method for flow by sector file. "
                                        "A valid method config file must exist with this name.")
    args = vars(ap.parse_args())
    return args


def load_method(method_name):
    """
    Loads a flowbysector method from a YAML
    :param method_name: str, FBS method name (ex. 'Water_national_m1_2015')
    :return: dictionary, items in the FBS method yaml
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
    Load the source dataframe. Data can be a FlowbyActivity or
    FlowBySector parquet stored in flowsa, or a FlowBySector
    formatted dataframe from another package.
    :param k: str, The datasource name
    :param v: dictionary, The datasource parameters
    :return: df of identified parquet
    """
    if v['data_format'] == 'FBA':
        # if yaml specifies a geoscale to load, use parameter to filter dataframe
        if 'source_fba_load_scale' in v:
            geo_level = v['source_fba_load_scale']
        else:
            geo_level = None
        log.info("Retrieving flowbyactivity for datasource %s in year %s", k, str(v['year']))
        flows_df = flowsa.getFlowByActivity(datasource=k, year=v['year'], flowclass=v['class'],
                                            geographic_level=geo_level)
    elif v['data_format'] == 'FBS':
        log.info("Retrieving flowbysector for datasource %s", k)
        flows_df = flowsa.getFlowBySector(k)
    elif v['data_format'] == 'FBS_outside_flowsa':
        log.info("Retrieving flowbysector for datasource %s", k)
        flows_df = dynamically_import_fxn(k, v["FBS_datapull_fxn"])(v)
    else:
        log.error("Data format not specified in method file for datasource %s", k)

    return flows_df


def main(**kwargs):
    """
    Creates a flowbysector dataset
    :param kwargs: dictionary of arguments, only argument is "method_name", the name of method
                   corresponding to flowbysector method yaml name
    :return: parquet, FBS save to local folder
    """
    if len(kwargs) == 0:
        kwargs = parse_args()

    method_name = kwargs['method']
    # assign arguments
    log.info("Initiating flowbysector creation for %s", method_name)
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
            flows = clean_df(flows, flow_by_activity_fields,
                             fba_fill_na_dict, drop_description=False)

            # map flows to federal flow list or material flow list
            flows_mapped, mapping_files = map_fbs_flows(flows, k, v, keep_fba_columns=True)

            # subset out the mapping information, add back in after cleaning up FBA data
            mapped_df = flows_mapped[['FlowName', 'Flowable', 'Compartment',
                                      'Context', 'FlowUUID']].drop_duplicates()
            flows_fba = flows_mapped[flow_by_activity_fields]

            # clean up fba, if specified in yaml
            if v["clean_fba_df_fxn"] != 'None':
                log.info("Cleaning up %s FlowByActivity", k)
                flows_fba = dynamically_import_fxn(k, v["clean_fba_df_fxn"])(flows_fba)

            # if activity_sets are specified in a file, call them here
            if 'activity_set_file' in v:
                aset_names = pd.read_csv(flowbysectoractivitysetspath +
                                         v['activity_set_file'], dtype=str)
            else:
                aset_names = None

            # create dictionary of allocation datasets for different activities
            activities = v['activity_sets']
            # subset activity data and allocate to sector
            for aset, attr in activities.items():
                # subset by named activities
                if 'activity_set_file' in v:
                    names = aset_names[aset_names['activity_set'] == aset]['name']
                else:
                    names = attr['names']

                log.info("Preparing to handle %s in %s", aset, k)
                log.debug("Preparing to handle subset of activities: %s", ', '.join(map(str, names)))
                # subset fba data by activity
                flows_subset =\
                    flows_fba[(flows_fba[fba_activity_fields[0]].isin(names)) |
                              (flows_fba[fba_activity_fields[1]].isin(names)
                               )].reset_index(drop=True)

                # if activities are sector-like, check sectors are valid
                if load_source_catalog()[k]['sector-like_activities']:
                    flows_subset =\
                        replace_naics_w_naics_from_another_year(flows_subset,
                                                                method['target_sector_source'])

                # extract relevant geoscale data or aggregate existing data
                flows_subset_geo = subset_df_by_geoscale(flows_subset, v['geoscale_to_use'],
                                                         attr['allocation_from_scale'])
                # if loading data subnational geoscale, check for data loss
                if attr['allocation_from_scale'] != 'national':
                    compare_geographic_totals(flows_subset_geo, flows_subset, k, method_name, aset)

                # Add sectors to df activity, depending on level of specified sector aggregation
                log.info("Adding sectors to %s", k)
                flows_subset_wsec =\
                    add_sectors_to_flowbyactivity(flows_subset_geo,
                                                  sectorsourcename=method['target_sector_source'],
                                                  allocationmethod=attr['allocation_method'])
                # clean up fba with sectors, if specified in yaml
                if v["clean_fba_w_sec_df_fxn"] != 'None':
                    log.info("Cleaning up %s FlowByActivity with sectors", k)
                    flows_subset_wsec = \
                        dynamically_import_fxn(k, v["clean_fba_w_sec_df_fxn"])(flows_subset_wsec,
                                                                               attr=attr, method=method)

                # add mapping columns back
                flows_mapped_wsec = flows_subset_wsec.merge(mapped_df, how='left')
                # rename SourceName to MetaSources and drop columns
                flows_mapped_wsec = flows_mapped_wsec.\
                    rename(columns={'SourceName': 'MetaSources'}).\
                    drop(columns=['FlowName', 'Compartment'])

                # if allocation method is "direct", then no need to create alloc ratios,
                # else need to use allocation
                # dataframe to create sector allocation ratios
                if attr['allocation_method'] == 'direct':
                    fbs = direct_allocation_method(flows_mapped_wsec, k, names, method)
                # if allocation method for an activity set requires a specific
                # function due to the complicated nature
                # of the allocation, call on function here
                elif attr['allocation_method'] == 'allocation_function':
                    fbs = function_allocation_method(flows_mapped_wsec, k, names, attr, fbs_list)
                else:
                    fbs =\
                        dataset_allocation_method(flows_mapped_wsec, attr,
                                                  names, method, k, v, aset,
                                                  method_name, aset_names)

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
                log.info("Aggregating flowbysector to %s level", method['target_geoscale'])
                # determine from scale
                if fips_number_key[v['geoscale_to_use']] <\
                        fips_number_key[attr['allocation_from_scale']]:
                    from_scale = v['geoscale_to_use']
                else:
                    from_scale = attr['allocation_from_scale']

                fbs_geo_agg = agg_by_geoscale(fbs, from_scale,
                                              method['target_geoscale'], groupingcols)

                # aggregate data to every sector level
                log.info("Aggregating flowbysector to all sector levels")
                fbs_sec_agg = sector_aggregation(fbs_geo_agg, groupingcols)
                # add missing naics5/6 when only one naics5/6 associated with a naics4
                fbs_agg = sector_disaggregation(fbs_sec_agg, groupingdict)

                # check if any sector information is lost before reaching
                # the target sector length, if so,
                # allocate values equally to disaggregated sectors
                log.debug('Checking for data at %s', method['target_sector_level'])
                fbs_agg_2 = check_if_losing_sector_data(fbs_agg, method['target_sector_level'])

                # compare flowbysector with flowbyactivity
                check_for_differences_between_fba_load_and_fbs_output(
                    flows_mapped_wsec, fbs_agg_2, aset, k, method)

                # return sector level specified in method yaml
                # load the crosswalk linking sector lengths
                sector_list = get_sector_list(method['target_sector_level'])

                # subset df, necessary because not all of the sectors are
                # NAICS and can get duplicate rows
                fbs_1 = fbs_agg_2.loc[(fbs_agg_2[fbs_activity_fields[0]].isin(sector_list)) &
                                      (fbs_agg_2[fbs_activity_fields[1]].isin(sector_list))].\
                    reset_index(drop=True)
                fbs_2 = fbs_agg_2.loc[(fbs_agg_2[fbs_activity_fields[0]].isin(sector_list)) &
                                      (fbs_agg_2[fbs_activity_fields[1]].isnull())].\
                    reset_index(drop=True)
                fbs_3 = fbs_agg_2.loc[(fbs_agg_2[fbs_activity_fields[0]].isnull()) &
                                      (fbs_agg_2[fbs_activity_fields[1]].isin(sector_list))].\
                    reset_index(drop=True)
                fbs_sector_subset = pd.concat([fbs_1, fbs_2, fbs_3])

                # drop activity columns
                fbs_sector_subset = fbs_sector_subset.drop(['ActivityProducedBy',
                                                            'ActivityConsumedBy'],
                                                           axis=1, errors='ignore')

                # save comparison of FBA total to FBS total for an activity set
                compare_fba_load_and_fbs_output_totals(flows_subset_geo, mapped_df,
                                                       fbs_sector_subset, aset, k,
                                                       attr, method, mapping_files)

                log.info("Completed flowbysector for %s", aset)
                fbs_list.append(fbs_sector_subset)
        else:
            # if the loaded flow dt is already in FBS format, append directly to list of FBS
            log.info("Append %s to FBS list", k)
            # ensure correct field datatypes and add any missing fields
            flows = clean_df(flows, flow_by_sector_fields, fbs_fill_na_dict)
            fbs_list.append(flows)
    # create single df of all activities
    log.info("Concat data for all activities")
    fbss = pd.concat(fbs_list, ignore_index=True, sort=False)
    log.info("Clean final dataframe")
    # add missing fields, ensure correct data type, add missing columns, reorder columns
    fbss = clean_df(fbss, flow_by_sector_fields, fbs_fill_na_dict)
    # prior to aggregating, replace MetaSources string with all sources
    # that share context/flowable/sector values
    fbss = harmonize_FBS_columns(fbss)
    # aggregate df as activities might have data for the same specified sector length
    fbss = aggregator(fbss, fbs_default_grouping_fields)
    # sort df
    log.info("Sort and store dataframe")
    # ensure correct data types/order of columns
    fbss = clean_df(fbss, flow_by_sector_fields, fbs_fill_na_dict)
    fbss = fbss.sort_values(
        ['SectorProducedBy', 'SectorConsumedBy', 'Flowable', 'Context']).reset_index(drop=True)
    # tmp reset data quality scores
    fbss = reset_fbs_dq_scores(fbss)
    # save parquet file
    meta = set_fb_meta(method_name, "FlowBySector")
    write_df_to_file(fbss, paths, meta)
    write_metadata(method_name, method, meta, "FlowBySector")
    # rename the log file saved to local directory
    rename_log_file(method_name, meta)


if __name__ == '__main__':
    main()
