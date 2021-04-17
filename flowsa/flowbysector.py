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
from esupy.processed_data_mgmt import write_df_to_file
from flowsa.common import log, flowbysectormethodpath, flow_by_sector_fields, \
    fips_number_key, flow_by_activity_fields, load_source_catalog, \
    flowbysectoractivitysetspath, flow_by_sector_fields_w_activity, set_fb_meta, paths, fba_activity_fields, \
    fbs_activity_fields, fba_fill_na_dict, fbs_fill_na_dict, fbs_default_grouping_fields, \
    fbs_grouping_fields_w_activities
from flowsa.fbs_allocation import direct_allocation_method, function_allocation_method, \
    dataset_allocation_method
from flowsa.mapping import add_sectors_to_flowbyactivity, map_elementary_flows, \
    get_sector_list
from flowsa.flowbyfunctions import agg_by_geoscale, sector_aggregation, \
    aggregator, subset_df_by_geoscale, sector_disaggregation
from flowsa.dataclean import clean_df, harmonize_FBS_columns, reset_fbs_dq_scores
from flowsa.datachecks import check_if_losing_sector_data, check_for_differences_between_fba_load_and_fbs_output, \
    compare_fba_load_and_fbs_output_totals, compare_geographic_totals

# import specific functions
from flowsa.data_source_scripts.BEA import subset_BEA_Use
from flowsa.data_source_scripts.Blackhurst_IO import convert_blackhurst_data_to_gal_per_year, convert_blackhurst_data_to_gal_per_employee
from flowsa.data_source_scripts.BLS_QCEW import clean_bls_qcew_fba, clean_bls_qcew_fba_for_employment_sat_table, \
    bls_clean_allocation_fba_w_sec
from flowsa.data_source_scripts.EIA_CBECS_Land import cbecs_land_fba_cleanup
from flowsa.data_source_scripts.EIA_MECS import mecs_energy_fba_cleanup, eia_mecs_energy_clean_allocation_fba_w_sec, \
    mecs_land_fba_cleanup, mecs_land_fba_cleanup_for_land_2012_fbs, mecs_land_clean_allocation_mapped_fba_w_sec
from flowsa.data_source_scripts.EPA_NEI import clean_NEI_fba, clean_NEI_fba_no_pesticides
from flowsa.data_source_scripts.StatCan_IWS_MI import convert_statcan_data_to_US_water_use
from flowsa.data_source_scripts.stewiFBS import stewicombo_to_sector, stewi_to_sector
from flowsa.data_source_scripts.USDA_CoA_Cropland import disaggregate_coa_cropland_to_6_digit_naics, coa_irrigated_cropland_fba_cleanup
from flowsa.data_source_scripts.USDA_ERS_MLU import allocate_usda_ers_mlu_land_in_urban_areas, allocate_usda_ers_mlu_other_land,\
    allocate_usda_ers_mlu_land_in_rural_transportation_areas
from flowsa.data_source_scripts.USDA_IWMS import disaggregate_iwms_to_6_digit_naics
from flowsa.data_source_scripts.USGS_NWIS_WU import usgs_fba_data_cleanup, usgs_fba_w_sectors_data_cleanup


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
        # if yaml specifies a geoscale to load, use parameter to filter dataframe
        if 'source_fba_load_scale' in v:
            geo_level = v['source_fba_load_scale']
        else:
            geo_level = None
        log.info("Retrieving flowbyactivity for datasource " + k + " in year " + str(v['year']))
        flows_df = flowsa.getFlowByActivity(datasource=k, year=v['year'], flowclass=v['class'],
                                            geographic_level=geo_level)
    elif v['data_format'] == 'FBS':
        log.info("Retrieving flowbysector for datasource " + k)
        flows_df = flowsa.getFlowBySector(k)
    elif v['data_format'] == 'FBS_outside_flowsa':
        log.info("Retrieving flowbysector for datasource " + k)
        flows_df = getattr(sys.modules[__name__], v["FBS_datapull_fxn"])(v)
    else:
        log.error("Data format not specified in method file for datasource " + k)

    return flows_df


def main(**kwargs):
    """
    Creates a flowbysector dataset
    :param method_name: Name of method corresponding to flowbysector method yaml name
    :return: flowbysector
    """
    if len(kwargs) == 0:
        kwargs = parse_args()

    method_name = kwargs['method']
    # assign arguments
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

                log.info("Preparing to handle " + aset + " in " + k)                
                log.debug("Preparing to handle subset of activities: " + ', '.join(map(str, names)))
                # subset fba data by activity
                flows_subset = flows[(flows[fba_activity_fields[0]].isin(names)) |
                                     (flows[fba_activity_fields[1]].isin(names))].reset_index(drop=True)

                # extract relevant geoscale data or aggregate existing data
                log.info("Subsetting/aggregating " + k + " to " + attr['allocation_from_scale'] + " geoscale")
                flows_subset_geo = subset_df_by_geoscale(flows_subset, v['geoscale_to_use'],
                                                         attr['allocation_from_scale'])
                # if loading data subnational geoscale, check for data loss
                if attr['allocation_from_scale'] != 'national':
                    compare_geographic_totals(flows_subset_geo, flows_subset, k, method_name, aset)

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
                # rename SourceName to MetaSources
                flow_subset_mapped = flow_subset_mapped.rename(columns={'SourceName': 'MetaSources'})

                # if allocation method is "direct", then no need to create alloc ratios, else need to use allocation
                # dataframe to create sector allocation ratios
                if attr['allocation_method'] == 'direct':
                    fbs = direct_allocation_method(flow_subset_mapped, k, names, method)
                # if allocation method for an activity set requires a specific function due to the complicated nature
                # of the allocation, call on function here
                elif attr['allocation_method'] == 'allocation_function':
                    fbs = function_allocation_method(flow_subset_mapped, names, attr, fbs_list)
                else:
                    fbs = dataset_allocation_method(flow_subset_mapped, attr, names, method, k, v, aset, method_name, aset_names)

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
                log.info("Aggregating flowbysector to " + method['target_geoscale'] + " level")
                # determine from scale
                if fips_number_key[v['geoscale_to_use']] < fips_number_key[attr['allocation_from_scale']]:
                    from_scale = v['geoscale_to_use']
                else:
                    from_scale = attr['allocation_from_scale']

                fbs_geo_agg = agg_by_geoscale(fbs, from_scale, method['target_geoscale'], groupingcols)

                # aggregate data to every sector level
                log.info("Aggregating flowbysector to all sector levels")
                fbs_sec_agg = sector_aggregation(fbs_geo_agg, groupingcols)
                # add missing naics5/6 when only one naics5/6 associated with a naics4
                fbs_agg = sector_disaggregation(fbs_sec_agg, groupingdict)

                # check if any sector information is lost before reaching the target sector length, if so,
                # allocate values equally to disaggregated sectors
                log.debug('Checking for data at ' + method['target_sector_level'])
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

                # save comparison of FBA total to FBS total for an activity set
                compare_fba_load_and_fbs_output_totals(flows_subset_geo, fbs_sector_subset, aset, k,
                                                       method_name, attr, method, mapping_files)

                log.info("Completed flowbysector for " + aset)
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
    # add missing fields, ensure correct data type, add missing columns, reorder columns
    fbss = clean_df(fbss, flow_by_sector_fields, fbs_fill_na_dict)
    # prior to aggregating, replace MetaSources string with all sources that share context/flowable/sector values
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
    write_df_to_file(fbss,paths,meta)


if __name__ == '__main__':
    main()

