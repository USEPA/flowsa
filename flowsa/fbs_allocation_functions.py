import logging as log
import sys
import pandas as pd
import flowsa
from flowsa.common import load_source_catalog, flow_by_activity_fields, activity_fields
from flowsa.datachecks import check_if_losing_sector_data, check_if_data_exists_at_geoscale, check_allocation_ratios, \
    check_if_location_systems_match
from flowsa.flowbyfunctions import fba_activity_fields, clean_df, fba_fill_na_dict, harmonize_units, \
    subset_df_by_geoscale, allocation_helper, fba_mapped_default_grouping_fields, allocate_by_sector, \
    collapse_activity_fields, fbs_activity_fields
from flowsa.mapping import add_sectors_to_flowbyactivity, get_fba_allocation_subset

# import specific functions
from flowsa.data_source_scripts.BEA import subset_BEA_Use
from flowsa.data_source_scripts.Blackhurst_IO import convert_blackhurst_data_to_gal_per_year, convert_blackhurst_data_to_gal_per_employee
from flowsa.data_source_scripts.BLS_QCEW import clean_bls_qcew_fba, clean_bls_qcew_fba_for_employment_sat_table, \
    bls_clean_allocation_fba_w_sec
from flowsa.data_source_scripts.EIA_CBECS_Land import cbecs_land_fba_cleanup
from flowsa.data_source_scripts.EIA_MECS import mecs_energy_fba_cleanup, eia_mecs_energy_clean_allocation_fba_w_sec, \
    mecs_land_fba_cleanup, mecs_land_fba_cleanup_for_land_2012_fbs, mecs_land_clean_allocation_mapped_fba_w_sec
from flowsa.data_source_scripts.EPA_NEI import clean_NEI_fba, clean_NEI_fba_no_pesticides
from flowsa.data_source_scripts.StatCan_IWS_MI import convert_statcan_data_to_US_water_use, disaggregate_statcan_to_naics_6
from flowsa.data_source_scripts.stewiFBS import stewicombo_to_sector, stewi_to_sector
from flowsa.data_source_scripts.USDA_CoA_Cropland import disaggregate_coa_cropland_to_6_digit_naics, coa_irrigated_cropland_fba_cleanup
from flowsa.data_source_scripts.USDA_ERS_MLU import allocate_usda_ers_mlu_land_in_urban_areas, allocate_usda_ers_mlu_other_land,\
    allocate_usda_ers_mlu_land_in_rural_transportation_areas
from flowsa.data_source_scripts.USDA_IWMS import disaggregate_iwms_to_6_digit_naics
from flowsa.data_source_scripts.USGS_NWIS_WU import usgs_fba_data_cleanup, usgs_fba_w_sectors_data_cleanup


def direct_allocation_method(flow_subset_mapped, k, names, method):
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
    return fbs


def function_allocation_method(flow_subset_mapped, names, attr, fbs_list):
    log.info('Calling on function specified in method yaml to allocate ' +
             ', '.join(map(str, names)) + ' to sectors')
    fbs = getattr(sys.modules[__name__], attr['allocation_source'])(flow_subset_mapped, attr, fbs_list)
    return fbs


def dataset_allocation_method(flow_subset_mapped, attr, names, method, k, v, aset, method_name, aset_names):
    # determine appropriate allocation dataset
    log.info("Loading allocation flowbyactivity " + attr['allocation_source'] + " for year " +
             str(attr['allocation_source_year']))
    fba_allocation = flowsa.getFlowByActivity(datasource=attr['allocation_source'],
                                              year=attr['allocation_source_year'],
                                              flowclass=attr['allocation_source_class'])
    fba_allocation = clean_df(fba_allocation, flow_by_activity_fields, fba_fill_na_dict)
    fba_allocation = harmonize_units(fba_allocation)

    # check if allocation data exists at specified geoscale to use
    log.info("Checking if allocation data exists at the " + attr['allocation_from_scale'] + " level")
    check_if_data_exists_at_geoscale(fba_allocation, attr['allocation_from_scale'])

    # aggregate geographically to the scale of the flowbyactivty source, if necessary
    fba_allocation = subset_df_by_geoscale(fba_allocation, attr['allocation_from_scale'], v['geoscale_to_use'])

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
                                                            allocMethod=attr['allocation_method'],
                                                            activity_set_names=aset_names)
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
    for i, j in activity_fields.items():
        flow_subset_mapped = flow_subset_mapped.merge(
            flow_allocation[['Location', 'Sector', 'FlowAmountRatio', 'FBA_Activity']],
            left_on=['Location', j[1]["flowbysector"], j[0]["flowbyactivity"]],
            right_on=['Location', 'Sector', 'FBA_Activity'], how='left')

    # merge the flowamount columns
    flow_subset_mapped.loc[:, 'FlowAmountRatio'] = flow_subset_mapped['FlowAmountRatio_x'].fillna(flow_subset_mapped['FlowAmountRatio_y'])
    # fill null rows with 0 because no allocation info
    flow_subset_mapped['FlowAmountRatio'] = flow_subset_mapped['FlowAmountRatio'].fillna(0)

    # check if fba and alloc dfs have data for same geoscales - comment back in after address the 'todo'
    # log.info("Checking if flowbyactivity and allocation dataframes have data at the same locations")
    # check_if_data_exists_for_same_geoscales(fbs, k, attr['names'])

    # drop rows where there is no allocation data
    fbs = flow_subset_mapped.dropna(subset=['Sector_x', 'Sector_y'], how='all').reset_index()

    # calculate flow amounts for each sector
    log.info("Calculating new flow amounts using flow ratios")
    fbs.loc[:, 'FlowAmount'] = fbs['FlowAmount'] * fbs['FlowAmountRatio']

    # drop columns
    log.info("Cleaning up new flow by sector")
    fbs = fbs.drop(columns=['Sector_x', 'FlowAmountRatio_x', 'Sector_y', 'FlowAmountRatio_y',
                            'FlowAmountRatio', 'FBA_Activity_x', 'FBA_Activity_y'])
    return fbs
