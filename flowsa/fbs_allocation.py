# fbs_allocation.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to allocate data using additional data sources
"""

import numpy as np
import pandas as pd
from flowsa.common import US_FIPS, fba_activity_fields, \
    fbs_activity_fields, fba_mapped_wsec_default_grouping_fields, \
    fba_wsec_default_grouping_fields, check_activities_sector_like, \
    return_bea_codes_used_as_naics
from flowsa.schema import activity_fields
from flowsa.settings import log
from flowsa.validation import check_allocation_ratios, \
    check_if_location_systems_match
from flowsa.flowbyfunctions import collapse_activity_fields, dynamically_import_fxn, \
    sector_aggregation, sector_disaggregation, subset_df_by_geoscale, \
    load_fba_w_standardized_units
from flowsa.allocation import allocate_by_sector, proportional_allocation_by_location_and_activity, \
    equally_allocate_parent_to_child_naics, equal_allocation
from flowsa.sectormapping import get_fba_allocation_subset, add_sectors_to_flowbyactivity
from flowsa.dataclean import replace_strings_with_NoneType
from flowsa.validation import check_if_data_exists_at_geoscale


def direct_allocation_method(fbs, k, names, method):
    """
    Directly assign activities to sectors
    :param fbs: df, FBA with flows converted using fedelemflowlist
    :param k: str, source name
    :param names: list, activity names in activity set
    :param method: dictionary, FBS method yaml
    :return: df with sector columns
    """
    log.info('Directly assigning activities to sectors')
    # for each activity, if activities are not sector like,
    # check that there is no data loss
    if check_activities_sector_like(k) is False:
        activity_list = []
        n_allocated = []
        for n in names:
            # avoid double counting by dropping n from the df after calling on
            # n, in the event both ACB and APB values exist
            fbs = fbs[~((fbs[fba_activity_fields[0]].isin(n_allocated)) |
                      (fbs[fba_activity_fields[1]].isin(n_allocated))
                        )].reset_index(drop=True)
            log.debug('Checking for %s at %s',
                      n, method['target_sector_level'])
            fbs_subset = \
                fbs[(fbs[fba_activity_fields[0]] == n) |
                    (fbs[fba_activity_fields[1]] == n)].reset_index(drop=True)
            # check if an Activity maps to more than one sector,
            # if so, equally allocate
            fbs_subset = equal_allocation(fbs_subset)
            fbs_subset = equally_allocate_parent_to_child_naics(fbs_subset, method['target_sector_level'])
            activity_list.append(fbs_subset)
            n_allocated.append(n)
        fbs = pd.concat(activity_list, ignore_index=True)
    return fbs


def function_allocation_method(flow_subset_mapped, k, names, attr, fbs_list):
    """
    Allocate df activities to sectors using a function identified
    in the FBS method yaml
    :param flow_subset_mapped: df, FBA with flows converted using
        fedelemflowlist
    :param k: str, source name
    :param names: list, activity names in activity set
    :param attr: dictionary, attribute data from method yaml for activity set
    :param fbs_list: list, fbs dfs created running flowbysector.py
    :return: df, FBS, with allocated activity columns to sectors
    """
    log.info('Calling on function specified in method yaml to allocate '
             '%s to sectors', ', '.join(map(str, names)))
    fbs = dynamically_import_fxn(
        k, attr['allocation_source'])(flow_subset_mapped, attr, fbs_list)
    return fbs


def dataset_allocation_method(flow_subset_mapped, attr, names, method,
                              k, v, aset, aset_names, download_FBA_if_missing):
    """
    Method of allocation using a specified data source
    :param flow_subset_mapped: FBA subset mapped using federal
        elementary flow list
    :param attr: dictionary, attribute data from method yaml for activity set
    :param names: list, activity names in activity set
    :param method: dictionary, FBS method yaml
    :param k: str, the datasource name
    :param v: dictionary, the datasource parameters
    :param aset: dictionary items for FBS method yaml
    :param aset_names: list, activity set names
    :param download_FBA_if_missing: bool, indicate if missing FBAs
       should be downloaded from Data Commons
    :return: df, allocated activity names
    """

    from flowsa.validation import compare_df_units

    # add parameters to dictionary if exist in method yaml
    fba_dict = {}
    if 'allocation_flow' in attr:
        fba_dict['flowname_subset'] = attr['allocation_flow']
    if 'allocation_compartment' in attr:
        fba_dict['compartment_subset'] = attr['allocation_compartment']
    if 'clean_allocation_fba' in attr:
        fba_dict['clean_fba'] = attr['clean_allocation_fba']
    if 'clean_allocation_fba_w_sec' in attr:
        fba_dict['clean_fba_w_sec'] = attr['clean_allocation_fba_w_sec']

    # load the allocation FBA
    fba_allocation_wsec = \
        load_map_clean_fba(method, attr,
                           fba_sourcename=attr['allocation_source'],
                           df_year=attr['allocation_source_year'],
                           flowclass=attr['allocation_source_class'],
                           geoscale_from=attr['allocation_from_scale'],
                           geoscale_to=v['geoscale_to_use'],
                           download_FBA_if_missing=download_FBA_if_missing,
                           **fba_dict)

    # subset fba datasets to only keep the sectors associated
    # with activity subset
    log.info("Subsetting %s for sectors in %s", attr['allocation_source'], k)
    fba_allocation_subset = \
        get_fba_allocation_subset(fba_allocation_wsec, k, names,
                                  flowSubsetMapped=flow_subset_mapped,
                                  allocMethod=attr['allocation_method'])

    # if there is an allocation helper dataset, modify allocation df
    if 'helper_source' in attr:
        log.info("Using the specified allocation help for subset of %s",
                 attr['allocation_source'])
        fba_allocation_subset = \
            allocation_helper(fba_allocation_subset, attr, method, v,
                              download_FBA_if_missing=download_FBA_if_missing)

    # create flow allocation ratios for each activity
    flow_alloc_list = []
    if 'Context' in fba_allocation_subset.columns:
        group_cols = fba_mapped_wsec_default_grouping_fields
    else:
        group_cols = fba_wsec_default_grouping_fields
    group_cols = [e for e in group_cols if e not in
                  ('ActivityProducedBy', 'ActivityConsumedBy')]
    n_allocated = []
    for n in names:
        log.debug("Creating allocation ratios for %s", n)
        # if n has already been called, drop all rows of data
        # containing n to avoid double counting when there are two
        # activities in each ACB and APB columns
        fba_allocation_subset = fba_allocation_subset[
            ~((fba_allocation_subset[
                   fba_activity_fields[0]].isin(n_allocated)) |
              (fba_allocation_subset[fba_activity_fields[1]].isin(n_allocated))
              )].reset_index(drop=True)
        fba_allocation_subset_2 = \
            get_fba_allocation_subset(fba_allocation_subset, k, [n],
                                      flowSubsetMapped=flow_subset_mapped,
                                      allocMethod=attr['allocation_method'],
                                      activity_set_names=aset_names)
        if len(fba_allocation_subset_2) == 0:
            log.info("No data found to allocate %s", n)
        else:
            flow_alloc = \
                allocate_by_sector(fba_allocation_subset_2, attr,
                                   attr['allocation_method'], group_cols,
                                   flowSubsetMapped=flow_subset_mapped)
            flow_alloc = flow_alloc.assign(FBA_Activity=n)
            n_allocated.append(n)
            flow_alloc_list.append(flow_alloc)
    flow_allocation = pd.concat(flow_alloc_list, ignore_index=True)

    # generalize activity field names to enable link to main fba source
    log.info("Generalizing activity columns in subset of %s",
             attr['allocation_source'])
    flow_allocation = collapse_activity_fields(flow_allocation)

    # check for issues with allocation ratios
    check_allocation_ratios(flow_allocation, aset, method, attr)

    # create list of sectors in the flow allocation df,
    # drop any rows of data in the flow df that aren't in list
    sector_list = flow_allocation['Sector'].unique().tolist()

    # subset fba allocation table to the values in the activity
    # list, based on overlapping sectors
    flow_subset_mapped = flow_subset_mapped.loc[
        (flow_subset_mapped[fbs_activity_fields[0]].isin(sector_list)) |
        (flow_subset_mapped[fbs_activity_fields[1]].isin(sector_list))]

    # check if fba and allocation dfs have the same LocationSystem
    log.info("Checking if flowbyactivity and allocation "
             "dataframes use the same location systems")
    check_if_location_systems_match(flow_subset_mapped, flow_allocation)

    # merge fba df w/flow allocation dataset
    log.info("Merge %s and subset of %s", k, attr['allocation_source'])
    for i, j in activity_fields.items():
        # check units
        compare_df_units(flow_subset_mapped, flow_allocation)
        # create list of columns to merge on
        if 'allocation_merge_columns' in attr:
            fa_cols = \
                ['Location', 'Sector', 'FlowAmountRatio', 'FBA_Activity'] + \
                attr['allocation_merge_columns']
            l_cols = \
                ['Location', j[1]["flowbysector"], j[0]["flowbyactivity"]] + \
                attr['allocation_merge_columns']
            r_cols = ['Location', 'Sector', 'FBA_Activity'] + \
                     attr['allocation_merge_columns']
        else:
            fa_cols = ['Location', 'Sector', 'FlowAmountRatio', 'FBA_Activity']
            l_cols = ['Location', j[1]["flowbysector"], j[0]["flowbyactivity"]]
            r_cols = ['Location', 'Sector', 'FBA_Activity']
        flow_subset_mapped = \
            flow_subset_mapped.merge(flow_allocation[fa_cols], left_on=l_cols,
                                     right_on=r_cols, how='left')

    # merge the flowamount columns
    flow_subset_mapped.loc[:, 'FlowAmountRatio'] =\
        flow_subset_mapped['FlowAmountRatio_x'].fillna(
            flow_subset_mapped['FlowAmountRatio_y'])
    # fill null rows with 0 because no allocation info
    flow_subset_mapped['FlowAmountRatio'] = \
        flow_subset_mapped['FlowAmountRatio'].fillna(0)

    # drop rows where there is no allocation data
    fbs = flow_subset_mapped.dropna(
        subset=['Sector_x', 'Sector_y'], how='all').reset_index()

    # calculate flow amounts for each sector
    log.info("Calculating new flow amounts using flow ratios")
    fbs.loc[:, 'FlowAmount'] = fbs['FlowAmount'] * fbs['FlowAmountRatio']

    # drop columns
    log.info("Cleaning up new flow by sector")
    fbs = fbs.drop(columns=['Sector_x', 'FlowAmountRatio_x', 'Sector_y',
                            'FlowAmountRatio_y', 'FlowAmountRatio',
                            'FBA_Activity_x', 'FBA_Activity_y'])
    return fbs


def allocation_helper(df_w_sector, attr, method, v, download_FBA_if_missing):
    """
    Function to help allocate activity names using secondary df
    :param df_w_sector: df, includes sector columns
    :param attr: dictionary, attribute data from method yaml for activity set
    :param method: dictionary, FBS method yaml
    :param v: dictionary, the datasource parameters
    :param download_FBA_if_missing: bool, indicate if missing FBAs
       should be downloaded from Data Commons or run locally
    :return: df, with modified fba allocation values
    """
    from flowsa.validation import compare_df_units

    # add parameters to dictionary if exist in method yaml
    fba_dict = {}
    if 'helper_flow' in attr:
        fba_dict['flowname_subset'] = attr['helper_flow']
    if 'clean_helper_fba' in attr:
        fba_dict['clean_fba'] = attr['clean_helper_fba']
    if 'clean_helper_fba_wsec' in attr:
        fba_dict['clean_fba_w_sec'] = attr['clean_helper_fba_wsec']

    # load the allocation FBA
    helper_allocation = \
        load_map_clean_fba(method, attr, fba_sourcename=attr['helper_source'],
                           df_year=attr['helper_source_year'],
                           flowclass=attr['helper_source_class'],
                           geoscale_from=attr['helper_from_scale'],
                           geoscale_to=v['geoscale_to_use'],
                           download_FBA_if_missing=download_FBA_if_missing,
                           **fba_dict)

    # run sector disagg to capture any missing lower level naics
    helper_allocation = sector_disaggregation(helper_allocation)

    # generalize activity field names to enable link to water withdrawal table
    helper_allocation = collapse_activity_fields(helper_allocation)
    # drop any rows not mapped
    helper_allocation = \
        helper_allocation[helper_allocation['Sector'].notnull()]
    # drop columns
    helper_allocation = \
        helper_allocation.drop(columns=['Activity', 'Min', 'Max'])

    # rename column
    helper_allocation = \
        helper_allocation.rename(columns={"FlowAmount": 'HelperFlow'})

    # determine the df_w_sector column to merge on
    df_w_sector = replace_strings_with_NoneType(df_w_sector)
    sec_consumed_list = \
        df_w_sector['SectorConsumedBy'].drop_duplicates().values.tolist()
    sec_produced_list = \
        df_w_sector['SectorProducedBy'].drop_duplicates().values.tolist()
    # if a sector field column is not all 'none', that is the column to merge
    if all(v is None for v in sec_consumed_list):
        sector_col_to_merge = 'SectorProducedBy'
    elif all(v is None for v in sec_produced_list):
        sector_col_to_merge = 'SectorConsumedBy'
    else:
        log.error('There is not a clear sector column to base '
                  'merge with helper allocation dataset')

    # merge allocation df with helper df based on sectors,
    # depending on geo scales of dfs
    if (attr['helper_from_scale'] == 'state') and \
            (attr['allocation_from_scale'] == 'county'):
        helper_allocation.loc[:, 'Location_tmp'] = \
            helper_allocation['Location'].apply(lambda x: x[0:2])
        df_w_sector.loc[:, 'Location_tmp'] = \
            df_w_sector['Location'].apply(lambda x: x[0:2])
        # merge_columns.append('Location_tmp')
        compare_df_units(df_w_sector, helper_allocation)
        modified_fba_allocation =\
            df_w_sector.merge(
                helper_allocation[['Location_tmp', 'Sector', 'HelperFlow']],
                how='left',
                left_on=['Location_tmp', sector_col_to_merge],
                right_on=['Location_tmp', 'Sector'])
        modified_fba_allocation = \
            modified_fba_allocation.drop(columns=['Location_tmp'])
    elif (attr['helper_from_scale'] == 'national') and \
            (attr['allocation_from_scale'] != 'national'):
        compare_df_units(df_w_sector, helper_allocation)
        modified_fba_allocation = \
            df_w_sector.merge(helper_allocation[['Sector', 'HelperFlow']],
                              how='left',
                              left_on=[sector_col_to_merge],
                              right_on=['Sector'])
    else:

        compare_df_units(df_w_sector, helper_allocation)
        modified_fba_allocation =\
            df_w_sector.merge(
                helper_allocation[['Location', 'Sector', 'HelperFlow']],
                left_on=['Location', sector_col_to_merge],
                right_on=['Location', 'Sector'],
                how='left')
        # load bea codes that sub for naics
        bea = return_bea_codes_used_as_naics()
        # replace sector column and helperflow value if the sector column to
        # merge is in the bea list to prevent dropped data
        modified_fba_allocation['Sector'] = \
            np.where(modified_fba_allocation[sector_col_to_merge].isin(bea),
                     modified_fba_allocation[sector_col_to_merge],
                     modified_fba_allocation['Sector'])
        modified_fba_allocation['HelperFlow'] = \
            np.where(modified_fba_allocation[sector_col_to_merge].isin(bea),
                     modified_fba_allocation['FlowAmount'],
                     modified_fba_allocation['HelperFlow'])

    # modify flow amounts using helper data
    if 'multiplication' in attr['helper_method']:
        # if missing values (na or 0), replace with national level values
        replacement_values =\
            helper_allocation[helper_allocation['Location'] ==
                              US_FIPS].reset_index(drop=True)
        replacement_values = \
            replacement_values.rename(
                columns={"HelperFlow": 'ReplacementValue'})
        compare_df_units(modified_fba_allocation, replacement_values)
        modified_fba_allocation = modified_fba_allocation.merge(
            replacement_values[['Sector', 'ReplacementValue']], how='left')
        modified_fba_allocation.loc[:, 'HelperFlow'] = \
            modified_fba_allocation['HelperFlow'].fillna(
            modified_fba_allocation['ReplacementValue'])
        modified_fba_allocation.loc[:, 'HelperFlow'] =\
            np.where(modified_fba_allocation['HelperFlow'] == 0,
                     modified_fba_allocation['ReplacementValue'],
                     modified_fba_allocation['HelperFlow'])

        # replace non-existent helper flow values with a 0,
        # so after multiplying, don't have incorrect value associated with
        # new unit
        modified_fba_allocation['HelperFlow'] =\
            modified_fba_allocation['HelperFlow'].fillna(value=0)
        modified_fba_allocation.loc[:, 'FlowAmount'] = \
            modified_fba_allocation['FlowAmount'] * \
            modified_fba_allocation['HelperFlow']
        # drop columns
        modified_fba_allocation =\
            modified_fba_allocation.drop(
                columns=["HelperFlow", 'ReplacementValue', 'Sector'])

    elif attr['helper_method'] == 'proportional':
        modified_fba_allocation =\
            proportional_allocation_by_location_and_activity(
                modified_fba_allocation, sector_col_to_merge)
        modified_fba_allocation['FlowAmountRatio'] =\
            modified_fba_allocation['FlowAmountRatio'].fillna(0)
        modified_fba_allocation.loc[:, 'FlowAmount'] = \
            modified_fba_allocation['FlowAmount'] * \
            modified_fba_allocation['FlowAmountRatio']
        modified_fba_allocation =\
            modified_fba_allocation.drop(
                columns=['FlowAmountRatio', 'HelperFlow', 'Sector'])

    elif attr['helper_method'] == 'proportional-flagged':
        # calculate denominators based on activity and 'flagged' column
        modified_fba_allocation =\
            modified_fba_allocation.assign(
                Denominator=modified_fba_allocation.groupby(
                    ['FlowName', 'ActivityConsumedBy', 'Location',
                     'disaggregate_flag'])['HelperFlow'].transform('sum'))
        modified_fba_allocation = modified_fba_allocation.assign(
            FlowAmountRatio=modified_fba_allocation['HelperFlow'] /
                            modified_fba_allocation['Denominator'])
        modified_fba_allocation =\
            modified_fba_allocation.assign(
                FlowAmount=modified_fba_allocation['FlowAmount'] *
                           modified_fba_allocation['FlowAmountRatio'])
        modified_fba_allocation =\
            modified_fba_allocation.drop(
                columns=['disaggregate_flag', 'Sector', 'HelperFlow',
                         'Denominator', 'FlowAmountRatio'])
        # run sector aggregation
        modified_fba_allocation = \
            sector_aggregation(modified_fba_allocation,
                               fba_wsec_default_grouping_fields)

    # drop rows of 0
    modified_fba_allocation =\
        modified_fba_allocation[
            modified_fba_allocation['FlowAmount'] != 0].reset_index(drop=True)

    modified_fba_allocation.loc[
        modified_fba_allocation['Unit'] == 'gal/employee', 'Unit'] = 'gal'

    # option to scale up fba values
    if 'scaled' in attr['helper_method']:
        log.info("Scaling %s to FBA values", attr['helper_source'])
        modified_fba_allocation = \
            dynamically_import_fxn(
                attr['allocation_source'], attr["scale_helper_results"])(
                modified_fba_allocation, attr,
                download_FBA_if_missing=download_FBA_if_missing)
    return modified_fba_allocation


def load_map_clean_fba(method, attr, fba_sourcename, df_year, flowclass,
                       geoscale_from, geoscale_to, **kwargs):
    """
    Load, clean, and map a FlowByActivity df
    :param method: dictionary, FBS method yaml
    :param attr: dictionary, attribute data from method yaml for activity set
    :param fba_sourcename: str, source name
    :param df_year: str, year
    :param flowclass: str, flowclass to subset df with
    :param geoscale_from: str, geoscale to use
    :param geoscale_to: str, geoscale to aggregate to
    :param kwargs: dictionary, can include parameters: 'allocation_flow',
                   'allocation_compartment','clean_allocation_fba',
                   'clean_allocation_fba_w_sec'
    :return: df, fba format
    """
    # dictionary to load/standardize fba
    kwargs_dict = {}
    if 'download_FBA_if_missing' in kwargs:
        kwargs_dict['download_FBA_if_missing'] = \
            kwargs['download_FBA_if_missing']
    if 'allocation_map_to_flow_list' in attr:
        kwargs_dict['allocation_map_to_flow_list'] = \
            attr['allocation_map_to_flow_list']

    log.info("Loading allocation flowbyactivity %s for year %s",
             fba_sourcename, str(df_year))
    fba = load_fba_w_standardized_units(datasource=fba_sourcename,
                                        year=df_year,
                                        flowclass=flowclass,
                                        **kwargs_dict
                                        )

    # check if allocation data exists at specified geoscale to use
    log.info("Checking if allocation data exists at the %s level",
             geoscale_from)
    check_if_data_exists_at_geoscale(fba, geoscale_from)

    # aggregate geographically to the scale of the flowbyactivty source,
    # if necessary
    fba = subset_df_by_geoscale(fba, geoscale_from, geoscale_to)

    # subset based on yaml settings
    if 'flowname_subset' in kwargs:
        if kwargs['flowname_subset'] != 'None':
            fba = fba.loc[fba['FlowName'].isin(kwargs['flowname_subset'])]
    if 'compartment_subset' in kwargs:
        if kwargs['compartment_subset'] != 'None':
            fba = \
                fba.loc[fba['Compartment'].isin(kwargs['compartment_subset'])]

    # cleanup the fba allocation df, if necessary
    if 'clean_fba' in kwargs:
        log.info("Cleaning %s", fba_sourcename)
        fba = dynamically_import_fxn(fba_sourcename, kwargs["clean_fba"])(
            fba, attr=attr,
            download_FBA_if_missing=kwargs['download_FBA_if_missing'])
    # reset index
    fba = fba.reset_index(drop=True)

    # assign sector to allocation dataset
    log.info("Adding sectors to %s", fba_sourcename)
    fba_wsec = add_sectors_to_flowbyactivity(fba, sectorsourcename=method[
        'target_sector_source'])

    # call on fxn to further clean up/disaggregate the fba
    # allocation data, if exists
    if 'clean_fba_w_sec' in kwargs:
        log.info("Further disaggregating sectors in %s", fba_sourcename)
        fba_wsec = dynamically_import_fxn(
            fba_sourcename, kwargs['clean_fba_w_sec'])(
            fba_wsec, attr=attr, method=method, sourcename=fba_sourcename,
            download_FBA_if_missing=kwargs['download_FBA_if_missing'])

    return fba_wsec
