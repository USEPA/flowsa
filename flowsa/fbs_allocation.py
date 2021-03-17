
import logging as log
import numpy as np
import pandas as pd
import sys
from flowsa.common import load_source_catalog, activity_fields, US_FIPS
from flowsa.datachecks import check_if_losing_sector_data, check_allocation_ratios, \
    check_if_location_systems_match
from flowsa.flowbyfunctions import fba_activity_fields, fba_mapped_default_grouping_fields, collapse_activity_fields, \
    fbs_activity_fields, sector_aggregation, sector_disaggregation
from flowsa.mapping import get_fba_allocation_subset
from flowsa.dataclean import load_map_clean_fba, replace_strings_with_NoneType, \
    replace_NoneType_with_empty_cells

# import specific functions
from flowsa.data_source_scripts.BEA import subset_BEA_Use
from flowsa.data_source_scripts.Blackhurst_IO import convert_blackhurst_data_to_gal_per_year, \
    convert_blackhurst_data_to_gal_per_employee, scale_blackhurst_results_to_usgs_values
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
    fba_allocation_wsec = load_map_clean_fba(method, attr, fba_sourcename=attr['allocation_source'],
                                             df_year=attr['allocation_source_year'],
                                             flowclass=attr['allocation_source_class'],
                                             geoscale_from=attr['allocation_from_scale'],
                                             geoscale_to=v['geoscale_to_use'], **fba_dict)

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


def allocation_helper(df_w_sector, attr, method, v):
    """
    Used when two df required to create allocation ratio
    :param df_w_sector:
    :param method: currently written for 'multiplication' and 'proportional'
    :param attr:
    :return:
    """

    # add parameters to dictionary if exist in method yaml
    fba_dict = {}
    if 'helper_flow' in attr:
        fba_dict['flowname_subset'] = attr['helper_flow']
    if 'clean_helper_fba' in attr:
        fba_dict['clean_fba'] = attr['clean_helper_fba']
    if 'clean_helper_fba_wsec' in attr:
        fba_dict['clean_fba_w_sec'] = attr['clean_helper_fba_wsec']

    # load the allocation FBA
    helper_allocation = load_map_clean_fba(method, attr, fba_sourcename=attr['helper_source'],
                                           df_year=attr['helper_source_year'],
                                           flowclass=attr['helper_source_class'],
                                           geoscale_from=attr['helper_from_scale'],
                                           geoscale_to=v['geoscale_to_use'], **fba_dict)

    # run sector disagg to capture any missing lower level naics
    helper_allocation = sector_disaggregation(helper_allocation, fba_mapped_default_grouping_fields)

    # generalize activity field names to enable link to water withdrawal table
    helper_allocation = collapse_activity_fields(helper_allocation)
    # drop any rows not mapped
    helper_allocation = helper_allocation[helper_allocation['Sector'].notnull()]
    # drop columns
    helper_allocation = helper_allocation.drop(columns=['Activity', 'Min', 'Max'])

    # rename column
    helper_allocation = helper_allocation.rename(columns={"FlowAmount": 'HelperFlow'})

    # determine the df_w_sector column to merge on
    df_w_sector = replace_strings_with_NoneType(df_w_sector)
    sec_consumed_list = df_w_sector['SectorConsumedBy'].drop_duplicates().values.tolist()
    sec_produced_list = df_w_sector['SectorProducedBy'].drop_duplicates().values.tolist()
    # if a sector field column is not all 'none', that is the column to merge
    if all(v is None for v in sec_consumed_list):
        sector_col_to_merge = 'SectorProducedBy'
    elif all(v is None for v in sec_produced_list):
        sector_col_to_merge = 'SectorConsumedBy'
    else:
        log.error('There is not a clear sector column to base merge with helper allocation dataset')

    # merge allocation df with helper df based on sectors, depending on geo scales of dfs
    if (attr['helper_from_scale'] == 'state') and (attr['allocation_from_scale'] == 'county'):
        helper_allocation.loc[:, 'Location_tmp'] = helper_allocation['Location'].apply(lambda x: x[0:2])
        df_w_sector.loc[:, 'Location_tmp'] = df_w_sector['Location'].apply(lambda x: x[0:2])
        # merge_columns.append('Location_tmp')
        modified_fba_allocation = df_w_sector.merge(helper_allocation[['Location_tmp', 'Sector', 'HelperFlow']],
                                                    how='left',
                                                    left_on=['Location_tmp', sector_col_to_merge],
                                                    right_on=['Location_tmp', 'Sector'])
        modified_fba_allocation = modified_fba_allocation.drop(columns=['Location_tmp'])
    elif (attr['helper_from_scale'] == 'national') and (attr['allocation_from_scale'] != 'national'):
        modified_fba_allocation = df_w_sector.merge(helper_allocation[['Sector', 'HelperFlow']],
                                                    how='left',
                                                    left_on=[sector_col_to_merge],
                                                    right_on=['Sector'])
    else:
        modified_fba_allocation = df_w_sector.merge(helper_allocation[['Location', 'Sector', 'HelperFlow']],
                                                    how='left',
                                                    left_on=['Location', sector_col_to_merge],
                                                    right_on=['Location', 'Sector'])

    # modify flow amounts using helper data
    if 'multiplication' in attr['helper_method']:
        # todo: modify so if missing data, replaced with value from one geoscale up instead of national
        # todo: modify year after merge if necessary
        # if missing values (na or 0), replace with national level values
        replacement_values = helper_allocation[helper_allocation['Location'] == US_FIPS].reset_index(
            drop=True)
        replacement_values = replacement_values.rename(columns={"HelperFlow": 'ReplacementValue'})
        modified_fba_allocation = modified_fba_allocation.merge(
            replacement_values[['Sector', 'ReplacementValue']], how='left')
        modified_fba_allocation.loc[:, 'HelperFlow'] = modified_fba_allocation['HelperFlow'].fillna(
            modified_fba_allocation['ReplacementValue'])
        modified_fba_allocation.loc[:, 'HelperFlow'] = np.where(modified_fba_allocation['HelperFlow'] == 0,
                                                                modified_fba_allocation['ReplacementValue'],
                                                                modified_fba_allocation['HelperFlow'])

        # replace non-existent helper flow values with a 0, so after multiplying, don't have incorrect value associated
        # with new unit
        modified_fba_allocation['HelperFlow'] = modified_fba_allocation['HelperFlow'].fillna(value=0)
        modified_fba_allocation.loc[:, 'FlowAmount'] = modified_fba_allocation['FlowAmount'] * \
                                                       modified_fba_allocation['HelperFlow']
        # drop columns
        modified_fba_allocation = modified_fba_allocation.drop(columns=["HelperFlow", 'ReplacementValue', 'Sector'])

    elif attr['helper_method'] == 'proportional':
        modified_fba_allocation = proportional_allocation_by_location_and_activity(modified_fba_allocation,
                                                                                   sector_col_to_merge,
                                                                                   attr['helper_method'])
        modified_fba_allocation['FlowAmountRatio'] = modified_fba_allocation['FlowAmountRatio'].fillna(0)
        modified_fba_allocation.loc[:, 'FlowAmount'] = modified_fba_allocation['FlowAmount'] * \
                                                       modified_fba_allocation['FlowAmountRatio']
        modified_fba_allocation = modified_fba_allocation.drop(columns=['FlowAmountRatio', 'HelperFlow', 'Sector'])

    elif attr['helper_method'] == 'proportional-flagged':
        # calculate denominators based on activity and 'flagged' column
        modified_fba_allocation = modified_fba_allocation.assign(Denominator=
                                                                  modified_fba_allocation.groupby(
                                                                      ['FlowName', 'ActivityConsumedBy', 'Location',
                                                                       'disaggregate_flag']
                                                                  )['HelperFlow'].transform('sum'))
        modified_fba_allocation = modified_fba_allocation.assign(
            FlowAmountRatio=modified_fba_allocation['HelperFlow'] / modified_fba_allocation['Denominator'])
        modified_fba_allocation = modified_fba_allocation.assign(FlowAmount=modified_fba_allocation['FlowAmount'] *
                                                                            modified_fba_allocation['FlowAmountRatio'])
        modified_fba_allocation = modified_fba_allocation.drop(columns=['disaggregate_flag', 'Sector', 'HelperFlow',
                                                                        'Denominator', 'FlowAmountRatio'])
        # run sector aggregation
        modified_fba_allocation = sector_aggregation(modified_fba_allocation, fba_mapped_default_grouping_fields)

    # drop rows of 0
    modified_fba_allocation = modified_fba_allocation[modified_fba_allocation['FlowAmount'] != 0].reset_index(drop=True)

    # todo: change units
    modified_fba_allocation.loc[modified_fba_allocation['Unit'] == 'gal/employee', 'Unit'] = 'gal'

    # option to scale up fba values
    if 'scaled' in attr['helper_method']:
        log.info("Scaling " + attr['helper_source'] + ' to FBA values')
        # tmp hard coded - need to generalize
        if attr['helper_source'] == 'BLS_QCEW':
            modified_fba_allocation = scale_blackhurst_results_to_usgs_values(modified_fba_allocation, attr)
            # modified_fba_allocation = getattr(sys.modules[__name__], attr["scale_helper_results"])(modified_fba_allocation, attr)

    return modified_fba_allocation


def allocate_by_sector(df_w_sectors, source_name, allocation_source, allocation_method, group_cols, **kwargs):
    """
    Create an allocation ratio for df

    :param df_w_sectors: df with column of sectors
    :param source_name: the name of the FBA df being allocated
    :param allocation_source: The name of the FBA allocation dataframe
    :param allocation_method: currently written for 'proportional'
    :param group_cols: columns on which to base aggregation and disaggregation
    :return: df with FlowAmountRatio for each sector
    """

    # first determine if there is a special case with how the allocation ratios are created
    if allocation_method == 'proportional-flagged':
        # if the allocation method is flagged, subset sectors that are flagged/notflagged, where nonflagged sectors \
        # have flowamountratio=1
        if kwargs != {}:
            if 'flowSubsetMapped' in kwargs:
                fsm = kwargs['flowSubsetMapped']
                flagged = fsm[fsm['disaggregate_flag'] == 1]
                #todo: change col to be generalized, not SEctorConsumedBy specific
                flagged_names = flagged['SectorConsumedBy'].tolist()

                nonflagged = fsm[fsm['disaggregate_flag'] == 0]
                nonflagged_names = nonflagged['SectorConsumedBy'].tolist()

                # subset the original df so rows of data that run through the proportioanl allocation process are
                # sectors included in the flagged list
                df_w_sectors_nonflagged = df_w_sectors.loc[
                    (df_w_sectors[fbs_activity_fields[0]].isin(nonflagged_names)) |
                    (df_w_sectors[fbs_activity_fields[1]].isin(nonflagged_names))
                    ].reset_index(drop=True)
                df_w_sectors_nonflagged = df_w_sectors_nonflagged.assign(FlowAmountRatio=1)

                df_w_sectors = df_w_sectors.loc[(df_w_sectors[fbs_activity_fields[0]].isin(flagged_names)) |
                                                (df_w_sectors[fbs_activity_fields[1]].isin(flagged_names))
                                                ].reset_index(drop=True)
            else:
                log.error('The proportional-flagged allocation method requires a column "disaggregate_flag" in the'
                          'flow_subset_mapped df')

    # run sector aggregation fxn to determine total flowamount for each level of sector
    if len(df_w_sectors) == 0:
        allocation_df = df_w_sectors_nonflagged.copy()
    else:
        df1 = sector_aggregation(df_w_sectors, group_cols)
        # run sector disaggregation to capture one-to-one naics4/5/6 relationships
        df2 = sector_disaggregation(df1, group_cols)

        # if statements for method of allocation
        # either 'proportional' or 'proportional-flagged'
        allocation_df = []
        if allocation_method == 'proportional' or allocation_method == 'proportional-flagged':
            allocation_df = proportional_allocation_by_location(df2)
        else:
            log.error('Must create function for specified method of allocation')

        if allocation_method == 'proportional-flagged':
            # drop rows where values are not in flagged names
            allocation_df = allocation_df.loc[(allocation_df[fbs_activity_fields[0]].isin(flagged_names)) |
                                              (allocation_df[fbs_activity_fields[1]].isin(flagged_names))
                                              ].reset_index(drop=True)
            # concat the flagged and nonflagged dfs
            allocation_df = pd.concat([allocation_df, df_w_sectors_nonflagged],
                                      ignore_index=True).sort_values(['SectorProducedBy', 'SectorConsumedBy'])

    return allocation_df


def proportional_allocation_by_location(df):
    """
    Creates a proportional allocation based on all the most aggregated sectors within a location
    Ensure that sectors are at 2 digit level - can run sector_aggregation() prior to using this function
    :param df:
    :param sectorcolumn:
    :return:
    """

    # tmp drop NoneType
    df = replace_NoneType_with_empty_cells(df)

    # find the shortest length sector

    denom_df = df.loc[(df['SectorProducedBy'].apply(lambda x: len(x) == 2)) |
                      (df['SectorConsumedBy'].apply(lambda x: len(x) == 2))]
    denom_df = denom_df.assign(Denominator=denom_df['FlowAmount'].groupby(
        denom_df['Location']).transform('sum'))
    denom_df_2 = denom_df[['Location', 'LocationSystem', 'Year', 'Denominator']].drop_duplicates()
    # merge the denominator column with fba_w_sector df
    allocation_df = df.merge(denom_df_2, how='left')
    # calculate ratio
    allocation_df.loc[:, 'FlowAmountRatio'] = allocation_df['FlowAmount'] / allocation_df[
        'Denominator']
    allocation_df = allocation_df.drop(columns=['Denominator']).reset_index()

    # add nonetypes
    allocation_df = replace_strings_with_NoneType(allocation_df)

    return allocation_df


def proportional_allocation_by_location_and_activity(df, sectorcolumn, allocation_method):
    """
    Creates a proportional allocation within each aggregated sector within a location
    :param df:
    :param sectorcolumn:
    :return:
    """

    # tmp replace NoneTypes with empty cells
    df = replace_NoneType_with_empty_cells(df)

    # denominator summed from highest level of sector grouped by location
    short_length = min(df[sectorcolumn].apply(lambda x: len(str(x))).unique())
    # want to create denominator based on short_length
    denom_df = df.loc[df[sectorcolumn].apply(lambda x: len(x) == short_length)].reset_index(drop=True)
    grouping_cols = [e for e in ['FlowName', 'Location', 'Activity', 'ActivityConsumedBy', 'ActivityProducedBy']
                     if e in denom_df.columns.values.tolist()]
    denom_df.loc[:, 'Denominator'] = denom_df.groupby(grouping_cols)['HelperFlow'].transform('sum')

    # list of column headers, that if exist in df, should be aggregated using the weighted avg fxn
    possible_column_headers = ('Location', 'LocationSystem', 'Year', 'Activity', 'ActivityConsumedBy', 'ActivityProducedBy')
    # list of column headers that do exist in the df being aggregated
    column_headers = [e for e in possible_column_headers if e in denom_df.columns.values.tolist()]
    merge_headers = column_headers.copy()
    column_headers.append('Denominator')
    # create subset of denominator values based on Locations and Activities
    denom_df_2 = denom_df[column_headers].drop_duplicates().reset_index(drop=True)
    # merge the denominator column with fba_w_sector df
    allocation_df = df.merge(denom_df_2,
                             how='left',
                             left_on=merge_headers,
                             right_on=merge_headers)
    # calculate ratio
    allocation_df.loc[:, 'FlowAmountRatio'] = allocation_df['HelperFlow'] / allocation_df['Denominator']
    allocation_df = allocation_df.drop(columns=['Denominator']).reset_index(drop=True)

    # fill empty cols with NoneType
    allocation_df = replace_strings_with_NoneType(allocation_df)
    # fill na values with 0
    allocation_df['HelperFlow'] = allocation_df['HelperFlow'].fillna(0)

    return allocation_df


# def proportional_allocation_by_location_and_sector(df, sectorcolumn):
#     """
#     Creates a proportional allocation within each aggregated sector within a location
#     :param df:
#     :param sectorcolumn:
#     :return:
#     """
#     from flowsa.common import load_source_catalog
#
#     cat = load_source_catalog()
#     src_info = cat[pd.unique(df['SourceName'])[0]]
#     # load source catalog to determine the level of sector aggregation associated with a crosswalk
#     level_of_aggregation = src_info['sector_aggregation_level']
#
#     # denominator summed from highest level of sector grouped by location
#     short_length = min(df[sectorcolumn].apply(lambda x: len(str(x))).unique())
#     # want to create denominator based on short_length - 1, unless short_length = 2
#     denom_df = df.loc[df[sectorcolumn].apply(lambda x: len(x) == short_length)]
#     if (level_of_aggregation == 'disaggregated') & (short_length != 2):
#         short_length = short_length - 1
#         denom_df.loc[:, 'sec_tmp'] = denom_df[sectorcolumn].apply(lambda x: x[0:short_length])
#         denom_df.loc[:, 'Denominator'] = denom_df.groupby(['Location', 'sec_tmp'])['FlowAmount'].transform('sum')
#     else:  # short_length == 2:]
#         denom_df.loc[:, 'Denominator'] = denom_df['FlowAmount']
#         denom_df.loc[:, 'sec_tmp'] = denom_df[sectorcolumn]
#     # if short_length == 2:
#     #     denom_df.loc[:, 'Denominator'] = denom_df['FlowAmount']
#     #     denom_df.loc[:, 'sec_tmp'] = denom_df[sectorcolumn]
#     # else:
#     #     short_length = short_length - 1
#     #     denom_df.loc[:, 'sec_tmp'] = denom_df[sectorcolumn].apply(lambda x: x[0:short_length])
#     #     denom_df.loc[:, 'Denominator'] = denom_df.groupby(['Location', 'sec_tmp'])['FlowAmount'].transform('sum')
#
#     denom_df_2 = denom_df[['Location', 'LocationSystem', 'Year', 'sec_tmp', 'Denominator']].drop_duplicates()
#     # merge the denominator column with fba_w_sector df
#     df.loc[:, 'sec_tmp'] = df[sectorcolumn].apply(lambda x: x[0:short_length])
#     allocation_df = df.merge(denom_df_2, how='left', left_on=['Location', 'LocationSystem', 'Year', 'sec_tmp'],
#                              right_on=['Location', 'LocationSystem', 'Year', 'sec_tmp'])
#     # calculate ratio
#     allocation_df.loc[:, 'FlowAmountRatio'] = allocation_df['FlowAmount'] / allocation_df[
#         'Denominator']
#     allocation_df = allocation_df.drop(columns=['Denominator', 'sec_tmp']).reset_index(drop=True)
#
#     return allocation_df
