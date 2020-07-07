# flowbysector.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Produces a FlowBySector data frame based on a method file for the given class
"""
import flowsa
import pandas as pd
import yaml
from flowsa.common import log, flowbyactivitymethodpath, flow_by_sector_fields, \
    generalize_activity_field_names, fbaoutputpath, fbsoutputpath, datapath, fips_number_key
from flowsa.mapping import add_sectors_to_flowbyactivity, get_fba_allocation_subset
from flowsa.flowbyfunctions import fba_activity_fields, fbs_default_grouping_fields, agg_by_geoscale, \
    fba_fill_na_dict, fbs_fill_na_dict, convert_unit, fba_default_grouping_fields, \
    add_missing_flow_by_fields, fbs_activity_fields, allocate_by_sector, allocation_helper, sector_aggregation, \
    filter_by_geoscale
from flowsa.USGS_NWIS_WU import standardize_usgs_nwis_names
from flowsa.datachecks import sector_flow_comparision


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
        fbs_df.to_parquet(f, engine="pyarrow")
    except:
        log.error('Failed to save ' + parquet_name + ' file.')


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
    for k, v in fbas.items():
        # pull water data for allocation
        flows = flowsa.getFlowByActivity(flowclass=[v['class']],
                                         years=[v['year']],
                                         datasource=k)
        # if allocating USGS NWIS water data, first standardize names in data set
        if k == 'USGS_NWIS_WU':
            flows = standardize_usgs_nwis_names(flows)

        # drop description field
        flows = flows.drop(columns='Description')
        # fill null values
        flows = flows.fillna(value=fba_fill_na_dict)
        # convert unit
        flows = convert_unit(flows)

        # dfs for combined activities
        df_list= []

        # create dictionary of allocation datasets for different usgs activities
        activities = v['activity_sets']
        for aset, attr in activities.items():
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
            # todo: add warning
            # if usgs is less aggregated than allocation df, aggregate usgs activity to target scale
            if fips_number_key[from_scale] > fips_number_key[to_scale]:
                flow_subset = agg_by_geoscale(flow_subset, from_scale, to_scale, fba_default_grouping_fields, names)
            # else, if usgs is more aggregated than allocation table, filter relevant rows
            else:
                flow_subset = filter_by_geoscale(flow_subset, from_scale, names)

            # location column pad zeros if necessary
            flow_subset['Location'] = flow_subset['Location'].apply(lambda x: x.ljust(3 + len(x), '0') if len(x) < 5
                                                                    else x
                                                                    )

            # Add sectors to usgs activity, creating two versions of the flow subset
            # the first version "flow_subset" is the most disaggregated version of the Sectors (NAICS)
            # the second version, "flow_subset_agg" includes only the most aggregated level of sectors
            flow_subset_wsec = add_sectors_to_flowbyactivity(flow_subset,
                                                             sectorsourcename=method['target_sector_source'])
            flow_subset_wsec_agg = add_sectors_to_flowbyactivity(flow_subset,
                                                                 sectorsourcename=method['target_sector_source'],
                                                                 levelofSectoragg='agg')

            # if allocation method is "direct", then no need to create alloc ratios, else need to use allocation
            # dataframe to create sector allocation ratios
            if attr['allocation_method'] == 'direct':
                fbs = flow_subset_wsec_agg.copy()
            else:
                # determine appropriate allocation dataset
                fba_allocation = flowsa.getFlowByActivity(flowclass=[attr['allocation_source_class']],
                                                          datasource=attr['allocation_source'],
                                                          years=[attr['allocation_source_year']]).reset_index(drop=True)

                # fill null values
                fba_allocation = fba_allocation.fillna(value=fba_fill_na_dict)
                # convert unit
                fba_allocation = convert_unit(fba_allocation)

                # subset based on yaml settings
                if attr['allocation_flow'] != 'None':
                    fba_allocation = fba_allocation.loc[fba_allocation['FlowName'].isin(attr['allocation_flow'])]
                if attr['allocation_compartment'] != 'None':
                    fba_allocation = fba_allocation.loc[
                        fba_allocation['Compartment'].isin(attr['allocation_compartment'])]
                # reste index
                fba_allocation = fba_allocation.reset_index(drop=True)

                # aggregate geographically to the scale of the flowbyactivty source, if necessary
                from_scale = attr['allocation_from_scale']
                to_scale = v['geoscale_to_use']
                # if allocation df is less aggregated than FBA df, aggregate allocation df to target scale
                if fips_number_key[from_scale] > fips_number_key[to_scale]:
                    fba_allocation = agg_by_geoscale(fba_allocation, from_scale, to_scale, fba_default_grouping_fields, names)
                # else, if usgs is more aggregated than allocation table, use usgs as both to and from scale
                else:
                    fba_allocation = filter_by_geoscale(fba_allocation, from_scale, names)

                # assign naics to allocation dataset
                fba_allocation = add_sectors_to_flowbyactivity(fba_allocation,
                                                               sectorsourcename=method['target_sector_source'],
                                                               levelofSectoragg=attr[
                                                                   'allocation_sector_aggregation'])
                # subset fba datsets to only keep the naics associated with usgs activity subset
                fba_allocation_subset = get_fba_allocation_subset(fba_allocation, k, names)
                # Reset index values after subset
                fba_allocation_subset = fba_allocation_subset.reset_index(drop=True)
                # generalize activity field names to enable link to water withdrawal table
                fba_allocation_subset = generalize_activity_field_names(fba_allocation_subset)
                # drop columns
                fba_allocation_subset = fba_allocation_subset.drop(columns=['Activity'])
                # fba_allocation_subset = fba_allocation_subset.drop(
                #     columns=['Activity', 'Description', 'Min', 'Max'])
                # if there is an allocation helper dataset, modify allocation df
                if attr['allocation_helper'] == 'yes':
                    fba_allocation_subset = allocation_helper(fba_allocation_subset, method, attr)

                # create flow allocation ratios
                flow_allocation = allocate_by_sector(fba_allocation_subset, attr['allocation_method'])

                # create list of sectors in the flow allocation df, drop any rows of data in the flow df that \
                # aren't in list
                sector_list = flow_allocation['Sector'].unique().tolist()
                # subset fba allocation table to the values in the activity list, based on overlapping sectors
                flow_subset_wsec = flow_subset_wsec.loc[
                    (flow_subset_wsec[fbs_activity_fields[0]].isin(sector_list)) |
                    (flow_subset_wsec[fbs_activity_fields[1]].isin(sector_list))]

                # merge water withdrawal df w/flow allocation dataset
                # todo: modify to recalculate data quality scores

                fbs = flow_subset_wsec.merge(
                    flow_allocation[['Location', 'LocationSystem', 'Sector', 'FlowAmountRatio']],
                    left_on=['Location', 'LocationSystem', 'SectorProducedBy'],
                    right_on=['Location', 'LocationSystem', 'Sector'], how='left')

                fbs = fbs.merge(
                    flow_allocation[['Location', 'LocationSystem', 'Sector', 'FlowAmountRatio']],
                    left_on=['Location', 'LocationSystem', 'SectorConsumedBy'],
                    right_on=['Location', 'LocationSystem', 'Sector'], how='left')

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

            # rename flow name to flowable
            fbs = fbs.rename(columns={"FlowName": 'Flowable',
                                      "Compartment": "Context"
                                      })

            # drop rows where flowamount = 0 (although this includes dropping suppressed data)
            fbs = fbs[fbs['FlowAmount'] != 0].reset_index(drop=True)
            # add missing data columns
            fbs = add_missing_flow_by_fields(fbs, flow_by_sector_fields)
            # fill null values
            fbs = fbs.fillna(value=fbs_fill_na_dict)

            # aggregate df geographically, if necessary
            if fips_number_key[v['geoscale_to_use']] < fips_number_key[attr['allocation_from_scale']]:
                from_scale = v['geoscale_to_use']
            else:
                from_scale = attr['allocation_from_scale']

            to_scale = method['target_geoscale']

            fbs = agg_by_geoscale(fbs, from_scale, to_scale, fbs_default_grouping_fields, names)

            # aggregate data to every sector level
            fbs_agg = sector_aggregation(fbs, fbs_default_grouping_fields)

            # test agg by sector
            sector_agg_comparison = sector_flow_comparision(fbs_agg)

            # return sector level specified in method yaml
            cw = pd.read_csv(datapath + "NAICS_2012_Crosswalk.csv", dtype="str")
            sector_list = cw[method['target_sector_level']].unique().tolist()
            fbs_subset = fbs_agg.loc[(fbs_agg[fbs_activity_fields[0]].isin(sector_list)) |
                                     (fbs_agg[fbs_activity_fields[1]].isin(sector_list))].reset_index(drop=True)

            # save as parquet file
            parquet_name = method_name + '_' + attr['names']
            store_flowbysector(fbs_subset, parquet_name)


    #todo: combine activities into single parquet

    #     df_list.append(fbs_subset)
    # combined_df = pd.concat(df_list)
    #
    # # save as parquet file
    # parquet_name = method_name
    # store_flowbysector(combined_df, parquet_name)

            return fbs_subset  # combined_df
