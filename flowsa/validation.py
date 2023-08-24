# validation.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to check data is loaded and transformed correctly
"""

import pandas as pd
import numpy as np
import flowsa
from flowsa import FlowBySector
from flowsa.flowbyfunctions import aggregator, collapse_fbs_sectors
from flowsa.flowsa_log import log, vlog
from flowsa.common import fba_activity_fields
from flowsa.location import US_FIPS
from flowsa.schema import dq_fields


def calculate_flowamount_diff_between_dfs(dfa_load, dfb_load):
    """
    Calculate the differences in FlowAmounts between two dfs
    :param dfa_load: df, initial df
    :param dfb_load: df, modified df
    :return: df, comparing changes in flowamounts between 2 dfs
    """

    # subset the dataframes, only keeping data for easy
    # comparison of flowamounts
    drop_cols = ['Year', 'MeasureofSpread', 'Spread', 'DistributionType',
                 'Min', 'Max', 'DataReliability', 'DataCollection']
    # drop cols and rename, ignore error if a df does not
    # contain a column to drop
    dfa = dfa_load.drop(drop_cols, axis=1, errors='ignore'
                        ).rename(columns={'FlowAmount': 'FlowAmount_Original'})
    dfb = dfb_load.drop(drop_cols, axis=1, errors='ignore'
                        ).rename(columns={'FlowAmount': 'FlowAmount_Modified'})
    # create df dict for modified dfs created in for loop
    df_list = []
    for d in ['a', 'b']:
        df_name = f'df{d}'
        # assign new column of geoscale by which to aggregate
        vars()[df_name+'2'] = vars()[df_name].assign(
            geoscale=np.where(vars()[df_name]['Location'].
                              apply(lambda x: x.endswith('000')),
                              'state', 'county'))
        vars()[df_name+'2'] = vars()[df_name+'2'].assign(
            geoscale=np.where(vars()[df_name+'2']['Location'] == '00000',
                              'national', vars()[df_name+'2']['geoscale']))
        # ensure all nan/nones filled/match
        df_list.append(vars()[df_name+'2'])
    # merge the two dataframes
    df = df_list[0].merge(df_list[1], how='outer')

    # determine if any new data is negative
    dfn = df[df['FlowAmount_Modified'] < 0].reset_index(drop=True)
    if len(dfn) > 0:
        vlog.info('There are negative FlowAmounts in new dataframe, '
                  'see Validation Log')
        vlog.info('Negative FlowAmounts in new dataframe: '
                  '\n {}'.format(dfn.to_string()))

    # Because code will sometimes change terminology, aggregate
    # data by context and flowable to compare df differences
    # subset df
    flowcols = ['FlowName', 'Compartment']
    if 'Flowable' in df.columns:
        flowcols = ['Flowable', 'Context']
    dfs_cols = flowcols + ['ActivityProducedBy', 'ActivityConsumedBy',
              'FlowAmount_Original', 'FlowAmount_Modified', 'Unit',
              'geoscale']
    dfs = df[dfs_cols]
    agg_cols = flowcols + ['ActivityProducedBy', 'ActivityConsumedBy',
                           'Unit', 'geoscale']
    dfagg = dfs.groupby(
        agg_cols, dropna=False, as_index=False).agg(
        {'FlowAmount_Original': sum, 'FlowAmount_Modified': sum})
    # column calculating difference
    dfagg['FlowAmount_Difference'] = \
        dfagg['FlowAmount_Modified'] - dfagg['FlowAmount_Original']
    dfagg['Percent_Difference'] = (dfagg['FlowAmount_Difference'] /
                                   dfagg['FlowAmount_Original']) * 100
    # drop rows where difference = 0
    dfagg2 = dfagg[dfagg['FlowAmount_Difference'] != 0].reset_index(drop=True)
    if len(dfagg2) == 0:
        vlog.info('No FlowAmount differences')
    else:
        # subset df and aggregate, also print out the total
        # aggregate diff at the geoscale
        dfagg3 = dfagg.drop(columns=[
            'ActivityProducedBy', 'ActivityConsumedBy',
            'FlowAmount_Difference', 'Percent_Difference'])
        dfagg4 = dfagg3.groupby(flowcols + ['Unit', 'geoscale'],
            dropna=False, as_index=False).agg(
            {'FlowAmount_Original': sum, 'FlowAmount_Modified': sum})
        # column calculating difference
        dfagg4['FlowAmount_Difference'] = \
            dfagg4['FlowAmount_Modified'] - dfagg4['FlowAmount_Original']
        dfagg4['Percent_Difference'] = (dfagg4['FlowAmount_Difference'] /
                                        dfagg4['FlowAmount_Original']) * 100
        # drop rows where difference = 0
        dfagg5 = dfagg4[
            dfagg4['FlowAmount_Difference'] != 0].reset_index(drop=True)
        vlog.info('Total FlowAmount differences between dataframes: '
                  '\n {}'.format(dfagg5.to_string(), index=False))

        # save detail output in log file
        vlog.info('Total FlowAmount differences by Activity Columns: '
                  '\n {}'.format(dfagg2.to_string(), index=False))


def compare_summation_at_sector_lengths_between_two_dfs(df1, df2):
    """
    Check summed 'FlowAmount' values at each sector length
    :param df1: df, first df of values with sector columns
    :param df2: df, second df of values with sector columns
    :return: df, comparison of sector summation results by region and
    printout if any child naics sum greater than parent naics
    """
    from flowsa.common import load_sector_length_cw_melt

    # determine if activity or sector col
    col = 'Sector'
    if 'ActivityProducedBy' in df1.columns:
        col = 'Activity'

    cw = load_sector_length_cw_melt()

    agg_cols = list(df2.select_dtypes(include=['object', 'int']).columns) + \
               ['SectorProducedByLength', 'SectorConsumedByLength']
    agg_cols = [e for e in agg_cols if e not in [f'{col}ProducedBy',
                                                 f'{col}ConsumedBy']]

    df_list = []
    for df in [df1, df2]:
        # merge df assigning sector lengths
        for s in ['Produced', 'Consumed']:
            df = df.merge(cw, how='left', left_on=f'{col}{s}By',
                          right_on='Sector').drop(columns=['Sector']).rename(
                columns={'SectorLength': f'Sector{s}ByLength'})
            df[f'Sector{s}ByLength'] = df[f'Sector{s}ByLength'].fillna(0)
        # sum flowamounts by sector length
        dfsum = df.groupby(agg_cols, dropna=False).agg({'FlowAmount': 'sum'}).reset_index()
        df_list.append(dfsum)

    df_list[0] = df_list[0].rename(columns={'FlowAmount': 'df1'})
    df_list[1] = df_list[1].rename(columns={'FlowAmount': 'df2'})
    dfm = df_list[0].merge(df_list[1], how='outer')
    dfm = dfm.fillna(0)
    dfm['flowIncrease_df1_to_df2_perc'] = (dfm['df2'] - dfm['df1'])/dfm[
        'df1'] * 100
    # dfm2 = dfm[dfm['flowIncrease_df1_to_df2'] != 0]
    # drop cases where sector length is 0 because not included in naics cw
    dfm2 = dfm[~((dfm['SectorProducedByLength'] == 0) & (dfm[
        'SectorConsumedByLength'] == 0))]
    # sort df
    dfm2 = dfm2.sort_values(['Location', 'SectorProducedByLength',
                             'SectorConsumedByLength']).reset_index(drop=True)

    dfm3 = dfm2[dfm2['flowIncrease_df1_to_df2_perc'] < 0]

    if len(dfm3) > 0:
        log.info('See validation log for cases where the second dataframe '
                 'has flow amounts greater than the first dataframe at the '
                 'same location/sector lengths.')
        vlog.info('The second dataframe has flow amounts greater than '
                  'the first dataframe at the same sector lengths: '
                  '\n {}'.format(dfm3.to_string()))
    else:
        vlog.info('The second dataframe does not have flow amounts '
                  'greater than the first dataframe at any sector '
                  'length')


def check_for_nonetypes_in_sector_col(df):
    """
    Check for NoneType in columns where datatype = string
    :param df: df with columns where datatype = object
    :return: warning message if there are NoneTypes
    """
    # if datatypes are strings, return warning message
    if df['Sector'].isnull().any():
        vlog.warning("There are NoneType values in the 'Sector' column")
    return df


def check_for_negative_flowamounts(df):
    """
    Check for negative FlowAmounts in a dataframe 'FlowAmount' column
    :param df: df, requires 'FlowAmount' column
    :return: df, unchanged
    """
    # return a warning if there are negative flowamount values
    if (df['FlowAmount'].values < 0).any():
        vlog.warning('There are negative FlowAmounts')

    return df


def compare_FBS_results(fbs1, fbs2, ignore_metasources=False,
                        compare_to_remote=False):
    """
    Compare results for two methods
    :param fbs1: str, name of method 1
    :param fbs2: str, name of method 2
    :param ignore_metasources: bool, True to compare fbs without
    matching metasources
    :param compare_to_remote: bool, True to download fbs1 from remote and
    compare to fbs2 generated here
    :return: df, comparison of the two dfs
    """
    import flowsa

    # load first file
    df1 = flowsa.getFlowBySector(fbs1,
                                 download_FBS_if_missing=compare_to_remote)
    # load second file
    if compare_to_remote:
        # Generate the FBS locally and then immediately load
        df2 = FlowBySector.generateFlowBySector(
            method=fbs2, download_sources_ok=True)
    else:
        df2 = flowsa.getFlowBySector(fbs2)
    df_m = compare_FBS(df1, df2, ignore_metasources=ignore_metasources)

    return df_m


def compare_FBS(df1, df2, ignore_metasources=False):
    "Assess differences between two FBS dataframes."
    df1 = df1.rename(columns={'FlowAmount': 'FlowAmount_fbs1'})
    df2 = df2.rename(columns={'FlowAmount': 'FlowAmount_fbs2'})
    merge_cols = [c for c in df2.select_dtypes(include=[
        'object', 'int']).columns if c not in dq_fields]
    if ignore_metasources:
        # todo: update this list
        for e in ['MetaSources', 'AttributionSources', 'SourceName',
                  'SectorSourceName', 'ProducedBySectorType',
                  'ConsumedBySectorType', 'Unit_other', 'AllocationSources',
                  'FlowName']:
            try:
                merge_cols.remove(e)
            except ValueError:
                pass

    # aggregate dfs before merge - might have duplicate sectors due to
    # dropping metasources/attribution sources
    df1 = (df1.groupby(merge_cols, dropna=False)
           .agg({'FlowAmount_fbs1': 'sum'}).reset_index())
    df2 = (df2.groupby(merge_cols, dropna=False)
           .agg({'FlowAmount_fbs2': 'sum'}).reset_index())
    # convert sector columns to object to avoid valueErrors
    cols = ['SectorProducedBy', 'SectorConsumedBy']
    for c in cols:
        df1[c] = df1[c].astype(str)
        df2[c] = df2[c].astype(str)
    for c in ['SectorSourceName']:
        df1 = df1.drop(columns=c, errors='ignore')
        df2 = df2.drop(columns=c, errors='ignore')
    # check units
    # compare_df_units(df1, df2)
    df_m = pd.DataFrame(
        pd.merge(df1[merge_cols + ['FlowAmount_fbs1']],
                 df2[merge_cols + ['FlowAmount_fbs2']],
                 how='outer'))
    df_m = df_m.assign(FlowAmount_diff=df_m['FlowAmount_fbs2']
                       .fillna(0) - df_m['FlowAmount_fbs1'].fillna(0))
    df_m = df_m.assign(
        Percent_Diff=(df_m['FlowAmount_diff']/df_m['FlowAmount_fbs1']) * 100)
    df_m = df_m[df_m['FlowAmount_diff'].apply(
        lambda x: round(abs(x), 2) != 0)].reset_index(drop=True)
    # if no differences, print, if differences, provide df subset
    if len(df_m) == 0:
        vlog.debug('No differences between dataframes')
    else:
        vlog.debug('Differences exist between dataframes')
        df_m = df_m.sort_values(['Location', 'SectorProducedBy',
                                 'SectorConsumedBy', 'Flowable',
                                 'Context', ]).reset_index(drop=True)
    return df_m


def compare_geographic_totals(
    df_subset, df_load, sourcename, attr, activity_set, activity_names,
    df_type='FBA', subnational_geoscale=None
):
    """
    Check for any data loss between the geoscale used and published
    national data
    :param df_subset: df, after subset by geography
    :param df_load: df, loaded data, including published national data
    :param sourcename: str, source name
    :param attr: dictionary, attributes
    :param activity_set: str, activity set
    :param activity_names: list of names in the activity set by which
        to subset national level data
    :param type: str, 'FBA' or 'FBS'
    :param subnational_geoscale: geoscale being compared against the
        national geoscale. Only necessary if df_subset is a FlowBy object
        rather than a DataFrame.
    :return: df, comparing published national level data to df subset
    """

    # subset df_load to national level
    nat = df_load[df_load['Location'] == US_FIPS].reset_index(
        drop=True).rename(columns={'FlowAmount': 'FlowAmount_nat'})
    # if df len is not 0, continue with comparison
    if len(nat) != 0:
        # if the unit is a rate, do not compare
        if '/' in nat['Unit'][0]:
            log.info(f"Skipping geoscale comparison because {nat['Unit'][0]} "
                     f"is a rate.")
        else:
            # subset national level data by activity set names
            nat = nat[(nat[fba_activity_fields[0]].isin(activity_names)) |
                      (nat[fba_activity_fields[1]].isin(activity_names)
                       )].reset_index(drop=True)
            # drop the geoscale in df_subset and sum
            sub = df_subset.assign(Location=US_FIPS)
            # depending on the datasource, might need to rename some
            # strings for national comparison
            sub = rename_column_values_for_comparison(sub, sourcename)

            # compare df
            merge_cols = ['Class', 'SourceName', 'Unit', 'FlowType',
                          'ActivityProducedBy', 'ActivityConsumedBy',
                          'Location', 'LocationSystem', 'Year']

            if df_type == 'FBA':
                merge_cols.extend(['FlowName', 'Compartment'])
            else:
                 merge_cols.extend(['Flowable', 'Context'])

            sub2 = aggregator(sub, merge_cols).rename(
                columns={'FlowAmount': 'FlowAmount_sub'})

            # compare units
            compare_df_units(nat, sub2)
            df_m = pd.merge(nat[merge_cols + ['FlowAmount_nat']],
                            sub2[merge_cols + ['FlowAmount_sub']],
                            how='outer')
            df_m = df_m.assign(
                FlowAmount_diff=df_m['FlowAmount_nat'] - df_m['FlowAmount_sub'])
            df_m = df_m.assign(Percent_Diff=(abs(df_m['FlowAmount_diff'] /
                                                 df_m['FlowAmount_nat']) * 100))
            df_m = df_m[df_m['FlowAmount_diff'] != 0].reset_index(drop=True)
            # subset the merged df to what to include in the validation df
            # include data where percent difference is > 1 or where value is nan
            df_m_sub = df_m[(df_m['Percent_Diff'] > 1) |
                            (df_m['Percent_Diff'].isna())].reset_index(drop=True)

            subnational_geoscale = (subnational_geoscale
                                    or attr['allocation_from_scale'])
            if len(df_m_sub) == 0:
                vlog.info(f'No data loss greater than 1%% between national '
                          f'level data and {subnational_geoscale} subset')
            else:
                vlog.info(f'There are data differences between published national '
                          f'values and {subnational_geoscale} subset, '
                          f'saving to validation log')

                vlog.debug(
                    'Comparison of National FlowAmounts to aggregated data '
                    'subset for %s: \n {}'.format(
                        df_m_sub.to_string()), activity_set)


def rename_column_values_for_comparison(df, sourcename):
    """
    To compare some datasets at different geographic scales,
    must rename FlowName and Compartments to those available at national level
    :param df: df with FlowName and Compartment columns
    :param sourcename: string, datasource name
    :return:
    """

    # at the national level, only have information for 'FlowName' = 'total'
    # and 'Compartment' = 'total'. At state/county level, have information
    # for fresh/saline and ground/surface water. Therefore, to compare
    # subset data to national level, rename to match national values.
    if sourcename == 'USGS_NWIS_WU':
        df['Flowable'] = np.where(
            df['ActivityConsumedBy'] != 'Livestock', 'Water', df['Flowable'])
        df['Context'] = np.where(df['Context'].str.contains('resource/water/'),
                                 'resource/water', df['Context'])

    return df


def compare_df_units(df1_load, df2_load):
    """
    Determine what units are in each df prior to merge
    :param df1_load:
    :param df2_load:
    :return:
    """
    df1 = df1_load['Unit'].drop_duplicates().tolist()
    df2 = df2_load['Unit'].drop_duplicates().tolist()

    # identify differnces between unit lists
    list_comp = list(set(df1) ^ set(df2))
    # if list is not empty, print warning that units are different
    if list_comp:
        log.info('Merging df with %s and df with %s units', df1, df2)


def calculate_industry_coefficients(fbs_load, year,region,
                                    io_level, impacts=False):
    """
    Generates sector coefficients (flow/$) for all sectors for all locations.

    :param fbs_load: flow by sector method
    :param year: year for industry output dataset
    :param region: str, 'state' or 'national'
    :param io_level: str, 'summary' or 'detail'
    :param impacts: bool or str, True to apply and aggregate on impacts using TRACI,
        False to compare flow/contexts, str to pass alternate method
    """
    from flowsa.sectormapping import map_to_BEA_sectors,\
        get_BEA_industry_output

    fbs = collapse_fbs_sectors(fbs_load)

    fbs = map_to_BEA_sectors(fbs, region, io_level, year)

    inventory = not(impacts)
    if impacts:
        if isinstance(impacts, bool):
            impacts = 'TRACI2.1'
        try:
            import lciafmt
            fbs_summary = (lciafmt.apply_lcia_method(fbs, impacts)
                           .rename(columns={'FlowAmount': 'InvAmount',
                                            'Impact': 'FlowAmount'}))
            groupby_cols = ['Location', 'Sector',
                            'Indicator', 'Indicator unit']
            sort_by_cols = ['Indicator', 'Sector', 'Location']
        except ImportError:
            log.warning('lciafmt not installed')
            inventory = True
        except AttributeError:
            log.warning('check lciafmt branch')
            inventory = True

    if inventory:
        fbs_summary = fbs.copy()
        groupby_cols = ['Location', 'Sector',
                        'Flowable', 'Context', 'Unit']
        sort_by_cols = ['Context', 'Flowable',
                        'Sector', 'Location']

    # Update location if needed prior to aggregation
    if region == 'national':
        fbs_summary["Location"] = US_FIPS

    fbs_summary = (fbs_summary.groupby(groupby_cols)
                   .agg({'FlowAmount': 'sum'}).
                   reset_index())

    bea = get_BEA_industry_output(region, io_level, year)

    # Add sector output and assign coefficients
    fbs_summary = fbs_summary.merge(bea.rename(
        columns={'BEA': 'Sector'}), how = 'left',
        on=['Sector','Location'])
    fbs_summary['Coefficient'] = (fbs_summary['FlowAmount'] /
                                      fbs_summary['Output'])
    fbs_summary = fbs_summary.sort_values(by=sort_by_cols)

    return fbs_summary


if __name__ == "__main__":
    df1 = calculate_industry_coefficients(
            flowsa.getFlowBySector('Water_national_2015_m1'), 2015,
            "national", "summary", False)
    df2 = calculate_industry_coefficients(
            flowsa.getFlowBySector('GRDREL_national_2017'), 2017,
            "national", "summary", True)
    df3 = calculate_industry_coefficients(
            flowsa.getFlowBySector('GRDREL_national_2017'), 2017,
            "national", "detail", True)
    df4 = calculate_industry_coefficients(
            flowsa.getFlowBySector('GRDREL_state_2017'), 2017,
            "national", "detail", True)
    try:
        df5 = calculate_industry_coefficients(
                flowsa.getFlowBySector('GRDREL_state_2017'), 2017,
                "state", "detail", True)
    except TypeError:
        df5 = None
