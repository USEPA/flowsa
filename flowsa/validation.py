# validation.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions to check data is loaded and transformed correctly
"""

import pandas as pd
import numpy as np
from esupy.processed_data_mgmt import download_from_remote
import flowsa
import flowsa.flowbysector
from flowsa.flowbysector import FlowBySector
from flowsa.flowbyfunctions import aggregator, collapse_fbs_sectors
from flowsa.flowsa_log import log, vlog
from flowsa.common import fba_activity_fields, load_yaml_dict
from flowsa.location import US_FIPS
from flowsa.metadata import set_fb_meta
from flowsa.schema import dq_fields
from flowsa.settings import paths, diffpath


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
        {'FlowAmount_Original': "sum", 'FlowAmount_Modified': "sum"})
    # column calculating difference
    dfagg['FlowAmount_Difference'] = \
        dfagg['FlowAmount_Modified'] - dfagg['FlowAmount_Original']
    dfagg['Percent_Increase'] = (dfagg['FlowAmount_Difference'] /
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
            'FlowAmount_Difference', 'Percent_Increase'])
        dfagg4 = dfagg3.groupby(flowcols + ['Unit', 'geoscale'],
            dropna=False, as_index=False).agg(
            {'FlowAmount_Original': "sum", 'FlowAmount_Modified': "sum"})
        # column calculating difference
        dfagg4['FlowAmount_Difference'] = \
            dfagg4['FlowAmount_Modified'] - dfagg4['FlowAmount_Original']
        dfagg4['Percent_Increase'] = (dfagg4['FlowAmount_Difference'] /
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

    cw = load_sector_length_cw_melt(year=df1.config['target_naics_year'])

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


def compare_FBA_results(source, year, fba1_version=None, fba2_version=None,
                        compare_to_remote=False):
    """
    Compare two FBA dataframes. Can specify version and git hash. Example:

    source = 'USDA_CoA_Cropland'
    year = 2017
    fba1_version = 'v1.2.1'
    fba2_version = 'v1.3.0'
    compare_to_remote=False

    :param source:
    :param year:
    :param fba1_version:
    :param fba2_version:
    :param compare_to_remote:
    :return:
    """
    import flowsa
    from flowsa.flowbyactivity import FlowByActivity

    # load first file (if compare to remote, this is remote file)
    df1 = flowsa.getFlowByActivity(datasource=source, year=year,
                                   git_version=fba1_version,
                                   download_FBA_if_missing=compare_to_remote)
    # load second file
    if compare_to_remote:
        # Generate the FBS locally and then immediately load
        flowsa.generateflowbyactivity.main(source=source, year=year)
        df2 = flowsa.getFlowByActivity(datasource=source, year=year)
    else:
        df2 = flowsa.getFlowByActivity(
            datasource=source, year=year, git_version=fba2_version)

    df1 = df1.rename(columns={'FlowAmount': 'FlowAmount_fba1'})
    df2 = df2.rename(columns={'FlowAmount': 'FlowAmount_fba2'})
    merge_cols = [c for c in df2.select_dtypes(include=[
        'object', 'int']).columns if c not in dq_fields]

    # convert activity columns to object to avoid valueErrors
    cols = ['ActivityProducedBy', 'ActivityConsumedBy']
    for c in cols:
        df1[c] = df1[c].astype(str)
        df2[c] = df2[c].astype(str)

    df_m = pd.DataFrame(
        pd.merge(df1[merge_cols + ['FlowAmount_fba1']],
                 df2[merge_cols + ['FlowAmount_fba2']],
                 how='outer'))
    df_m = df_m.assign(FlowAmount_diff=df_m['FlowAmount_fba2']
                       .fillna(0) - df_m['FlowAmount_fba1'].fillna(0))
    df_m = df_m.assign(
        Percent_Increase=(df_m['FlowAmount_diff']/df_m['FlowAmount_fba1']) * 100)
    df_m = df_m[df_m['FlowAmount_diff'].apply(
        lambda x: round(abs(x), 2) != 0)].reset_index(drop=True)
    # if no differences, print, if differences, provide df subset
    if len(df_m) == 0:
        log.info(f'No differences between FBA dataframes for {source}')
    else:
        log.info(f'Differences exist between FBA dataframes for {source}')
        df_m = df_m.sort_values(['Location', 'ActivityProducedBy',
                                 'ActivityConsumedBy', 'FlowName',
                                 'Class']).reset_index(drop=True)
    return df_m


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
    df1 = flowsa.flowbysector.getFlowBySector(fbs1,
                                              download_FBS_if_missing=compare_to_remote)
    # load second file
    if compare_to_remote:
        # Generate the FBS locally and then immediately load
        df2 = FlowBySector.generateFlowBySector(
            method=fbs2, download_sources_ok=True)
    else:
        df2 = flowsa.flowbysector.getFlowBySector(fbs2)
    df_m = compare_FBS(df1, df2, ignore_metasources=ignore_metasources)

    return df_m


def compare_FBS(df1_load, df2_load, ignore_metasources=False):
    """Assess differences between two FBS dataframes."""
    df1 = pd.DataFrame(df1_load.rename(columns={'FlowAmount': 'FlowAmount_fbs1'}))
    df2 = pd.DataFrame(df2_load.rename(columns={'FlowAmount': 'FlowAmount_fbs2'}))
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

    # convert sector columns to object to avoid valueErrors and df clean up
    cols = ['SectorProducedBy', 'SectorConsumedBy']
    for c in cols:
        df1[c] = df1[c].astype(str)
        df2[c] = df2[c].astype(str)
    for c in ['SectorSourceName']:
        df1 = df1.drop(columns=c, errors='ignore')
        df2 = df2.drop(columns=c, errors='ignore')
        merge_cols = [x for x in merge_cols if x != c]
    # convert all np.nan in the string type merge cols to empty strings, to ensure correct merge
    fill_cols = [c for c in merge_cols if df2[c].dtype == 'object']
    df1[fill_cols] = df1[fill_cols].replace(['nan', np.nan], '')
    df2[fill_cols] = df2[fill_cols].replace(['nan', np.nan], '')

    # subset dfs
    df1_sub = df1[merge_cols + ['FlowAmount_fbs1']]
    df2_sub = df2[merge_cols + ['FlowAmount_fbs2']]

    # aggregate dfs before merge - might have duplicate sectors due to
    # dropping metasources/attribution sources
    df1_sub = df1_sub.groupby(merge_cols, dropna=False).agg({'FlowAmount_fbs1': 'sum'}).reset_index()
    df2_sub = df2_sub.groupby(merge_cols, dropna=False).agg({'FlowAmount_fbs2': 'sum'}).reset_index()

    # check units
    # compare_df_units(df1_sub, df2_sub)
    df_m = pd.merge(df1_sub, df2_sub,how='outer')
    df_m = df_m.assign(FlowAmount_diff=df_m['FlowAmount_fbs2']
                       .fillna(0) - df_m['FlowAmount_fbs1'].fillna(0))
    df_m = df_m.assign(
        Percent_Increase=(df_m['FlowAmount_diff']/df_m['FlowAmount_fbs1']) * 100)
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


def compare_single_FBS_against_remote(m, outdir=diffpath,
                                      run_single=False):
    """Action function to compare a generated FBS with that in remote"""
    downloaded = download_from_remote(set_fb_meta(m, "FlowBySector"),
                                      paths)
    if not downloaded:
        if run_single:
            # Run a single file even if no comparison available
            FlowBySector.generateFlowBySector(
                method=m, download_sources_ok=True)
        else:
            print(f"{m} not found in remote server. Skipping...")
        return
    print("--------------------------------\n"
          f"Method: {m}\n"
          "--------------------------------")
    df = compare_FBS_results(m, m, ignore_metasources=True,
                             compare_to_remote=True)
    df.rename(columns = {'FlowAmount_fbs1': 'FlowAmount_remote',
                         'FlowAmount_fbs2': 'FlowAmount_HEAD'},
              inplace=True)
    if len(df) > 0:
        print(f"Saving differences in {m} to csv")
        # maintain leading 0s in location col
        df.Location = df.Location.apply('="{}"'.format)
        df.to_csv(f"{outdir}/{m}_diff.csv", index=False)
    else:
        print(f"***No differences found in {m}***")


def compare_single_FBA_against_remote(source, year, outdir=diffpath,
                                      run_single=False):
    """Action function to compare a generated FBA with that in remote"""
    meta_name = f'{source}_{year}'
    downloaded = download_from_remote(set_fb_meta(
        meta_name, "FlowByActivity"), paths)
    if not downloaded:
        if run_single:
            # Run a single file even if no comparison available
            flowsa.flowbyactivity.generateflowbyactivity.main(
                year=year, source=source)
        else:
            print(f"{source} {year} not found in remote server. Skipping...")
        return
    print("--------------------------------\n"
          f"Method: {source} {year}\n"
          "--------------------------------")
    df = compare_FBA_results(source, year, compare_to_remote=True)

    df.rename(columns={'FlowAmount_fba1': 'FlowAmount_remote',
                       'FlowAmount_fba2': 'FlowAmount_HEAD'},
              inplace=True)
    if len(df) > 0:
        print(f"Saving differences in {source} {year} to csv")
        # maintain leading 0s in location col
        df.Location = df.Location.apply('="{}"'.format)
        df.to_csv(f"{outdir}/{source}_{year}_diff.csv", index=False)
    else:
        print(f"***No differences found in {source} {year}***")


def compare_national_state_fbs(dataname=None, year=None, method=None,
                               nationalname=None, statename=None,
                               compare_metasources=False):
    """
    Developed to compare national and state FBS. Either include
    dataname/year/method OR specify nationalname/statename if want to
    compare specific githash versions of the FBS

    :param dataname: str, Only want the beginning of the flowname, so for
    example, in "Water_national_2015", the dataname is "Water"
    :param year: str, method year
    :param method: str, if there is a method, include here
    :param nationalname: str, name of national method, can include version
    and githash
    :param statename: str, name of national method, can include version
    and githash
    :param compare_metasources: bool, include MetaSources in the comparion
    :return:
    """
    # declare string versions of national and state dataframes
    if nationalname is not None:
        n = nationalname
        s = statename
    else:
        if method is None:
            method = ''
        else:
            method = f'_{method}'

        n = f'{dataname}_national_{year}{method}'
        s = f'{dataname}_state_{year}{method}'

    # load the FBS as dataframes
    national = FlowBySector.return_FBS(n)
    state = FlowBySector.return_FBS(s)

    # load state level target sectors - assumption state will always be
    # equal or more aggregated than national
    if state.config == {}:
        state.config = load_yaml_dict(s, 'FBS')
    if national.config == {}:
        national.config = load_yaml_dict(n, 'FBS')
    state_target = state.config['industry_spec']

    groupby_fields = ['Flowable','Context','SectorProducedBy', 'SectorConsumedBy',
                      'Unit', 'Location', 'FlowUUID']
    for g in groupby_fields:
        if national[g].isna().all() & state[g].isna().all():
            groupby_fields.remove(g)
    if compare_metasources:
        groupby_fields = groupby_fields + ['MetaSources']
    subset_fields = groupby_fields + ['FlowAmount']

    # attribute national data to state level target sectors, subset df,
    # and aggregate
    national_agg = (
        national
        .sector_aggregation(industry_spec=state_target)[subset_fields]
        .aggregate_flowby()
    )

    # attribute state data to national location, subset df,
    # and aggregate
    state_agg = (
        state
        .convert_fips_to_geoscale(target_geoscale='national')[subset_fields]
        .aggregate_flowby()
    )

    # compare FBS results
    sectors = pd.DataFrame(
        national_agg
        .merge(state_agg, how='outer', on=groupby_fields)
        .rename(columns={'FlowAmount_x':'national',
                         'FlowAmount_y':'state'})
        .drop(columns='Location'))
    sectors['comp'] = sectors['state'].fillna(0) / sectors['national']

    flows = (
        sectors
        .groupby('Flowable').agg({'state':'sum','national':'sum'})
        .reset_index())
    flows['comp'] = round(flows['state'] / flows['national'], 3)

    return sectors, flows


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
                FlowAmount_diff=df_m['FlowAmount_sub'] - df_m['FlowAmount_nat'])
            df_m = df_m.assign(Percent_Increase=(abs(df_m['FlowAmount_diff'] /
                                                 df_m['FlowAmount_nat']) * 100))
            df_m = df_m[df_m['FlowAmount_diff'] != 0].reset_index(drop=True)
            # subset the merged df to what to include in the validation df
            # include data where percent difference is > 1 or where value is nan
            df_m_sub = df_m[(df_m['Percent_Increase'] > 1) |
                            (df_m['Percent_Increase'].isna())].reset_index(drop=True)

            subnational_geoscale = (subnational_geoscale
                                    or attr['allocation_from_scale'])
            if len(df_m_sub) == 0:
                vlog.info(f'No data loss greater than 1% between national '
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
