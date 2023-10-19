from typing import Literal
import pandas as pd
import numpy as np
from flowsa.flowbyfunctions import aggregator
from flowsa.flowsa_log import vlog, log
from . import (common, settings)


def return_naics_crosswalk(
        year: Literal[2012, 2017]
) -> pd.DataFrame:
    """
    Load a naics crosswalk for 2012 or 2017 codes

    :param industry_spec:
    :param year:
    :return:
    """

    crosswalk = f'NAICS_{year}_Crosswalk'

    naics_crosswalk = common.load_crosswalk(crosswalk)

    return naics_crosswalk


def industry_spec_key(
    industry_spec: dict,
    year: Literal[2002, 2007, 2012, 2017]  # Year of NAICS code
) -> pd.DataFrame:
    """
    Provides a key for mapping any set of NAICS codes to a given industry
    breakdown, specified in industry_spec. The key is a DataFrame with columns
    'source_naics' and 'target_naics'; it is 1-to-many for any NAICS codes
    shorter than the relevant level given in industry-spec, and many-to-1 for
    any NAICS codes longer than the relevant level.

    The industry_spec is a (possibly nested) dictionary formatted as in this
    example:

    industry_spec = {'default': 'NAICS_3',
                     'NAICS_4': ['112', '113'],
                     'NAICS_6': ['1129']

    This example specification would map any set of NAICS codes to the 3-digit
    level, except that codes in 112 and 113 would be mapped to the 4-digit
    level, with codes in 1129 being mapped to the 6 digits level.

    The top industry_spec dictionary may also include a key 'non_naics', where
    the associated value is a non-NAICS "industry" or list of such "industries"
    that should be included in the set of industries that can be mapped to.
    In this case, the user will need to supply their own crosswalk which maps
    activities to that industry.

    Some important points in formatting an industry specification:
    1.  Every dictionary in the spec must have a 'default' key, whose value is
        used for any relevant codes not specifically named in the dictionary.
    2.  Each non-default key in a dictionary must be at the length given
        by the default value for the dictionary (so if 'default': 'NAICS_3',
        then any non-default keys must be NAICS codes with exactly 3 digits).
    3.  Each dictionary is applied only to those codes matching its parent
        key (with the root dictionary being applied to all codes).
    """

    naics = return_naics_crosswalk(year)
    naics = naics.assign(
        target_naics=naics[industry_spec['default']])
    for level, industries in industry_spec.items():
        if level not in ['default', 'non_naics']:
            naics['target_naics'] = naics['target_naics'].mask(
                naics.drop(columns='target_naics').isin(industries).any(axis='columns'),
                naics[level]
            )
    # melt the dataframe to include source naics
    naics_key = naics.melt(id_vars="target_naics", value_name="source_naics")
    # add user-specified non-naics
    if 'non_naics' in industry_spec:
        non_naics = industry_spec['non_naics']
        if isinstance(non_naics, str):
            non_naics = [non_naics]
        naics_key = pd.concat([naics_key,
                               pd.DataFrame({'source_naics': non_naics,
                                             'target_naics': non_naics})])

    # drop nans
    naics_key = (naics_key[['source_naics', 'target_naics']]
                 .dropna()
                 .drop_duplicates()
                 .sort_values(by=['source_naics', 'target_naics'])
                 .reset_index(drop=True)
                 )

    return naics_key


def map_target_sectors_to_less_aggregated_sectors(
    industry_spec: dict,
    year: Literal[2002, 2007, 2012, 2017]
) -> pd.DataFrame:
    """
    Map target NAICS to all possible other sector lengths
    flat hierarchy
    """
    naics = return_naics_crosswalk(year)
    naics = naics.assign(
        target_naics=naics[industry_spec['default']])
    for level, industries in industry_spec.items():
        if level not in ['default', 'non_naics']:
            naics['target_naics'] = naics['target_naics'].mask(
                naics.drop(columns='target_naics').isin(industries).any(axis='columns'),
                naics[level]
            )

    # todo: add user-specified non-naics
    # if 'non_naics' in industry_spec:
    #     non_naics = industry_spec['non_naics']
    #     if isinstance(non_naics, str):
    #         non_naics = [non_naics]
    #     naics_key = pd.concat([naics_key,
    #                            pd.DataFrame({'source_naics': non_naics,
    #                                          'target_naics': non_naics})])

    # drop source_naics that are more aggregated than target_naics, reorder
    for n in (2, 7):
        naics[f'NAICS_{n}'] = np.where(
            naics[f'NAICS_{n}'].str.len() > naics['target_naics'].str.len(),
            np.nan,
            naics[f'NAICS_{n}'])

    # rename columns to align with previous code
    naics = naics.rename(columns={'NAICS_2': '_naics_2',
                                  'NAICS_3': '_naics_3',
                                  'NAICS_4': '_naics_4',
                                  'NAICS_5': '_naics_5',
                                  'NAICS_6': '_naics_6',
                                  'NAICS_7': '_naics_7'}
                         )

    return naics.drop_duplicates().reset_index(drop=True)


def map_source_sectors_to_more_aggregated_sectors(
    year: Literal[2002, 2007, 2012, 2017]
) -> pd.DataFrame:
    """
    Map source NAICS to all possible other sector lengths
    parent-childhierarchy
    """
    naics_crosswalk = return_naics_crosswalk(year)

    naics = []
    for n in naics_crosswalk.columns.values.tolist():
        naics_sub = naics_crosswalk.assign(source_naics=naics_crosswalk[n])
        naics.append(naics_sub)

    # concat data into single dataframe
    naics_key = pd.concat(naics, sort=False)
    naics_key = naics_key.dropna(subset=['source_naics'])

    # drop source_naics that are more aggregated than target_naics, reorder
    for n in range(2, 8):
        naics_key[f'NAICS_{n}'] = np.where(
            naics_key[f'NAICS_{n}'].str.len() > naics_key[
                'source_naics'].str.len(),
            np.nan,
            naics_key[f'NAICS_{n}'])

    # rename columns to align with previous code
    naics_key = naics_key.rename(columns={'NAICS_2': 'n2',
                                          'NAICS_3': 'n3',
                                          'NAICS_4': 'n4',
                                          'NAICS_5': 'n5',
                                          'NAICS_6': 'n6',
                                          'NAICS_7': 'n7'}
                                 )

    return naics_key.drop_duplicates()


def map_source_sectors_to_less_aggregated_sectors(
    year: Literal[2002, 2007, 2012, 2017]
) -> pd.DataFrame:
    """
    Map source NAICS to all possible other sector lengths
    parent-childhierarchy
    """
    naics_crosswalk = return_naics_crosswalk(year)

    naics = []
    for n in naics_crosswalk.columns.values.tolist():
        naics_sub = naics_crosswalk.assign(source_naics=naics_crosswalk[n])
        naics.append(naics_sub)

    # concat data into single dataframe
    naics_key = pd.concat(naics, sort=False)
    naics_key = naics_key.dropna(subset=['source_naics'])

    # drop source_naics that are more aggregated than target_naics, reorder
    for n in range(2, 8):
        naics_key[f'NAICS_{n}'] = np.where(
            naics_key[f'NAICS_{n}'].str.len() < naics_key[
                'source_naics'].str.len(),
            np.nan,
            naics_key[f'NAICS_{n}'])

    cw_melt = naics_key.melt(id_vars="source_naics",
                             var_name="SectorLength",
                             value_name='Sector'
                             ).drop_duplicates().reset_index(drop=True)

    cw_melt = (cw_melt
               .query("source_naics != Sector")
               .query("~Sector.isna()")
               ).drop_duplicates().reset_index(drop=True)

    return cw_melt


def year_crosswalk(
    source_year: Literal[2002, 2007, 2012, 2017],
    target_year: Literal[2002, 2007, 2012, 2017]
) -> pd.DataFrame:
    '''
    Provides a key for switching between years of the NAICS specification.

    :param source_year: int, one of 2002, 2007, 2012, or 2017.
    :param target_year: int, one of 2002, 2007, 2012, or 2017.
    :return: pd.DataFrame with columns 'source_naics' and 'target_naics',
        corresponding to NAICS codes for the source and target specifications.
    '''
    return (
        pd.read_csv(settings.datapath / 'NAICS_Crosswalk_TimeSeries.csv',
                    dtype='object')
        .assign(source_naics=lambda x: x[f'NAICS_{source_year}_Code'],
                target_naics=lambda x: x[f'NAICS_{target_year}_Code'])
        [['source_naics', 'target_naics']]
        .drop_duplicates()
        .reset_index(drop=True)
    )


def check_if_sectors_are_naics(df_load, crosswalk_list, column_headers):
    """
    Check if activity-like sectors are in fact sectors.
    Also works for the Sector column
    :param df_load: df with activity or sector columns
    :param crosswalk_list: list, sectors found in crosswalk
    :param column_headers: list, headers to check for sectors
    :return: list, values that are not sectors
    """

    # create a df of non-sectors to export
    non_sectors_df = []
    # create a df of just the non-sectors column
    non_sectors_list = []
    # loop through the df headers and determine if value
    # is not in crosswalk list
    for c in column_headers:
        # create df where sectors do not exist in master crosswalk
        non_sectors = df_load[~df_load[c].isin(crosswalk_list)]
        # drop rows where c is empty
        non_sectors = non_sectors[~non_sectors[c].isna()]
        # subset to just the sector column
        if len(non_sectors) != 0:
            sectors = non_sectors[[c]].rename(columns={c: 'NonSectors'})
            non_sectors_df.append(non_sectors)
            non_sectors_list.append(sectors)

    if len(non_sectors_df) != 0:
        # concat the df and the df of sectors
        ns_list = pd.concat(non_sectors_list, sort=False, ignore_index=True)
        # print the NonSectors
        non_sectors = ns_list['NonSectors'].drop_duplicates().tolist()
        vlog.debug('There are sectors that are not target NAICS Codes')
        vlog.debug(non_sectors)
    else:
        log.info('Sectors are all in the target NAICS year and do not require '
                 'conversion')

    return non_sectors


def melt_naics_crosswalk(targetsectorsourcename):
    """
    Create a melt version of the naics 07 to 17 crosswalk to map
    naics to naics 2012
    :param targetsectorsourcename: str, the target sector year, such as
    "NAICS_2012_Code"
    :return: df, naics crosswalk melted
    """

    # load the mastercroswalk and subset by sectorsourcename,
    # save values to list
    cw_load = common.load_crosswalk('NAICS_Crosswalk_TimeSeries')

    # create melt table of possible naics from other years that can
    # be mapped to target naics year
    cw_melt = cw_load.melt(
        id_vars=targetsectorsourcename, var_name='NAICS_year', value_name='NAICS')
    # drop the naics year because not relevant for replacement purposes
    cw_replacement = cw_melt.dropna(how='any')
    cw_replacement = cw_replacement[
        [targetsectorsourcename, 'NAICS']].drop_duplicates()
    # drop rows where contents are equal
    cw_replacement = cw_replacement[
        cw_replacement[targetsectorsourcename] != cw_replacement['NAICS']]
    # drop rows where length > 6
    cw_replacement = cw_replacement[cw_replacement[targetsectorsourcename].apply(
        lambda x: len(x) < 7)].reset_index(drop=True)
    # order by naics target tear
    cw_replacement = cw_replacement.sort_values(
        ['NAICS', targetsectorsourcename]).reset_index(drop=True)

    # create allocation ratios by determining number of
    # NAICS year to other naics when not a 1:1 ratio
    cw_replacement_2 = cw_replacement.assign(
        naics_count=cw_replacement.groupby(
            ['NAICS'])[targetsectorsourcename].transform('count'))
    cw_replacement_2 = cw_replacement_2.assign(
        allocation_ratio=1/cw_replacement_2['naics_count'])

    return cw_replacement_2


def convert_naics_year(df_load, targetsectorsourcename, sectorsourcename,
                       dfname):
    """
    Replace any non sectors with sectors.
    :param df_load: df with sector columns or sector-like activities
    :param sectorsourcename: str, sector source name (ex. NAICS_2012_Code)
    :param dfname: str, name of data source
    :return: df, with non-sectors replaced with sectors
    """
    # todo: update this function to work better with recursive method

    # load the mastercrosswalk and subset by sectorsourcename,
    # save values to list
    if targetsectorsourcename == sectorsourcename:
        cw_load = common.load_crosswalk('NAICS_Crosswalk_TimeSeries')[[
        targetsectorsourcename]]
    else:
        log.info(f"Converting {sectorsourcename} to "
                 f"{targetsectorsourcename} in {dfname}")
        cw_load = common.load_crosswalk('NAICS_Crosswalk_TimeSeries')[[
            targetsectorsourcename, sectorsourcename]]
    cw = cw_load[targetsectorsourcename].drop_duplicates().tolist()

    # load melted crosswalk
    cw_melt = melt_naics_crosswalk(targetsectorsourcename)
    # drop the count column
    cw_melt = cw_melt.drop(columns='naics_count')

    # determine which headers are in the df
    column_headers = ['ActivityProducedBy', 'ActivityConsumedBy']
    if 'SectorConsumedBy' in df_load:
        column_headers = ['SectorProducedBy', 'SectorConsumedBy']

    # check if there are any sectors that are not in the naics 2012 crosswalk
    non_naics = check_if_sectors_are_naics(df_load, cw, column_headers)

    # loop through the df headers and determine if value is
    # not in crosswalk list
    df = df_load.copy()
    if len(non_naics) != 0:
        log.info('Checking if sectors represent a different '
                 f'NAICS year, if so, replace with {targetsectorsourcename}')
        for c in column_headers:
            if df[c].isna().all():
                continue
            # merge df with the melted sector crosswalk
            df = df.merge(cw_melt, left_on=c, right_on='NAICS', how='left')
            # if there is a value in the sectorsourcename column,
            # use that value to replace sector in column c if value in
            # column c is in the non_naics list
            df[c] = np.where(
                (df[c] == df['NAICS']) & (df[c].isin(non_naics)),
                df[targetsectorsourcename], df[c])
            # multiply the FlowAmount col by allocation_ratio
            df.loc[df[c] == df[targetsectorsourcename],
                   'FlowAmount'] = df['FlowAmount'] * df['allocation_ratio']
            # drop columns
            df = df.drop(
                columns=[targetsectorsourcename, 'NAICS', 'allocation_ratio'])
        log.info(f'Replaced NAICS with {targetsectorsourcename}')
        # replace the sector year in the sectorsourcename column
        df['SectorSourceName'] = targetsectorsourcename

        # check if there are any sectors that are not in
        # the target sector crosswalk and if so, drop those sectors
        log.info('Checking for unconverted NAICS - determine if rows should '
                 'be dropped.')
        nonsectors = check_if_sectors_are_naics(df, cw, column_headers)
        if len(nonsectors) != 0:
            vlog.debug('Dropping non-NAICS from dataframe')
            for c in column_headers:
                if df[c].isna().all():
                    continue
                # drop rows where column value is in the nonnaics list
                df = df[~df[c].isin(nonsectors)]
        # aggregate data
        if hasattr(df, 'aggregate_flowby'):
            df = (df.aggregate_flowby()
                    .reset_index(drop=True).reset_index()
                    .rename(columns={'index': 'group_id'}))
        else:
            # todo: drop else statement once all dataframes are converted
            #  to classes
            possible_column_headers = \
                ('FlowAmount', 'Spread', 'Min', 'Max', 'DataReliability',
                 'TemporalCorrelation', 'GeographicalCorrelation',
                 'TechnologicalCorrelation', 'DataCollection', 'Description')
            # list of column headers to group aggregation by
            groupby_cols = [e for e in df.columns.values.tolist()
                            if e not in possible_column_headers]
            df = aggregator(df, groupby_cols)

    return df
