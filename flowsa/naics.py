from typing import Literal
import numpy as np
import pandas as pd
import numpy as np
from flowsa.flowbyfunctions import aggregator
from flowsa.flowsa_log import vlog
from . import (common, dataclean, settings)

naics_crosswalk = pd.read_csv(
    settings.datapath / 'NAICS_2012_Crosswalk.csv', dtype='object'
)


def industry_spec_key(
    industry_spec: dict,
    year: Literal[2002, 2007, 2012, 2017] = 2012
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

    naics = naics_crosswalk.assign(
        target_naics=naics_crosswalk[industry_spec['default']])
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

    # drop source_naics that are more aggregated than target_naics, reorder
    naics_key = (naics_key[['source_naics', 'target_naics']]
                 .dropna()
                 .drop_duplicates()
                 .sort_values(by=['source_naics', 'target_naics'])
                 .reset_index(drop=True)
                 )

    return naics_key


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
        non_sectors = non_sectors[non_sectors[c] != '']
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
        vlog.debug('There are sectors that are not NAICS 2012 Codes')
        vlog.debug(non_sectors)
    else:
        vlog.debug('All sectors are NAICS 2012 Codes')

    return non_sectors


def melt_naics_crosswalk():
    """
    Create a melt version of the naics 07 to 17 crosswalk to map
    naics to naics 2012
    :return: df, naics crosswalk melted
    """
    # load the mastercroswalk and subset by sectorsourcename,
    # save values to list
    cw_load = common.load_crosswalk('sector_timeseries')

    # create melt table of possible 2007 and 2017 naics that can
    # be mapped to 2012
    cw_melt = cw_load.melt(
        id_vars='NAICS_2012_Code', var_name='NAICS_year', value_name='NAICS')
    # drop the naics year because not relevant for replacement purposes
    cw_replacement = cw_melt.dropna(how='any')
    cw_replacement = cw_replacement[
        ['NAICS_2012_Code', 'NAICS']].drop_duplicates()
    # drop rows where contents are equal
    cw_replacement = cw_replacement[
        cw_replacement['NAICS_2012_Code'] != cw_replacement['NAICS']]
    # drop rows where length > 6
    cw_replacement = cw_replacement[cw_replacement['NAICS_2012_Code'].apply(
        lambda x: len(x) < 7)].reset_index(drop=True)
    # order by naics 2012
    cw_replacement = cw_replacement.sort_values(
        ['NAICS', 'NAICS_2012_Code']).reset_index(drop=True)

    # create allocation ratios by determining number of
    # NAICS 2012 to other naics when not a 1:1 ratio
    cw_replacement_2 = cw_replacement.assign(
        naics_count=cw_replacement.groupby(
            ['NAICS'])['NAICS_2012_Code'].transform('count'))
    cw_replacement_2 = cw_replacement_2.assign(
        allocation_ratio=1/cw_replacement_2['naics_count'])

    return cw_replacement_2

def replace_naics_w_naics_from_another_year(df_load, sectorsourcename):
    """
    Replace any non sectors with sectors.
    :param df_load: df with sector columns or sector-like activities
    :param sectorsourcename: str, sector source name (ex. NAICS_2012_Code)
    :return: df, with non-sectors replaced with sectors
    """
    # drop NoneType
    df = dataclean.replace_NoneType_with_empty_cells(df_load).reset_index(drop=True)

    # load the mastercroswalk and subset by sectorsourcename,
    # save values to list
    cw_load = common.load_crosswalk('sector_timeseries')
    cw = cw_load[sectorsourcename].drop_duplicates().tolist()

    # load melted crosswalk
    cw_melt = melt_naics_crosswalk()
    # drop the count column
    cw_melt = cw_melt.drop(columns='naics_count')

    # determine which headers are in the df
    if 'SectorConsumedBy' in df:
        column_headers = ['SectorProducedBy', 'SectorConsumedBy']
    else:
        column_headers = ['ActivityProducedBy', 'ActivityConsumedBy']

    # check if there are any sectors that are not in the naics 2012 crosswalk
    non_naics = check_if_sectors_are_naics(df, cw, column_headers)

    # loop through the df headers and determine if value is
    # not in crosswalk list
    if len(non_naics) != 0:
        vlog.debug('Checking if sectors represent a different '
                   f'NAICS year, if so, replace with {sectorsourcename}')
        for c in column_headers:
            # merge df with the melted sector crosswalk
            df = df.merge(cw_melt, left_on=c, right_on='NAICS', how='left')
            # if there is a value in the sectorsourcename column,
            # use that value to replace sector in column c if value in
            # column c is in the non_naics list
            df[c] = np.where(
                (df[c] == df['NAICS']) & (df[c].isin(non_naics)),
                df[sectorsourcename], df[c])
            # multiply the FlowAmount col by allocation_ratio
            df.loc[df[c] == df[sectorsourcename],
                   'FlowAmount'] = df['FlowAmount'] * df['allocation_ratio']
            # drop columns
            df = df.drop(
                columns=[sectorsourcename, 'NAICS', 'allocation_ratio'])
        vlog.debug(f'Replaced NAICS with {sectorsourcename}')

        # check if there are any sectors that are not in
        # the naics 2012 crosswalk
        vlog.debug('Check again for non NAICS 2012 Codes')
        nonsectors = check_if_sectors_are_naics(df, cw, column_headers)
        if len(nonsectors) != 0:
            vlog.debug('Dropping non-NAICS from dataframe')
            for c in column_headers:
                # drop rows where column value is in the nonnaics list
                df = df[~df[c].isin(nonsectors)]
        # aggregate data
        possible_column_headers = \
            ('FlowAmount', 'Spread', 'Min', 'Max', 'DataReliability',
             'TemporalCorrelation', 'GeographicalCorrelation',
             'TechnologicalCorrelation', 'DataCollection', 'Description')
        # list of column headers to group aggregation by
        groupby_cols = [e for e in df.columns.values.tolist()
                        if e not in possible_column_headers]
        df = aggregator(df, groupby_cols)

    df = dataclean.replace_strings_with_NoneType(df)
    # drop rows where both SectorConsumedBy and SectorProducedBy NoneType
    if 'SectorConsumedBy' in df:
        df_drop = df[(df['SectorConsumedBy'].isnull()) &
                     (df['SectorProducedBy'].isnull())]
        if len(df_drop) != 0:
            activities_dropped = pd.unique(
                df_drop[['ActivityConsumedBy',
                         'ActivityProducedBy']].values.ravel('K'))
            activities_dropped = list(filter(
                lambda x: x is not None, activities_dropped))
            vlog.debug('Dropping rows where the Activity columns '
                       f'contain {", ".join(activities_dropped)}')
        df = df[~((df['SectorConsumedBy'].isnull()) &
                  (df['SectorProducedBy'].isnull()))].reset_index(drop=True)
    else:
        df = df[~((df['ActivityConsumedBy'].isnull()) &
                  (df['ActivityProducedBy'].isnull()))].reset_index(drop=True)

    return df
