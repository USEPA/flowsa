from typing import Literal
import pandas as pd
import numpy as np
import re
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


def subset_sector_key(flowbyactivity, activitycol, primary_sector_key, secondary_sector_key=None):
    """
    Subset the sector key to return an industry that most closely maps source sectors to target
    sectors by matching on sector length, based on the sectors that are in the FBA

    @param flowbyactivity: FBA (if activities are sector like) or df of activity to sector mapping (if activites are text
    based) that contains activity data
    @param activitycol:
    @param primary_sector_key:
    @param secondary_sector_key:
    @return:
    """
    # todo: add warning for the sectors that are dropped/not mapped

    # if the primary sector key is the activity to sector crosswalk, which is the case for FBAs with non-sector-like
    # activities, merge with the secondary sector key (the naics industry key) to pull in target sectors and tech
    # corr scoring
    group_cols = ["target_naics", "Class", "Flowable", "Context"]
    merge_col = "source_naics"
    drop_col = activitycol
    if "Activity" in primary_sector_key.columns:
        group_cols = group_cols + ["Activity"]
        merge_col = "Activity"
        drop_col = "source_naics"

        primary_sector_key = (primary_sector_key.merge(
            secondary_sector_key,
            how='left',
            left_on='Sector',
            right_on='source_naics',
        ))
        # print where values are not mapped
        unmapped = primary_sector_key.query('source_naics.isnull()')
        if len(unmapped) > 0:
            log.warning('Activities are unmapped for %s',
                        set(zip(unmapped['Activity'], unmapped['Sector'])))
        # drop null values and sector col
        primary_sector_key = (primary_sector_key
                              .dropna(subset=['source_naics'])
                              .drop(columns=['Sector']))

    # want to best match class/flowable/context/activities combos with target sectors
    flowbyactivity = flowbyactivity[['Class', 'Flowable', 'Context', activitycol]].drop_duplicates()

    primary_sector_key = pd.DataFrame(flowbyactivity.merge(
        primary_sector_key,
        how='left',
        left_on=activitycol,
        right_on=merge_col,
    )).dropna(subset=[merge_col]).drop(columns=activitycol)

    # Keep rows where source = target
    df_keep = primary_sector_key[primary_sector_key["source_naics"] ==
                                 primary_sector_key["target_naics"]].reset_index(drop=True)

    # subset df to all remaining target sectors and Activity if present by dropping the one to one matches
    df_remaining = primary_sector_key.merge(
        df_keep,
        on=list(primary_sector_key.columns),
        how='left',
        indicator=True
    ).query('_merge == "left_only"').drop('_merge', axis=1)

    # function to identify which source naics most closely match to the target naics
    def subset_target_sectors_by_source_sectors(group):
        target = group["target_naics"].iloc[0]
        target_length = len(target)

        # first check for length source > length target
        group_filtered_greater = group[group["source_naics"].apply(len) > target_length]
        if not group_filtered_greater.empty:
            # keep rows where source length is smallest greater length
            min_source_length = min(group_filtered_greater["source_naics"].apply(len))
            result_greater = group_filtered_greater[group_filtered_greater["source_naics"].apply(len)
                                                    == min_source_length]
            # drop the greater data from the remainder df before looking for shorter lengths
            if "Activity" in group.columns:
                group = group[~((group["target_naics"].isin(result_greater["target_naics"])) &
                                (group["Activity"].isin(result_greater["Activity"])))]
            else:
                group = group[~group["target_naics"].isin(result_greater["target_naics"])]
        else:
            result_greater = pd.DataFrame()

        # if there are no source length greater than target, check for source values shorter
        group_filtered_shorter = group[group["source_naics"].apply(len) < target_length]
        if not group_filtered_shorter.empty:
            # keep rows where source length is smallest smaller length
            max_source_length = max(group_filtered_shorter["source_naics"].apply(len))
            result_shorter = group_filtered_shorter[
                group_filtered_shorter["source_naics"].apply(len) == max_source_length]
        else:
            result_shorter = pd.DataFrame()
        return pd.concat([result_greater, result_shorter], ignore_index=True)

    df_remaining_mapped = (df_remaining
                       .groupby(group_cols, dropna=False)
                       .apply(subset_target_sectors_by_source_sectors)
                       .reset_index(drop=True)
                       )

    mapping = pd.concat([df_keep, df_remaining_mapped], ignore_index=True)

    # depending on if activities are naics-like or not determines which column to drop,
    # if text based activities, drop duplicates.
    # Necessary when source activities initially map to a finer resolution NAICS level
    mapping = (mapping
               .drop(columns=drop_col, errors='ignore') # if activities naics-like, already dropped col
               .drop_duplicates()
               .reset_index(drop=True)
               )

    return mapping


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


def generate_naics_crosswalk_conversion_ratios(sectorsourcename, targetsectorsourcename):
    """
    Create a melt version of the source naics source years crosswalk to map
    naics to naics target year
    :param sectorsourcename: str, the source sector year
    :param targetsectorsourcename: str, the target sector year, such as
    "NAICS_2012_Code"
    :return: df, naics crosswalk melted
    """

    # load the mastercroswalk and subset by sectorsourcename,
    # save values to list
    df = common.load_crosswalk('NAICS_Year_Concordance')[[sectorsourcename,
                                                               targetsectorsourcename]].drop_duplicates()

    # test
    # df = df[df[sectorsourcename].str.startswith('1119')]

    all_ratios = []

    # Calculate allocation ratios for each length from 6 to 2
    for length in range(6, 1, -1):
        # Truncate both NAICS and NAICS_2017_Code to the current string length
        df['source'] = df[f'{sectorsourcename}'].str[:length]
        df['target'] = df[f'{targetsectorsourcename}'].str[:length]

        # Group by the truncated NAICS codes
        df_grouped = df.groupby(['source', 'target']).size().reset_index(name='naics_count')

        # Calculate the allocation ratios
        df_grouped['allocation_ratio'] = df_grouped.groupby('source')['naics_count'].transform(lambda x: x / x.sum())

        # Add the length to the results
        df_grouped['length'] = length

        # Collect the results
        all_ratios.append(df_grouped)

    # Combine all ratios into a single DataFrame
    ratios_df = pd.concat(all_ratios, ignore_index=True)
    ratios_df = ratios_df.rename(columns={'source': 'NAICS',
                                          'target': f'{targetsectorsourcename}'
                                          })

    return ratios_df

def convert_naics_year(df_load, targetsectorsourcename, sectorsourcename,
                       dfname):
    """
    Convert sector year
    :param df_load: df with sector columns or sector-like activities
    :param sectorsourcename: str, sector source name (ex. NAICS_2012_Code)
    :param dfname: str, name of data source
    :return: df, with sectors replaced with new sector year
    """
    # todo: update this function to work better with recursive method

    # todo: ensure non-naics (7-digits, etc are converted)

    # load the mastercrosswalk and subset by sectorsourcename,
    # save values to list
    if targetsectorsourcename == sectorsourcename:
        return df_load
    else:
        log.info(f"Converting {sectorsourcename} to "
                 f"{targetsectorsourcename} in {dfname}")

        # load conversion crosswalk
        cw_melt = generate_naics_crosswalk_conversion_ratios(sectorsourcename, targetsectorsourcename)
        # drop the count column
        cw_melt = cw_melt.drop(columns=['naics_count', 'length'])
        cw_list = cw_melt[targetsectorsourcename].drop_duplicates().tolist()

        # determine which headers are in the df
        column_headers = ['ActivityProducedBy', 'ActivityConsumedBy']
        if 'SectorConsumedBy' in df_load:
            column_headers = ['SectorProducedBy', 'SectorConsumedBy']
        if 'Sector' in df_load:
            column_headers = ['Sector']

        # check if there are any sectors that are not in the naics annual crosswalk
        non_naics = check_if_sectors_are_naics(df_load, cw_list, column_headers)

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
            nonsectors = check_if_sectors_are_naics(df, cw_list, column_headers)
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

def return_max_sector_level(
    industry_spec: dict,
) -> pd.DataFrame:
    """
    Return max sector length/level based on industry spec.

    The industry_spec is a (possibly nested) dictionary formatted as in this
    example:

    industry_spec = {'default': 'NAICS_3',
                     'NAICS_4': ['112', '113'],
                     'NAICS_6': ['1129']
                     }
    """
    # list of keys in industry spec
    level_list = list(industry_spec.keys())
    # append default sector level
    level_list.append(industry_spec['default'])

    n = []
    for string in level_list:
        # Convert each found number to an integer and extend the result into the list n
        n.extend(map(int, re.findall(r'\d+', string)))

    max_level = max(n)

    return max_level
