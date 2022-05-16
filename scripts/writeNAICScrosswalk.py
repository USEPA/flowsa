# writeNAICScrosswalk.py
# !/usr/bin/env python3
# coding=utf-8

"""

Loops through the data source crosswalks to find any NAICS not in
official Census NAICS Code list. Adds the additional NAICS
to NAICS crosswalk.

- Writes reshaped file to datapath as csv.
"""


import glob

import numpy as np
import pandas as pd

from flowsa import log
from flowsa.common import load_crosswalk
from flowsa.dataclean import replace_NoneType_with_empty_cells, replace_strings_with_NoneType
from flowsa.settings import datapath, crosswalkpath


def load_naics_02_to_07_crosswalk():
    """
    Load the 2002 to 2007 crosswalk from US Census
    :return:
    """
    naics_url = \
        'https://www.census.gov/naics/concordances/2002_to_2007_NAICS.xls'
    df_load = pd.read_excel(naics_url)
    # drop first rows
    df = pd.DataFrame(df_load.loc[2:]).reset_index(drop=True)
    # Assign the column titles
    df.columns = df_load.loc[1, ]

    # df subset columns
    naics_02_to_07_cw = df[['2002 NAICS Code', '2007 NAICS Code']].rename(
        columns={'2002 NAICS Code': 'NAICS_2002_Code',
                 '2007 NAICS Code': 'NAICS_2007_Code'})
    # ensure datatype is string
    naics_02_to_07_cw = naics_02_to_07_cw.astype(str)

    naics_02_to_07_cw = naics_02_to_07_cw.apply(
        lambda x: x.str.strip() if isinstance(x, str) else x)

    return naics_02_to_07_cw


def update_naics_crosswalk():
    """
    update the useeior crosswalk with crosswalks created for
    flowsa datasets - want to add any NAICS > 6 digits
    Add NAICS 2002
    :return: df of NAICS that include any unofficial NAICS
    """

    # read useeior master crosswalk, subset NAICS columns
    naics_load = load_crosswalk('BEA')
    naics = naics_load[['NAICS_2007_Code', 'NAICS_2012_Code', 'NAICS_2017_Code'
                        ]].drop_duplicates().reset_index(drop=True)
    # convert all rows to string
    naics = naics.astype(str)
    # ensure all None are NoneType
    naics = replace_strings_with_NoneType(naics)
    # drop rows where all None
    naics = naics.dropna(how='all')

    # drop naics > 6 in mastercrosswalk (all manufacturing) because unused
    # and slows functions
    naics = naics[naics['NAICS_2012_Code'].apply(
        lambda x: len(x) < 7)].reset_index(drop=True)

    # find any NAICS where length > 6 that are used for allocation purposes
    # and add to naics list
    missing_naics_df_list = []
    # read in all the crosswalk csv files (ends in toNAICS.csv)
    for file_name in glob.glob(
            datapath + "activitytosectormapping/" + 'NAICS_Crosswalk_*.csv'):
        # skip Statistics Canada GDP because not all sectors relevant
        if file_name != crosswalkpath + 'Crosswalk_StatCan_GDP_toNAICS.csv':
            df = pd.read_csv(file_name, low_memory=False, dtype=str)
            # convert all rows to string
            df = df.astype(str)
            # determine sector year
            naics_year = df['SectorSourceName'][0]
            if naics_year == 'nan':
                log.info(f'Missing SectorSourceName for {file_name}')
                continue
            # subset dataframe so only sector
            df = df[['Sector']]
            # trim whitespace and cast as string, rename column
            df['Sector'] = df['Sector'].astype(str).str.strip()
            df = df.rename(columns={'Sector': naics_year})
            # extract sector year column from master crosswalk
            df_naics = naics[[naics_year]]
            # find any NAICS that are in source crosswalk but not in
            # mastercrosswalk
            common = df.merge(df_naics, on=[naics_year, naics_year])
            missing_naics = df[(~df[naics_year].isin(common[naics_year]))]
            # extract sectors where len > 6 and that does not include a '-'
            missing_naics = missing_naics[missing_naics[naics_year].apply(
                lambda x: len(x) > 6)]
            if len(missing_naics) != 0:
                missing_naics = missing_naics[
                    ~missing_naics[naics_year].str.contains('-')]
                # append to df list
                missing_naics_df_list.append(missing_naics)
    # concat df list and drop duplications
    missing_naics_df = \
        pd.concat(missing_naics_df_list, ignore_index=True,
                  sort=False).drop_duplicates().reset_index(drop=True)
    # sort df
    missing_naics_df = missing_naics_df.sort_values(['NAICS_2012_Code'])
    missing_naics_df = missing_naics_df.reset_index(drop=True)

    # add missing naics to master naics crosswalk
    total_naics = naics.append(missing_naics_df, ignore_index=True)

    # sort df
    total_naics = total_naics.sort_values(
        ['NAICS_2012_Code', 'NAICS_2007_Code']).drop_duplicates()
    total_naics = total_naics[~total_naics['NAICS_2012_Code'].isin(
        ['None', 'unknown', 'nan', 'Unknown', np.nan])].reset_index(drop=True)

    # convert all columns to string
    total_naics = total_naics.astype(str)

    # add naics 2002
    naics_02 = load_naics_02_to_07_crosswalk()
    naics_cw = pd.merge(total_naics, naics_02, how='left')

    # ensure NoneType
    naics_cw = replace_strings_with_NoneType(naics_cw)

    # reorder
    naics_cw = naics_cw[['NAICS_2002_Code', 'NAICS_2007_Code',
                         'NAICS_2012_Code', 'NAICS_2017_Code']]

    # save as csv
    naics_cw.to_csv(datapath + "NAICS_Crosswalk_TimeSeries.csv", index=False)


def write_naics_2012_crosswalk():
    """
    Create a NAICS 2 - 6 digit crosswalk
    :return:
    """
    # load the useeior mastercrosswalk subset to the naics timeseries
    cw_load = load_crosswalk('sector_timeseries')

    # load BEA codes that will act as NAICS
    house = load_crosswalk('household')
    govt = load_crosswalk('government')
    bea = pd.concat([house, govt], ignore_index=True).rename(
        columns={'Code': 'NAICS_2012_Code',
                 'NAICS_Level_to_Use_For': 'secLength'})
    bea = bea[['NAICS_2012_Code', 'secLength']]

    # extract naics 2012 code column and drop duplicates and empty cells
    cw = cw_load[['NAICS_2012_Code']].drop_duplicates()
    cw = replace_NoneType_with_empty_cells(cw)
    cw = cw[cw['NAICS_2012_Code'] != '']
    # also drop the existing household and government codes because not all
    # inclusive and does not conform to NAICS length standards
    cw = cw[~cw['NAICS_2012_Code'].str.startswith(
        tuple(['F0', 'S0']))].reset_index(drop=True)

    # add column of sector length
    cw['secLength'] = cw['NAICS_2012_Code'].apply(
        lambda x: f"NAICS_{str(len(x))}")
    # add bea codes subbing for NAICS
    cw2 = pd.concat([cw, bea], ignore_index=True)
    # return max string length
    max_naics_length = cw['NAICS_2012_Code'].apply(lambda x: len(x)).max()

    # create dictionary of dataframes
    d = dict(tuple(cw2.groupby('secLength')))

    for k in d.keys():
        d[k].rename(columns=({'NAICS_2012_Code': k}), inplace=True)

    naics_cw = d['NAICS_2']
    for l in range(3, max_naics_length+1):
        naics_cw = (d[f'NAICS_{l}'].assign(temp=d[f'NAICS_{l}'][
            f'NAICS_{l}'].str.extract(
            pat=f"({'|'.join(naics_cw[f'NAICS_{l-1}'])})")).merge(
            naics_cw, how='right', left_on='temp',
            right_on=f'NAICS_{l-1}',
            suffixes=['', '_y'])).drop(columns=['temp', 'secLength_y'])
    # drop seclength column
    naics_cw = naics_cw.drop(columns='secLength')

    # reorder
    naics_cw = naics_cw.reindex(sorted(naics_cw.columns), axis=1)
    # save as csv
    naics_cw.to_csv(datapath + "NAICS_2012_Crosswalk.csv", index=False)


if __name__ == '__main__':
    update_naics_crosswalk()
    write_naics_2012_crosswalk()
