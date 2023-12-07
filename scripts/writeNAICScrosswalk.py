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
import re
import numpy as np
import pandas as pd

from flowsa.flowsa_log import log
from flowsa.common import load_crosswalk
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
    flowsa datasets
    Add NAICS 2002
    :return: df of NAICS that include any unofficial NAICS
    """

    # read useeior master crosswalk, subset NAICS columns
    naics_load = load_crosswalk('NAICS_to_BEA_Crosswalk_2012')
    naics = naics_load[['NAICS_2007_Code', 'NAICS_2012_Code', 'NAICS_2017_Code'
                        ]].drop_duplicates().reset_index(drop=True)
    # convert all rows to string
    naics = naics.astype(str)
    # drop rows where all None
    naics = (naics
             .replace('nan', np.nan)
             .replace('None', np.nan)
             .dropna(axis=0, how='all')
             )

    # drop naics > 6 in mastercrosswalk (all manufacturing) because unused
    # and slows functions
    naics = naics[naics['NAICS_2012_Code'].apply(
        lambda x: len(x) < 7)].reset_index(drop=True)

    # find any NAICS where length > 6 that are used for allocation purposes
    # and add to naics list
    missing_naics_df_list = []
    # read in all the crosswalk csv files (ends in toNAICS.csv)
    for file_name in glob.glob(f'{crosswalkpath}/NAICS_Crosswalk_*.csv'):
        # skip Statistics Canada GDP because not all sectors relevant
        if not any(s in file_name for s in ('StatCan', 'BEA')):
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
            # drop sectors that include a '-'
            if len(missing_naics) != 0:
                missing_naics = missing_naics[
                    ~missing_naics[naics_year].str.contains('-')]
                # append to df list
                missing_naics_df_list.append(missing_naics)
    # concat df list and drop duplications
    missing_naics_df = \
        pd.concat(missing_naics_df_list, ignore_index=True,
                  sort=False).drop_duplicates().reset_index(drop=True)
    # drop known non-2012 sectors  # todo: evaluate why these are identified as naics 2012
    missing_naics_df = missing_naics_df[~missing_naics_df[
        'NAICS_2012_Code'].isin(['325190', '516', '99'])]
    # sort df
    missing_naics_df = missing_naics_df.sort_values(['NAICS_2012_Code'])
    missing_naics_df = missing_naics_df.reset_index(drop=True)
    # duplicate 2012 into 2017 schema
    missing_naics_df['NAICS_2017_Code'] = missing_naics_df['NAICS_2012_Code']

    # add missing naics to master naics crosswalk
    total_naics = pd.concat([naics, missing_naics_df], ignore_index=True)

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

    # reorder
    naics_cw = naics_cw[['NAICS_2002_Code', 'NAICS_2007_Code',
                         'NAICS_2012_Code', 'NAICS_2017_Code']]
    naics_cw = (naics_cw
                .replace(np.nan, '')
                .replace('nan', '')
                )

    # save as csv
    naics_cw.to_csv(f"{datapath}/NAICS_Crosswalk_TimeSeries.csv", index=False)


def merge_df_by_crosswalk_lengths(naics_cw, d, l):
    """

    :param naics_cw:
    :param d:
    :param l:
    :return:
    """
    naics_cw = (d[f'NAICS_{l}'].assign(temp=d[f'NAICS_{l}'][
        f'NAICS_{l}'].str.extract(
        pat=f"({'|'.join(map(str, naics_cw[f'NAICS_{l - 1}']))})")).merge(
        naics_cw, how='right', left_on='temp',
        right_on=f'NAICS_{l - 1}',
        suffixes=['', '_y'])).drop(columns=['temp', 'secLength_y'])

    return naics_cw


def write_annual_naics_crosswalk():
    """
    Create a NAICS 2 - 6 digit crosswalk
    :return:
    """
    for year in ['2012', '2017']:
        # load the useeior mastercrosswalk subset to the naics timeseries
        cw_load = load_crosswalk('NAICS_Crosswalk_TimeSeries')

        # load BEA codes that will act as NAICS
        house = load_crosswalk('Household_SectorCodes')
        govt = load_crosswalk('Government_SectorCodes')
        bea = pd.concat([house, govt], ignore_index=True).rename(
            columns={'Code': f'NAICS_{year}_Code',
                     'NAICS_Level_to_Use_For': 'secLength'})
        bea = bea[[f'NAICS_{year}_Code', 'secLength']]

        # extract naics year code column and drop duplicates and empty cells
        cw = cw_load[[f'NAICS_{year}_Code']].drop_duplicates()
        cw = cw[cw[f'NAICS_{year}_Code'] != '']
        # also drop the existing household and government codes because not all
        # inclusive and does not conform to NAICS length standards
        cw = cw[~cw[f'NAICS_{year}_Code'].str.startswith(
            tuple(['F0', 'S0', '562B']))].reset_index(drop=True)

        # add column of sector length
        cw['secLength'] = cw[f'NAICS_{year}_Code'].apply(
            lambda x: f"NAICS_{str(len(x))}")
        # add bea codes subbing for NAICS
        cw2 = pd.concat([cw, bea], ignore_index=True).drop_duplicates()
        # return max string length
        max_naics_length = cw[f'NAICS_{year}_Code'].apply(lambda x: len(
            x)).max()

        # create dictionary of dataframes
        d = dict(tuple(cw2.groupby('secLength')))

        for k in d.keys():
            d[k].rename(columns=({f'NAICS_{year}_Code': k}), inplace=True)

        naics_cw = d['NAICS_2']
        for l in range(3, max_naics_length+1):
            # first check that there are corresponding length - 1 sectors in the
            # crosswalk, and if not, append the length-1 sectors to the previous
            # run and rerun, drop government and household sectors
            existing_sec_list = d[f'NAICS_{l-1}'][
                f'NAICS_{l-1}'].drop_duplicates().tolist()
            df_sub = d[f'NAICS_{l}'].copy()
            df_sub[f'NAICS_{l}'] = df_sub[f'NAICS_{l}'].str[:-1]
            df_sub.rename(columns={f'NAICS_{l}': f'NAICS_{l - 1}'}, inplace=True)
            df_sub['secLength'] = df_sub['secLength'].str.replace(
                f"{l}", f"{l - 1}")
            # drop household and gov codes
            df_sub = df_sub[~df_sub[f'NAICS_{l - 1}'].str.startswith(
                tuple(['F0', 'S0', '562B']))].drop_duplicates()
            missing_sectors = df_sub[~df_sub[f'NAICS_{l - 1}'].isin(
                existing_sec_list)]

            # if there are missing sectors at length l-1, append the missing
            # sectors and rerun the previous crosswalk merge
            if (len(missing_sectors) > 0) & (l > 3):
                d[f'NAICS_{l - 1}'] = pd.concat(
                    [d[f'NAICS_{l - 1}'], missing_sectors], ignore_index=True)
                # redo merge at length l-1
                naics_cw = merge_df_by_crosswalk_lengths(naics_cw, d, l - 1).drop(
                    columns=[f"NAICS_{l - 1}_y"]).drop_duplicates()

            naics_cw = merge_df_by_crosswalk_lengths(naics_cw, d, l)

        # drop seclength column
        naics_cw = naics_cw.drop(columns='secLength')
        # reorder
        naics_cw = naics_cw.reindex(sorted(naics_cw.columns), axis=1)
        # save as csv
        naics_cw.to_csv(f"{datapath}/NAICS_{year}_Crosswalk.csv", index=False)


def write_sector_name_crosswalk():
    """
    Generate csv for NAICS 2012 and NAICS 2017 codes for the names of
    sectors, include additional non-official sector codes/names
    :return:
    """
    # import census defined NAICS
    cols_2012 = ['index', 'NAICS_2012_Code', 'NAICS_2012_Name']
    naics_2012 = pd.read_excel(
        "https://www.census.gov/naics/2012NAICS/2-digit_2012_Codes.xls",
        names=cols_2012, skiprows=[0], dtype=str)[['NAICS_2012_Code',
                                                   'NAICS_2012_Name']]

    cols_2017 = ['NAICS_2017_Code', 'NAICS_2017_Name',
                 'NAICS_2017_Description']
    naics_2017 = pd.read_excel(
        "https://www.census.gov/naics/2017NAICS/2017_NAICS_Descriptions.xlsx",
        names=cols_2017, dtype=str)[['NAICS_2017_Code', 'NAICS_2017_Name']]

    # for loop through years to add unoffical NAICS and split hyphenated
    # sectors
    for y in ['2012', '2017']:
        # dictionary of new sector names
        new_sectors = pd.DataFrame(
            {f"NAICS_{y}_Code": ['311119',
                                 '5622121',
                                 '5622122',
                                 '5622191',
                                 '5622192',
                                 '5629201',
                                 '5629202',
                                 '5629203'
                                 ],
             f"NAICS_{y}_Name": ['Other Animal Food Manufacturing',
                                 'MSW Landfill',
                                 'Industrial Waste Landfills',
                                 'Anaerobic Digestion',
                                 'MSW Composting',
                                 'MSW Recycling',
                                 'Mixed CDD MRFs',
                                 'Single material MRFs'
                                 ]})

        # add new sector names to offical sector names
        df = pd.concat([vars()[f'naics_{y}'], new_sectors], ignore_index=True)
        # # strip whitespaces
        df[f"NAICS_{y}_Name"] = df[f"NAICS_{y}_Name"].str.rstrip()
        # strip superscripts
        df[f"NAICS_{y}_Name"] = (
            df[f"NAICS_{y}_Name"]
            .apply(lambda x: re.sub(r"(?<=[a-z])[A-Z]+$", "", x))
            .apply(lambda x: re.sub(r"(?<=[)])[A-Z]+$", "", x))
        )
        # split and add names for hyphenated sectors
        df[f"NAICS_{y}_Code"] = df[f"NAICS_{y}_Code"].str.split(
            '\s*-\s*').apply(lambda x: list(range(int(x[0]), int(x[-1]) + 1)))
        df = df.explode(f"NAICS_{y}_Code")
        df[f"NAICS_{y}_Code"] = df[f"NAICS_{y}_Code"].astype(str)
        # load household and gov sectors - do this after explode because
        # household and gov sectors contain letters
        for s in ["Government", "Household"]:
            cw = (load_crosswalk(f"{s}_SectorCodes")[['Code', 'Name']]
                  .rename(columns={"Code": f"NAICS_{y}_Code",
                                   "Name": f"NAICS_{y}_Name"})
                  .drop_duplicates()
                  )
            df = pd.concat([df, cw], ignore_index=True)
        # sort and save csv
        df = (df
              .sort_values(f"NAICS_{y}_Code")
              .drop_duplicates()
              .reset_index(drop=True)
              )
        df.to_csv(f'{datapath}/Sector_{y}_Names.csv', index=False)


if __name__ == '__main__':
    update_naics_crosswalk()
    write_annual_naics_crosswalk()
    write_sector_name_crosswalk()
