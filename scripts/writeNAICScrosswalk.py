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
from flowsa.common import load_crosswalk, sector_level_key
from flowsa.settings import datapath, crosswalkpath


def load_naics_concordance(y1, y2):
    """
    Load the naics concordance from US Census, which are provided in naics6 format
    https://www.census.gov/naics/?68967

    Must be valid years
    For example: 2002 to 2007 and 2017 to 2022
    :return:
    """
    df_load = pd.read_csv(f'{datapath}/external_data/{y1}_to_{y2}_NAICS.csv', dtype=str)
    # drop first rows
    df = pd.DataFrame(df_load.loc[2:]).reset_index(drop=True)
    # Assign the column titles
    df.columns = df_load.loc[1, ]

    # df subset columns
    naics_cw = df[[f'{y1} NAICS Code', f'{y2} NAICS Code']].rename(
        columns={f'{y1} NAICS Code': f'NAICS_{y1}_Code',
                 f'{y2} NAICS Code': f'NAICS_{y2}_Code'})
    # ensure datatype is string
    naics_cw = naics_cw.astype(str)

    naics_cw = naics_cw.apply(lambda x: x.str.strip() if isinstance(x, str) else x)

    return naics_cw


def write_naics_year_concordance():
    """
    Generate a csv of 6-digit NAICS year concordances
    """

    # List of year pairs for concordances
    year_pairs = [('2002', '2007'), ('2007', '2012'), ('2012', '2017'), ('2017', '2022')]

    # empty df
    cw_merged = pd.DataFrame()
    # Loop through each pair of years and call the function
    for y1, y2 in year_pairs:
        cw = load_naics_concordance(y1, y2)

        if cw_merged.empty:
            cw_merged = cw
        else:
            # Merge df based on shared column
            cw_merged = pd.merge(cw_merged, cw, how='outer')

    # append household and gov data
    cw_name = ('Household_SectorCodes','Government_SectorCodes')
    for n in cw_name:
        df = load_crosswalk(n)
        df = df.query("NAICS_Level_to_Use_For == 'NAICS_6'")

        # Add rows to each column of the cw merged
        df_extend = pd.concat([df[['Code']]] * len(cw_merged.columns), axis=1)
        df_extend.columns = cw_merged.columns
        cw_merged = pd.concat([cw_merged, df_extend], ignore_index=True)

    # save as csv
    cw_merged.to_csv(f"{datapath}/NAICS_Year_Concordance.csv", index=False)


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


def write_annual_naics_crosswalk(year):
    """
    Create a NAICS 2 - 6 digit crosswalk
    :return:
    """
    cw = pd.read_csv(f'{datapath}/external_data/2-6 digit_{year}_Codes.csv', dtype=str)

    if year != "2002":
        # Delete first row, drop nulls, rename col, subset df by col
        cw = (cw
              .drop(index=0)
              .dropna(how="all")
              .rename(columns={cw.columns[1]: f'NAICS_{year}_Code'})
              )
        cw = cw[[f'NAICS_{year}_Code']]

    # drop "-"
    cw = cw[~cw[f'NAICS_{year}_Code'].str.contains('-')]

    # ensure all parent naics are included
    parent_set = set(cw[f'NAICS_{year}_Code'])
    missing_parents = set()
    for value in cw[f'NAICS_{year}_Code']:
        parent_value = value[:-1]
        while len(parent_value) >= 2:
            if parent_value not in parent_set:
                missing_parents.add(parent_value)
                parent_set.add(parent_value)
            parent_value = parent_value[:-1]

    cw2 = (pd.concat([cw, pd.DataFrame(missing_parents, columns=[f'NAICS_{year}_Code'])],
                    ignore_index=True)
           .sort_values(by=[f'NAICS_{year}_Code'], ascending=True)
           .reset_index(drop=True)
           )

    # append any unofficial sector codes defined within crosswalk files
    # and add to naics list
    missing_naics_df_list = []
    # read in all the crosswalk csv files (ends in toNAICS.csv)
    for file_name in glob.glob(f'{crosswalkpath}/NAICS_Crosswalk_*.csv'):
        # skip Statistics Canada GDP and BEA because not all sectors relevant
        if not any(s in file_name for s in ('StatCan', 'BEA')):
            df = pd.read_csv(file_name, low_memory=False, dtype=str)
            df = (df[['Sector']]
                  .query('Sector.str.len() > 6')
                  .query('~Sector.str.contains("-").values')
                  )
            # trim whitespace and cast as string, rename column
            df['Sector'] = df['Sector'].astype(str).str.strip()
            df = df.rename(columns={'Sector': f'NAICS_{year}_Code'})
            # find any NAICS that are in source crosswalk but not in
            # annual crosswalk
            common = df.merge(cw2[f'NAICS_{year}_Code'])
            missing_naics = df[(~df[f'NAICS_{year}_Code'].isin(common[f'NAICS_{year}_Code']))]
            # append to df list
            missing_naics_df_list.append(missing_naics)
    # concat df list and drop duplications
    missing_naics_df = \
        pd.concat(missing_naics_df_list, ignore_index=True,
                  sort=False).drop_duplicates().reset_index(drop=True)

    # add additional cddpath sectors - defined in HIO github repo
    cdd_sectors = pd.DataFrame(
        {f"NAICS_{year}_Code": ['5629202',
                                '5629203'
                                ]})

    # add missing naics to master naics crosswalk
    cw3 = pd.concat([cw2, missing_naics_df, cdd_sectors], ignore_index=True)

    # sort df
    cw3 = (cw3
           .sort_values([f'NAICS_{year}_Code'])
           .reset_index(drop=True)
           )

    # load BEA codes that will act as NAICS
    house = load_crosswalk('Household_SectorCodes')
    govt = load_crosswalk('Government_SectorCodes')
    bea = pd.concat([house, govt], ignore_index=True).rename(
        columns={'Code': f'NAICS_{year}_Code',
                 'NAICS_Level_to_Use_For': 'secLength'})
    bea = bea[[f'NAICS_{year}_Code', 'secLength']]

    # add column of sector length
    cw3['secLength'] = cw3[f'NAICS_{year}_Code'].apply(
        lambda x: f"NAICS_{str(len(x))}")
    # add bea codes subbing for NAICS
    cw4 = pd.concat([cw3, bea], ignore_index=True).drop_duplicates()
    # return max string length
    max_naics_length = cw4[f'NAICS_{year}_Code'].apply(lambda x: len(
        x)).max()

    # create dictionary of dataframes
    d = dict(tuple(cw4.groupby('secLength')))

    for k in d.keys():
        d[k].rename(columns=({f'NAICS_{year}_Code': k}), inplace=True)

    naics_cw = d['NAICS_2']
    for l in range(3, max_naics_length+1):
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

def write_sector_level_crosswalk():
    """
    Write a csv that contains sector codes with their sector level
    """
    df = pd.DataFrame()
    for y in ['2002', '2007', '2012', '2017', '2022']:
        cw = pd.read_csv(datapath / f'NAICS_{y}_Crosswalk.csv')
        df = pd.concat([df,cw], ignore_index=True)
    df2 = pd.melt(df, var_name='Sector_Level', value_name='Sector')
    df2 = (df2
           .drop_duplicates()
           .dropna()
           .sort_values(by=['Sector_Level', 'Sector'])
           .reset_index(drop=True)
           )
    df2 = df2.assign(SectorLength=df2['Sector_Level'].apply(
        lambda x: sector_level_key.get(x)))
    df2.SectorLength = df2.SectorLength.astype(int)
    df2.to_csv(f'{datapath}/Sector_Levels.csv', index=False)


if __name__ == '__main__':
    # update_naics_crosswalk()
    write_annual_naics_crosswalk('2002')
    write_annual_naics_crosswalk('2007')
    write_annual_naics_crosswalk('2012')
    write_annual_naics_crosswalk('2017')
    write_annual_naics_crosswalk('2022')
    # write_sector_name_crosswalk()
