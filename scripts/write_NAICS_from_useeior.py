# write_NAICS_from_useeior.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""
3 scripts:

A script to get NAICS names and a NAICS 2-3-4-5-6 crosswalk.

- from useeior amd store them as .csv.
- Depends on rpy2 and tzlocal as well as having R installed and useeior installed.

Loops through the source crosswalks to find any NAICS not in offical Census NAICS Code list. Adds the additional NAICS
to NAICS crosswalk.

- Writes reshaped file to datapath as csv.
"""

from flowsa.common import datapath, load_household_sector_codes
import glob
import pandas as pd
import numpy as np
import rpy2.robjects.packages as packages
from rpy2.robjects import pandas2ri
from flowsa.flowbyfunctions import replace_NoneType_with_empty_cells, replace_strings_with_NoneType



def import_useeior_mastercrosswalk():
    """
    Load USEEIOR's MasterCrosswalk that links BEA data to NAICS
    :return:
    """
    pandas2ri.activate()
    # import the useeior package (r package)
    useeior = packages.importr('useeior')
    # load the .Rd file for
    cw = packages.data(useeior).fetch('MasterCrosswalk2012')['MasterCrosswalk2012']

    return cw


def write_naics_2012_crosswalk():
    """
    Create a NAICS 2 - 6 digit crosswalk
    :return:
    """

    # load the useeior mastercrosswalk
    cw_load = import_useeior_mastercrosswalk()

    # extract naics 2012 code column and drop duplicates and empty cells
    cw = cw_load[['NAICS_2012_Code']].drop_duplicates()
    cw = replace_NoneType_with_empty_cells(cw)
    cw = cw[cw['NAICS_2012_Code'] != '']

    # dictionary to replace housing and gov't transport sectors after subsetting by naics length
    dict_replacement = {'F0': 'F010',
                        'F01': 'F010',
                        'F0100': 'F01000',
                        'S0': 'S00201',
                        'S00': 'S00201',
                        'S002': 'S00201',
                        'S0020': 'S00201'
                        }

    # define sectors that might need to be appended
    house_4 = ['F010']
    house_6 = ['F01000']
    govt = ['S00201']

    # extract naics by length
    for i in range(2, 7):
        cw_name = 'cw_' + str(i)
        cw_col = 'NAICS_' + str(i)
        cw_col_m1 = 'NAICS_' + str(i-1)
        vars()[cw_name] = cw[cw['NAICS_2012_Code'].apply(lambda x: len(x) == i)].\
            reset_index(drop=True).rename(columns={'NAICS_2012_Code': cw_col})
        # address exceptions to naics length rule - housing and gov't sector transport
        vars()[cw_name][cw_col] = vars()[cw_name][cw_col].replace(dict_replacement)
        # add some housing/gov't transport sectors, depending on length
        if i in range(2, 4):
            vars()[cw_name] = vars()[cw_name].append(pd.DataFrame(house_4, columns=[cw_col]), ignore_index=True)
        if i == 5:
            vars()[cw_name] = vars()[cw_name].append(pd.DataFrame(house_6, columns=[cw_col]), ignore_index=True)
        if i in range(2, 6):
            vars()[cw_name] = vars()[cw_name].append(pd.DataFrame(govt, columns=[cw_col]), ignore_index=True)
        # add columns to dfs with naics length - 1
        if i in range(3, 7):
            vars()[cw_name][cw_col_m1] = vars()[cw_name][cw_col].apply(lambda x: x[0:i-1])
            # address exceptions to naics length rule - housing and gov't sector transport
            vars()[cw_name][cw_col_m1] = vars()[cw_name][cw_col_m1].replace(dict_replacement)

    # merge dfs of various lengths
    naics_cw = cw_2.copy()
    for i in range(3, 7):
        cw_merge = 'cw_' + str(i)
        naics_cw = naics_cw.merge(vars()[cw_merge], how='outer')

    # save as csv
    naics_cw.to_csv(datapath + "NAICS_2012_Crosswalk.csv", index=False)

    return None


def load_naics_02_to_07_crosswalk():
    """
    Load the 2002 to 2007 crosswalk from US Census
    :return:
    """
    naics_url = 'https://www.census.gov/eos/www/naics/concordances/2002_to_2007_NAICS.xls'
    df_load = pd.read_excel(naics_url)
    # drop first rows
    df = pd.DataFrame(df_load.loc[2:]).reset_index(drop=True)
    # Assign the column titles
    df.columns = df_load.loc[1, ]

    # df subset columns
    naics_02_to_07_cw = df[['2002 NAICS Code', '2007 NAICS Code']].rename(columns={'2002 NAICS Code': 'NAICS_2002_Code',
                                                                                   '2007 NAICS Code': 'NAICS_2007_Code'})
    # ensure datatype is string
    naics_02_to_07_cw = naics_02_to_07_cw.astype(str)

    naics_02_to_07_cw = naics_02_to_07_cw.apply(lambda x: x.str.strip() if isinstance(x, str) else x)

    return naics_02_to_07_cw

def update_naics_crosswalk():
    """
    update the useeior crosswalk with crosswalks created for flowsa datasets - want to add any NAICS > 6 digits

    Add NAICS 2002
    :return:
    """

    # read useeior master crosswalk, subset NAICS columns
    naics_load = import_useeior_mastercrosswalk()
    naics = naics_load[['NAICS_2007_Code', 'NAICS_2012_Code', 'NAICS_2017_Code']].drop_duplicates().reset_index(drop=True)
    # convert all rows to string
    naics = naics.astype(str)
    # ensure all None are NoneType
    naics = replace_strings_with_NoneType(naics)
    # drop rows where all None
    naics = naics.dropna(how='all')

    # find any NAICS where length > 6 that are sed for allocation purposes and add to naics list
    missing_naics_df_list = []
    # read in all the crosswalk csv files (ends in toNAICS.csv)
    for file_name in glob.glob(datapath + "activitytosectormapping/"+'*_toNAICS.csv'):
        # skip Statistics Canada GDP because not all sectors relevant
        if file_name != 'C:/Users/cbirney/git_projects/flowsa/flowsa/data/activitytosectormapping\Crosswalk_StatCan_GDP_toNAICS.csv':
            df = pd.read_csv(file_name, low_memory=False, dtype=str)
            # convert all rows to string
            df = df.astype(str)
            # determine sector year
            naics_year = df['SectorSourceName'].all()
            # subset dataframe so only sector
            df = df[['Sector']]
            # trim whitespace and cast as string, rename column
            df['Sector'] = df['Sector'].astype(str).str.strip()
            df = df.rename(columns={'Sector': naics_year})
            # extract sector year column from master crosswalk
            df_naics = naics[[naics_year]]
            # find any NAICS that are in source crosswalk but not in mastercrosswalk
            common = df.merge(df_naics, on=[naics_year, naics_year])
            missing_naics = df[(~df[naics_year].isin(common[naics_year]))]
            # extract sectors where len > 6 and that does not include a '-'
            missing_naics = missing_naics[missing_naics[naics_year].apply(lambda x: len(x) > 6)]
            if len(missing_naics) != 0:
                missing_naics = missing_naics[~missing_naics[naics_year].str.contains('-')]
                # append to df list
                missing_naics_df_list.append(missing_naics)
    # concat df list and drop duplications
    missing_naics_df = pd.concat(missing_naics_df_list,
                                 ignore_index=True, sort=False).drop_duplicates().reset_index(drop=True)
    # sort df
    missing_naics_df = missing_naics_df.sort_values(['NAICS_2012_Code'])
    missing_naics_df = missing_naics_df.reset_index(drop=True)

    # add missing naics to master naics crosswalk
    total_naics= naics.append(missing_naics_df, ignore_index=True)

    # sort df
    total_naics = total_naics.sort_values(['NAICS_2012_Code', 'NAICS_2007_Code']).drop_duplicates()
    total_naics = total_naics[~total_naics['NAICS_2012_Code'].isin(['None', 'unknown', 'nan',
                                                                    'Unknown', np.nan])].reset_index(drop=True)

    # convert all columns to string
    total_naics = total_naics.astype(str)

    # add naics 2002
    naics_02 = load_naics_02_to_07_crosswalk()
    naics_cw = pd.merge(total_naics, naics_02, how='left')

    # ensure NoneType
    naics_cw = replace_strings_with_NoneType(naics_cw)

    # reorder
    naics_cw = naics_cw[['NAICS_2002_Code', 'NAICS_2007_Code', 'NAICS_2012_Code', 'NAICS_2017_Code']]

    # save as csv
    naics_cw.to_csv(datapath + "NAICS_Crosswalk.csv", index=False)

    return None
