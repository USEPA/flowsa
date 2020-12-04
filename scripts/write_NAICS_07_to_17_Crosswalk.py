# write_NAICS_from_Census.py (scripts)
# !/usr/bin/env python3
# coding=utf-8


"""
Uses a csv file manually loaded, originally from USEEIOR (4/18/2020), to form base NAICS crosswalk from 2007-2017
Loops through the source crosswalks to find any NAICS not in offical Census NAICS Code list. Adds the additional NAICS
to NAICS crosswalk.

- Writes reshaped file to datapath as csv.
"""

from flowsa.common import datapath, load_household_sector_codes
import glob
import pandas as pd
import numpy as np
#from rpy2.robjects.packages import importr
#from rpy2.robjects import pandas2ri


# does not work due to issues with rpy2. Crosswalk was manually copied from useeior and added as csv (4/18/2020)
# pandas2ri.activate()
# useeior = importr('useeior')
# NAICS_crosswalk = useeior.getMasterCrosswalk(2012)
# NAICS_crosswalk = pandas2ri.ri2py_dataframe(NAICS_crosswalk)

# update the useeior crosswalk with crosswalks created for flowsa datasets
# read the csv loaded as a raw datafile
naics = pd.read_csv(datapath + "NAICS_useeior_Crosswalk.csv")
naics = naics[naics['NAICS_2007_Code'].notna()]
# convert all rows to string
naics = naics.astype(str)


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
        # drop sectors with '-'
        missing_naics = missing_naics[~missing_naics[naics_year].str.contains('-')]
        # append to df list
        missing_naics_df_list.append(missing_naics)
# concat df list and drop duplications
missing_naics_df = pd.concat(missing_naics_df_list,
                             ignore_index=True, sort=False).drop_duplicates().reset_index(drop=True)
missing_naics_df = missing_naics_df[missing_naics_df['NAICS_2012_Code'] != 'None']
# sort df
missing_naics_df = missing_naics_df.sort_values(['NAICS_2012_Code', 'NAICS_2007_Code'])
missing_naics_df = missing_naics_df.reset_index(drop=True)

# add missing naics to master naics crosswalk
total_naics= naics.append(missing_naics_df, ignore_index=True)

# append household codes
household = load_household_sector_codes()
h = household['Code'].drop_duplicates().tolist()
for i in h:
    if (total_naics['NAICS_2012_Code'] != i).all():
        total_naics = total_naics.append({'NAICS_2007_Code': np.nan, 'NAICS_2012_Code': i, 'NAICS_2017_Code': np.nan},
                                         ignore_index =True)

# sort df
total_naics = total_naics.sort_values(['NAICS_2012_Code', 'NAICS_2007_Code']).drop_duplicates()
total_naics = total_naics[~total_naics['NAICS_2012_Code'].isin(['None', 'unknown', 'nan',
                                                                'Unknown', np.nan])].reset_index(drop=True)

# save as csv
total_naics.to_csv(datapath + "NAICS_07_to_17_Crosswalk.csv", index=False)
