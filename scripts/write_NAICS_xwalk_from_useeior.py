# write_NAICS_from_Census.py (scripts)
# !/usr/bin/env python3
# coding=utf-8


"""
Grabs NAICS 2007, 2012, and 2017 codes from useeior.

- Writes reshaped file to datapath as csv.
"""

from flowsa.common import datapath
import glob
import pandas as pd
#from rpy2.robjects.packages import importr
#from rpy2.robjects import pandas2ri


# does not work due to issues with rpy2. Crosswalk was manually copied from useeior and added as csv (4/18/2020)

# pandas2ri.activate()
#
# useeior = importr('useeior')
#
# NAICS_crosswalk = useeior.getMasterCrosswalk(2012)
# NAICS_crosswalk = pandas2ri.ri2py_dataframe(NAICS_crosswalk)

# update the useeior crosswalk with crosswalks created for flowsa datasets
# read the csv loaded as a raw datafile
naics = pd.read_csv(datapath + "NAICS_useeior_Crosswalk.csv")
naics = naics[naics['NAICS_2007_Code'].notna()]
# add new column for 2002
naics.insert(loc=0, column="NAICS_2002_Code", value=None)
# convert all rows to string
naics = naics.astype(str)


missing_naics_df_list = []
# read in all the crosswalk csv files (ends in toNAICS.csv)
for file_name in glob.glob(datapath + "activitytosectormapping/"+'*_toNAICS.csv'):
    df = pd.read_csv(file_name, low_memory=False)
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
    missing_naics = df[(~df[naics_year].isin(common[naics_year])) & (~df[naics_year].isin(common[naics_year]))]
    # append to df list
    missing_naics_df_list.append(missing_naics)
# concat df list and drop duplications
missing_naics_df = pd.concat(missing_naics_df_list, ignore_index=True, sort=True).drop_duplicates()
# sort df
missing_naics_df = missing_naics_df.sort_values(['NAICS_2012_Code', 'NAICS_2007_Code', 'NAICS_2002_Code'])
missing_naics_df = missing_naics_df.reset_index(drop=True)

# add missing naics to master naics crosswalk
total_naics= naics.append(missing_naics_df, sort=True)
# sort df
total_naics = total_naics.sort_values(['NAICS_2012_Code', 'NAICS_2007_Code', 'NAICS_2002_Code'])

# save as csv
total_naics.to_csv(datapath + "NAICS_07_to_17_Crosswalk.csv", index=False)



