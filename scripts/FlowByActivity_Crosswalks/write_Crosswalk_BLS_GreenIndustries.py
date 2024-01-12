# write_Crosswalk_BLS_GreenIndustries.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
A NAICS list for green goods and services industries created in 2010.
No longer being maintained by BLS.

Converted from a pdf to a csv prior to importing dataset into flowsa.

Website:
https://www.bls.gov/green/

Original PDF (downloaded on May 30, 2020):
https://www.bls.gov/green/industry_by_naics.pdf

"""
import pandas as pd
from flowsa.settings import datapath

# read the csv loaded as a raw datafile
df_raw = pd.read_csv(f"{datapath}/BLS_GreenIndustries_Raw.csv")

# only keep columns where naics is included in green goods and services
df1 = df_raw[df_raw['Included'] == 'Y']

# keep and rename first two columns
df2 = df1[['NAICS 2007', 'Title']]

# rename columns to match flowbyactivity format
df2 = df2.rename(columns={"NAICS 2007": "Sector",
                          "Title": "Activity"})
# add columns
df2['ActivitySourceName'] = 'BLS_GreenIndustries'
df2['SectorSourceName'] = 'NAICS_2007_Code'
df2['SectorType'] = None

# reorder columns
df3 = df2[['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector', 'SectorType']]
df3['Sector'] = df3['Sector'].astype(str).str.strip()

# sort df
df3 = df3.sort_values('Sector')
# reset index
df3.reset_index(drop=True, inplace=True)
# save as csv
df3.to_csv(f"{datapath}/activitytosectormapping/NAICS_Crosswalk_"
           f"BLS_GreenIndustries.csv", index=False)
