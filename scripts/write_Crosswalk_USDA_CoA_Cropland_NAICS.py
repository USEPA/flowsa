# write_Crosswalk_USDA_CoA_Cropland_NAICS.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk for CoA Cropland Naics to NAICS 2012. Downloaded data is already provided in NAICS
"""
import pandas as pd
from flowsa.common import datapath
from scripts.common_scripts import unique_activity_names, order_crosswalk


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2012', '2017']
    # flowclass
    flowclass = ['Land']
    # datasource
    datasource = 'USDA_CoA_Cropland_NAICS'
    # df of unique ers activity names
    df = unique_activity_names(flowclass, years, datasource)
    # drop activities with symbol '&'
    df = df[~df['Activity'].str.contains('&')]
    # Activity and Sector are the same
    df['Sector'] = df['Activity'].copy()
    # modify the sector for activity ranges
    #df.loc[df['Activity'] == '11193 & 11194 & 11199', 'Sector'] = '11193'
    df = df.append(pd.DataFrame([['USDA_CoA_Cropland_NAICS', '11193 & 11194 & 11199', '11193']],
                                columns=['ActivitySourceName', 'Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['USDA_CoA_Cropland_NAICS', '11193 & 11194 & 11199', '11194']],
                                columns=['ActivitySourceName', 'Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['USDA_CoA_Cropland_NAICS', '11193 & 11194 & 11199', '11199']],
                                columns=['ActivitySourceName', 'Activity', 'Sector']), sort=True)
    #df.loc[df['Activity'] == '1125 & 1129', 'Sector'] = '1125'
    df = df.append(pd.DataFrame([['USDA_CoA_Cropland_NAICS', '1125 & 1129', '1125']],
                                columns=['ActivitySourceName', 'Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['USDA_CoA_Cropland_NAICS', '1125 & 1129', '1129']],
                                columns=['ActivitySourceName', 'Activity', 'Sector']), sort=True)
    # Add additional columns
    df['SectorSourceName'] = "NAICS_2012_Code"
    df['SectorType'] = None
    # reorder
    df = df[['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector', 'SectorType']]
    # sort df
    df = order_crosswalk(df)
    # save as csv
    df.to_csv(datapath + "activitytosectormapping/" + "Crosswalk_" + datasource + "_toNAICS.csv", index=False)
