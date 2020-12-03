# write_Crosswalk_Census_CBP.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""
Create a crosswalk for Census CBP to NAICS 2012. Downloaded data is already provided in NAICS
"""
import pandas as pd
from flowsa.common import datapath
from scripts.common_scripts import unique_activity_names, order_crosswalk


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2012']
    # flowclass
    flowclass = ['Employment', 'Money', 'Other']
    # datasource
    datasource = 'Census_CBP'
    # df of unique ers activity names
    df = unique_activity_names(flowclass, years, datasource)
    # Activity and Sector are the same
    df['Sector'] = df['Activity'].copy()
    # modify the sector for activity = '31-33'
    df.loc[df['Activity'] == '31-33', 'Sector'] = '31'
    df = df.append(pd.DataFrame([['Census_CBP', '31-33', '32']],
                                columns=['ActivitySourceName', 'Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['Census_CBP', '31-33', '33']],
                                columns=['ActivitySourceName', 'Activity', 'Sector']), sort=True)
    # Add additional columns
    df['SectorSourceName'] = "NAICS_2012_Code"
    df['SectorType'] = "I"
    # reorder
    df = order_crosswalk(df)
    # save as csv
    df.to_csv(datapath + "activitytosectormapping/" + "Crosswalk_" + datasource + "_toNAICS.csv", index=False)
