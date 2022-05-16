# write_UDSA_ERS_FIWS_xwalk.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk linking Statistics Canada to NAICS

"""
import pandas as pd
from flowsa.settings import datapath
from scripts.FlowByActivity_Crosswalks.common_scripts import unique_activity_names, order_crosswalk

if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2011', '2015']
    # datsource
    datasource = 'StatCan_GDP'
    # df of unique ers activity names
    df_list = []
    for y in years:
        dfy = unique_activity_names(datasource, y)
        df_list.append(dfy)
    df = pd.concat(df_list, ignore_index=True).drop_duplicates()
    # add manual naics 2012 assignments
    # Activity and Sector are the same
    df['Sector'] = df['Activity'].copy()
    # modify the sector for activity = '31-33'
    df.loc[df['Activity'] == '31-33', 'Sector'] = '31'
    df = df.append(pd.DataFrame([['StatCan_GDP', '31-33', '32']],
                                columns=['ActivitySourceName', 'Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['StatCan_GDP', '31-33', '33']],
                                columns=['ActivitySourceName', 'Activity', 'Sector']), sort=True)
    # drop 'Other' and nan
    df = df[~df['Activity'].isin(['Other', 'nan'])]
    # Add additional columns
    df['SectorSourceName'] = "NAICS_2012_Code"
    df['SectorType'] = "I"
    # reorder
    df = order_crosswalk(df)
    # save as csv
    df.to_csv(datapath + "activitytosectormapping/" +
              "NAICS_Crosswalk_" + datasource + ".csv", index=False)
