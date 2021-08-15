# write_Crosswalk_Census_VIP.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk for EPA CDD Path to NAICS 2012.
"""
import pandas as pd
from flowsa.common import datapath, externaldatapath
from scripts.common_scripts import unique_activity_names, order_crosswalk


def assign_naics(df):
    """
    """

    cw = pd.read_csv(externaldatapath + 'VIPNametoNAICStoFF.csv',
                     usecols=['FF Source Category','2012_NAICS_Code']).drop_duplicates()
    df = df.merge(cw, how = 'left', left_on = ['Activity'],
                  right_on = ['FF Source Category'])
    df = df.drop(columns='FF Source Category')
    df = df.rename(columns={'2012_NAICS_Code':'Sector'})
    
    return df

if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2014']
    # datasource
    datasource = 'EPA_CDDPath'
    # df of unique ers activity names
    df_list = []
    for y in years:
        dfy = unique_activity_names(datasource, y)
        df_list.append(dfy)
    df = pd.concat(df_list, ignore_index=True).drop_duplicates()

    df = assign_naics(df)
    # Add additional columns
    df['SectorSourceName'] = "NAICS_2012_Code"
    df['SectorType'] = 'I'
    # reorder
    df = order_crosswalk(df)
    # save as csv
    df.to_csv(datapath + "activitytosectormapping/" +
              "Crosswalk_" + datasource + "_toNAICS.csv", index=False)
