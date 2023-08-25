# write_Crosswalk_EPA_CDDPath.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk for EPA CDDPath to NAICS 2012.
"""
import pandas as pd
from flowsa.settings import datapath, externaldatapath
from scripts.FlowByActivity_Crosswalks.common_scripts import unique_activity_names, order_crosswalk


def assign_naics(df):
    """
    Function to assign NAICS codes to each dataframe activity
    :param df: df, a FlowByActivity subset that contains unique activity names
    :return: df with assigned Sector columns
    """

    cw = pd.read_csv(f"{externaldatapath}/VIPNametoNAICStoFF.csv",
                     usecols=['FF Source Category', '2012_NAICS_Code'],
                     dtype='str').drop_duplicates()
    df = df.merge(cw, how='left', left_on=['Activity'],
                  right_on=['FF Source Category'])
    df = df.drop(columns='FF Source Category')
    
    # append additional mapping for Wood see EPA_CDDPath.py
    # function assign_wood_to_engineering()
    df = pd.concat([df, pd.DataFrame(
        [['Other - Wood', 'EPA_CDDPath', '237990']],
        columns=['Activity', 'ActivitySourceName', '2012_NAICS_Code'])], ignore_index=True)
    
    df = df.rename(columns={'2012_NAICS_Code': 'Sector'})

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2014']
    # datasource
    datasource = 'EPA_CDDPath'
    # df of unique activity names
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
    df.to_csv(f"{datapath}/activitytosectormapping/NAICS_Crosswalk_"
              f"{datasource}.csv", index=False)
