# write_Crosswalk_Census_VIP.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk for Census VIP to NAICS 2012.
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
                     usecols=['Name', '2012_NAICS_Code'])
    cw['Name'] = cw['Name'].str.lower()
    df['Name'] = df['Activity'].str.split(' - ').str[1].str.lower()
    df.loc[df['Activity'].str.contains('Residential'), 'Name'] = 'residential'
    df = df.merge(cw, how='left', on=['Name'])
    df = df.drop(columns='Name')
    df = df.rename(columns={'2012_NAICS_Code': 'Sector'})
    
    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2014']
    # datasource
    datasource = 'Census_VIP'
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
    df.to_csv(f"{datapath}/activitytosectormapping/"
              f"NAICS_Crosswalk_{datasource}.csv", index=False)
