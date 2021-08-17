# write_Crosswalk_CalRecycle_WasteCharacterization.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk for CalRecycle Waste Characterization to NAICS 2012.
"""
import pandas as pd
from flowsa.common import datapath
from common_scripts import unique_activity_names, order_crosswalk


def assign_naics(df):
    """
    Function to assign NAICS codes to each dataframe activity
    :param df: df, a FlowByActivity subset that contains unique activity names
    :return: df with assigned Sector columns
    """

    df.loc[df['Activity'] == 'Arts Entertainment Recreation', 'Sector'] = '111'
    df.loc[df['Activity'] == 'Durable Wholesale Trucking', 'Sector'] = '112'
    df.loc[df['Activity'] == 'Education', 'Sector'] = '333'
    df.loc[df['Activity'] == 'Electronic Equipment', 'Sector'] = ''
    df.loc[df['Activity'] == 'Food Beverage Stores', 'Sector'] = ''
    df.loc[df['Activity'] == 'Food Nondurable Wholesale', 'Sector'] = ''
    df.loc[df['Activity'] == 'Hotel Lodging', 'Sector'] = ''
    df.loc[df['Activity'] == 'Medical Health', 'Sector'] = ''
    df.loc[df['Activity'] == 'Multifamily', 'Sector'] = ''
    df.loc[df['Activity'] == 'Other Manufacturing', 'Sector'] = ''
    df.loc[df['Activity'] == 'Other Retail Trade', 'Sector'] = ''
    df.loc[df['Activity'] == 'Public Administration', 'Sector'] = ''
    df.loc[df['Activity'] == 'Restaurants', 'Sector'] = ''
    df.loc[df['Activity'] == 'Services Management Administration Support Social', 'Sector'] = ''
    df.loc[df['Activity'] == 'Services Professional Technical Financial', 'Sector'] = ''
    df.loc[df['Activity'] == 'Services Repair Personal', 'Sector'] = ''
    df.loc[df['Activity'] == 'Not Elsewhere Classified', 'Sector'] = ''
    
    return df

if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2014']
    # datasource
    datasource = 'CalRecycle_WasteCharacterization'
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
    df.to_csv(datapath + "activitytosectormapping/" +
              "Crosswalk_" + datasource + "_toNAICS.csv", index=False)
