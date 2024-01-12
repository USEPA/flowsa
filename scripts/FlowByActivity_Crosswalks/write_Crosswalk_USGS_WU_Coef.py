# write_Crosswalk_USGS_WU_Coef.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk linking the USGS Water Use Coefficients
(for animals) to NAICS_12. Created by selecting unique
Activity Names and manually assigning to NAICS

"""
import pandas as pd
from flowsa.settings import datapath
from scripts.FlowByActivity_Crosswalks.common_scripts import unique_activity_names, order_crosswalk


def assign_naics(df):
    """
    Function to assign NAICS codes to each dataframe activity
    :param df: df, a FlowByActivity subset that contains unique activity names
    :return: df with assigned Sector columns
    """

    # assign sector source name
    df['SectorSourceName'] = 'NAICS_2012_Code'

    # cattle ranching and farming: 1121
    # beef cattle ranching and farming including feedlots: 11211
    df.loc[df['Activity'] == 'Beef and other cattle, including calves',
           'Sector'] = pd.Series([['11211', '11213']]*df.shape[0])
    # dairy cattle and milk production: 11212
    df.loc[df['Activity'] == 'Dairy cows', 'Sector'] = '11212'
    # hog and pig farming: 1122
    df.loc[df['Activity'] == 'Hogs and pigs', 'Sector'] = '1122'
    # poultry and egg production: 1123
    # chicken egg production: 11231
    df.loc[df['Activity'] == 'Laying hens', 'Sector'] = '11231'
    # broilers and other meat-type chicken production: 11232
    df.loc[df['Activity'] == 'Broilers and other chickens',
           'Sector'] = pd.Series([['11232', '11239', '11293']]*df.shape[0])
    # turkey production: 11233
    df.loc[df['Activity'] == 'Turkeys', 'Sector'] = '11233'
    # poultry hatcheries: 11234
    # # other poultry production: 11239
    # sheep and goat farming: 1124
    # sheep farming: 11241
    df.loc[df['Activity'] == 'Sheep and lambs',
           'Sector'] = pd.Series([['11241', '11299']]*df.shape[0])
    # goat farming: 11242
    df.loc[df['Activity'] == 'Goats', 'Sector'] = '11242'
    # animal aquaculture: 1125
    # other animal production: 1129
    # apiculture: 11291
    # horse and other equine production: 11292
    df.loc[df['Activity'] == 'Horses (including ponies, mules, burrows, ' \
                             'and donkeys)', 'Sector'] = '11292'
    # fur-bearing animal and rabbit production: 11293
    # all other animal production: 11299

    # explode so each sector is on separate line
    df = df.explode('Sector')

    return df


if __name__ == '__main__':
    # select unique activity names from file
    years = ['2005']
    # datasource
    datasource = 'USGS_WU_Coef'
    df_list = []
    for y in years:
        dfy = unique_activity_names(datasource, y)
        df_list.append(dfy)
    df = pd.concat(df_list, ignore_index=True).drop_duplicates()
    # add manual naics 2012 assignments
    df = assign_naics(df)
    # drop any rows where naics12 is 'nan'
    # (because level of detail not needed or to prevent double counting)
    df.dropna(subset=["Sector"], inplace=True)
    # assign sector type
    df['SectorType'] = None
    # sort df
    df = order_crosswalk(df)
    # save as csv
    df.to_csv(f"{datapath}/activitytosectormapping/NAICS_Crosswalk_"
              f"{datasource}.csv", index=False)
