# write_UDSA_IWMS_crosswalk.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk linking the USDA Irrigation and Water Management Surveyto NAICS_12.
Created by selecting unique
Activity Names and manually assigning to NAICS

NAICS8 are unofficial and are not used again after initial aggregation to NAICS6. NAICS8 are based
on NAICS definitions from the Census.

7/8 digit NAICS align with USDA ERS FIWS

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

    # assigning iwms activity items to naics 12,
    df.loc[df['Activity'] == 'BEANS, DRY EDIBLE, INCL CHICKPEAS', 'Sector'] = '11113'

    df.loc[df['Activity'] == 'CORN, GRAIN', 'Sector'] = '111150A'
    df.loc[df['Activity'] == 'CORN, SILAGE', 'Sector'] = '111150B'

    df.loc[df['Activity'] == 'COTTON', 'Sector'] = '11192'

    # a number of naics are the generalized "crops, other", so manually add each row
    # tobacco farming, sugarcane farming, oilseed (except soybean) farming,
    # sugarbeets
    df.loc[df['Activity'] == 'CROPS, OTHER',
           'Sector'] = pd.Series([['11191', '11193', '11112', '111991', '111998'
                                   ]]*df.shape[0])

    df.loc[df['Activity'] == 'HAY & HAYLAGE, (EXCL ALFALFA)', 'Sector'] = '111940A'
    df.loc[df['Activity'] == 'HAY & HAYLAGE, ALFALFA', 'Sector'] = '111940B'

    df.loc[df['Activity'] == 'HORTICULTURE TOTALS', 'Sector'] = '1114'

    df.loc[df['Activity'] == 'PASTURELAND', 'Sector'] = '112'

    # aggregates to fruit and tree nut farming: 1113, orange groves, citrus except orange groves
    df.loc[df['Activity'] == 'ORCHARDS', 'Sector'] = pd.Series(
        [['111331', '111332', '111335', '111336', '111339',
          '11131', '11132']]*df.shape[0])

    df.loc[df['Activity'] == 'BERRY TOTALS', 'Sector'] = pd.Series([[
        '111333', '111334']]*df.shape[0])

    df.loc[df['Activity'] == 'PEANUTS', 'Sector'] = '111992'

    df.loc[df['Activity'] == 'RICE', 'Sector'] = '11116'

    # seven types of other small grains, so manually add 6 rows
    # BARLEY, BUCKWHEAT, MILLET, PROSO, OATS, RYE, TRITICALE, WILD RICE
    df.loc[df['Activity'] == 'SMALL GRAINS, OTHER', 'Sector'] = pd.Series([[
        '111199A', '111199B', '111199C', '111199D', '111199E', '111199I',
        '111199J']]*df.shape[0])

    # three types of sorghum, so manually add two rows
    # grain, syrup, silage
    df.loc[df['Activity'] == 'SORGHUM, GRAIN', 'Sector'] = pd.Series([[
        '111199F', '111199G', '111199H']]*df.shape[0])

    df.loc[df['Activity'] == 'SOYBEANS', 'Sector'] = '11111'

    df.loc[df['Activity'] == 'VEGETABLE TOTALS', 'Sector'] = '1112'

    df.loc[df['Activity'] == 'WHEAT', 'Sector'] = '11114'

    # explode so each sector is on separate line
    df = df.explode('Sector')

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2013', '2018']
    # datasource
    datasource = 'USDA_IWMS'
    # df of unique ers activity names
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
