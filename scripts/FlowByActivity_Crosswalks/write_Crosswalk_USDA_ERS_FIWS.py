# write_Crosswalk_USDA_ERS_FIWS.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk linking the downloaded USDA_ERS_FIWS to NAICS_12.
Created by selecting unique Activity Names and
manually assigning to NAICS

The assigned NAICS line up with 7/8 digit USDA CoA Cropland/Livestock
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

    # crops and livestock
    df.loc[df['Activity'] == 'All Commodities', 'Sector'] = '111-112'

    # CROPS

    # equivalent to crop production: 111
    df.loc[df['Activity'] == 'Crops', 'Sector'] = '111'

    # equivalent to soybean farming: 11111
    df.loc[df['Activity'] == 'Soybeans', 'Sector'] = '11111'

    # aggregates to oilseed (except soybean) farming: 11112
    df.loc[df['Activity'] == 'Flaxseed', 'Sector'] = '111120B'
    df.loc[df['Activity'] == 'Safflower', 'Sector'] = '111120E'
    df.loc[df['Activity'] == 'Sunflower', 'Sector'] = '111120G'
    df.loc[df['Activity'] == 'Oil crops, Miscellaneous', 'Sector'] = '111120B'

    #  aggregates to dry pea and bean farming: 11113
    df.loc[df['Activity'] == 'Dry beans', 'Sector'] = '111130I'
    df.loc[df['Activity'] == 'Dry peas', 'Sector'] = '111130J'
    df.loc[df['Activity'] == 'Lentils (Beans)', 'Sector'] = '111130E'

    # equivalent to wheat farming: 11114
    df.loc[df['Activity'] == 'Wheat', 'Sector'] = '11114'

    # aggregates to corn farming: 11115
    df.loc[df['Activity'] == 'Corn', 'Sector'] = '11115'

    # equivalent to rice farming: 11116
    df.loc[df['Activity'] == 'Rice', 'Sector'] = '11116'

    # aggregates to all other grain farming: 111199
    df.loc[df['Activity'] == 'Barley', 'Sector'] = '111199A'
    df.loc[df['Activity'] == 'Proso millet', 'Sector'] = '111199C'
    df.loc[df['Activity'] == 'Oats', 'Sector'] = '111199D'
    df.loc[df['Activity'] == 'Rye', 'Sector'] = '111199E'
    df.loc[df['Activity'] == 'Sorghum grain', 'Sector'] = '111199F'

    # equivalent to vegetable and melon farming: 1112

    # equivalent to potato farming: 111211
    df.loc[df['Activity'] == 'Potatoes', 'Sector'] = '111211A'
    df.loc[df['Activity'] == 'Sweet potatoes', 'Sector'] = '111211B'

    # equivalent to other vegetable (except potato) and melon farming:
    # 111219 (includes tomatoes)
    df.loc[df['Activity'] == 'Artichokes', 'Sector'] = '111219A'
    df.loc[df['Activity'] == 'Asparagus', 'Sector'] = '111219B'
    df.loc[df['Activity'] == 'Beans, green lima', 'Sector'] = '111219C'
    df.loc[df['Activity'] == 'Beans, snap', 'Sector'] = '111219D'
    df.loc[df['Activity'] == 'Broccoli', 'Sector'] = '111219E'
    df.loc[df['Activity'] == 'Cabbage', 'Sector'] = '111219F'
    df.loc[df['Activity'] == 'Carrots', 'Sector'] = '111219G'
    df.loc[df['Activity'] == 'Cauliflower', 'Sector'] = '111219H'
    df.loc[df['Activity'] == 'Celery', 'Sector'] = '111219I'
    df.loc[df['Activity'] == 'Corn, Sweet corn, fresh', 'Sector'] = '111219J'
    df.loc[df['Activity'] == 'Cucumbers', 'Sector'] = '111219K'
    df.loc[df['Activity'] == 'Garlic', 'Sector'] = '111219L'
    df.loc[df['Activity'] == 'Lettuce', 'Sector'] = '111219M'
    df.loc[df['Activity'] == 'Pumpkins', 'Sector'] = '111219N'
    df.loc[df['Activity'] == 'Onions', 'Sector'] = '111219P'
    df.loc[df['Activity'] == 'Peas, green', 'Sector'] = '111219Q'
    df.loc[df['Activity'] == 'Peppers, chile', 'Sector'] = '111219R'
    df.loc[df['Activity'] == 'Peppers, bell', 'Sector'] = '111219S'
    df.loc[df['Activity'] == 'Spinach', 'Sector'] = '111219T'
    df.loc[df['Activity'] == 'Squash', 'Sector'] = '111219U'
    df.loc[df['Activity'] == 'Taro', 'Sector'] = '111219V'
    df.loc[df['Activity'] == 'Tomatoes', 'Sector'] = '111219W'

    df.loc[df['Activity'] == 'Cantaloups', 'Sector'] = '111219X'
    df.loc[df['Activity'] == 'Honeydews', 'Sector'] = '111219Y'
    df.loc[df['Activity'] == 'Watermelons', 'Sector'] = '111219Z'

    # aggregates to fruit and tree nut farming: 1113
    df.loc[df['Activity'] == 'Fruits/Nuts', 'Sector'] = '1113'

    # aggregates to greenhouse nursery and floriculture production: 1114
    df.loc[df['Activity'] == 'Greenhouse/Nursery, Floriculture',
           'Sector'] = '1114'

    # equivalent to tobacco farming: 11191
    df.loc[df['Activity'] == 'Tobacco', 'Sector'] = '11191'

    # aggregates to cotton: 11192
    df.loc[df['Activity'] == 'Cotton', 'Sector'] = '11192'

    # aggregates to sugarcane farming: 11193
    df.loc[df['Activity'] == 'Cane for sugar', 'Sector'] = '11193'

    # aggregates to hay farming: 11194
    df.loc[df['Activity'] == 'Hay', 'Sector'] = '11194'

    # aggregates to all other crop farming: 11199
    df.loc[df['Activity'] == 'Sugar beets', 'Sector'] = '111991'
    df.loc[df['Activity'] == 'Peanuts', 'Sector'] = '111992'
    df.loc[df['Activity'] == 'Maple products', 'Sector'] = '111998B'
    df.loc[df['Activity'] == 'Hops', 'Sector'] = '111998E'
    df.loc[df['Activity'] == 'Mint', 'Sector'] = '111998G'
    df.loc[df['Activity'] == 'All other crops, Miscellaneous crops',
           'Sector'] = '111998I'
    df.loc[df['Activity'] == 'Mushrooms', 'Sector'] = '111998J'

    # ANIMALS

    # animal totals: 112
    df.loc[df['Activity'] == 'Animals and products', 'Sector'] = '112'

    # cattle ranching and farming: 1121
    # beef cattle ranching and farming including feedlots: 11211
    df.loc[df['Activity'] == 'Cattle and calves', 'Sector'] = '11211'
    # dairy cattle and milk production: 11212
    df.loc[df['Activity'] == 'Dairy products', 'Sector'] = '11212'

    # hog and pig farming: 1122
    df.loc[df['Activity'] == 'Hogs', 'Sector'] = '1122'

    # poultry and egg production: 1123
    df.loc[df['Activity'] == 'Poultry/Eggs', 'Sector'] = '1123'

    # sheep and goat farming: 1124
    # goat farming: 11242
    df.loc[df['Activity'] == 'Mohair', 'Sector'] = '11242'

    # animal aquaculture: 1125
    df.loc[df['Activity'] == 'Aquaculture', 'Sector'] = '1125'
    # other animal production: 112900
    # apiculture: 11291
    df.loc[df['Activity'] == 'Honey', 'Sector'] = '112910A'
    # all other animal production: 11299
    df.loc[df['Activity'] == 'Animals and products, Other animals and '
                             'products', 'Sector'] = '112990E'
    df.loc[df['Activity'] == 'Wool', 'Sector'] = '112990F'

    # FORESTRY
    df.loc[df['Activity'] == 'Forest products', 'Sector'] = '11531'

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2012', '2017']
    # datasource
    datasource = 'USDA_ERS_FIWS'
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
