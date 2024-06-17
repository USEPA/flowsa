# write_Crosswalk_USDA_CoA_Livestock.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk linking the downloaded USDA_CoA_Livestock to NAICS_12.
Created by selecting unique Activity Names and
manually assigning to NAICS

NAICS8 are unofficial and are not used again after initial aggregation to NAICS6.
NAICS8 are based on NAICS definitions from the Census.

7/8 digit NAICS align with USDA ERS IWMS
"""

import pandas as pd
from pathlib import Path
import sys
from flowsa.settings import datapath

cw_path = Path(__file__).parents[1]
sys.path.append(str(cw_path / 'FlowByActivity_Crosswalks'))  # accepts str, not pathlib obj
from FlowByActivity_Crosswalks.common_scripts import unique_activity_names, order_crosswalk


def assign_naics(df):
    """
    Function to assign NAICS codes to each dataframe activity
    :param df: df, a FlowByActivity subset that contains unique activity names
    :return: df with assigned Sector columns
    """
    # assign sector source name
    df['SectorSourceName'] = 'NAICS_2012_Code'

    # cattle ranching and farming: 1121
    df.loc[df['Activity'] == 'CATTLE, INCL CALVES', 'Sector'] = '1121'

    # dual-purpose cattle ranching and farming: 11213
    df.loc[df['Activity'] == 'CATTLE, (EXCL COWS)', 'Sector'] = '112130A'
    df.loc[df['Activity'] == 'CATTLE, COWS', 'Sector'] = '112130B'

    # hog and pig farming: 1122
    df.loc[df['Activity'] == 'HOGS', 'Sector'] = '1122'

    # poultry and egg production: 1123
    df.loc[df['Activity'] == 'POULTRY TOTALS', 'Sector'] = '1123'

    # chicken egg production: 11231
    df.loc[df['Activity'] == 'CHICKENS, LAYERS', 'Sector'] = '11231'

    # broilers and other meat-type chicken production: 11232
    df.loc[df['Activity'] == 'CHICKENS, BROILERS', 'Sector'] = '112320A'
    df.loc[df['Activity'] == 'CHICKENS, PULLETS, REPLACEMENT',
           'Sector'] = '112320B'
    df.loc[df['Activity'] == 'CHICKENS, ROOSTERS', 'Sector'] = '112320C'

    # turkey production: 11233
    df.loc[df['Activity'] == 'TURKEYS', 'Sector'] = '11233'

    # poultry hatcheries: 11234

    # other poultry production: 11239
    df.loc[df['Activity'] == 'CHUKARS', 'Sector'] = '112390A'
    df.loc[df['Activity'] == 'DUCKS', 'Sector'] = '112390B'
    df.loc[df['Activity'] == 'EMUS', 'Sector'] = '112390C'
    df.loc[df['Activity'] == 'GEESE', 'Sector'] = '112390D'
    df.loc[df['Activity'] == 'GUINEAS', 'Sector'] = '112390E'
    df.loc[df['Activity'] == 'OSTRICHES', 'Sector'] = '112390F'
    df.loc[df['Activity'] == 'PARTRIDGES, HUNGARIAN', 'Sector'] = '112390G'
    df.loc[df['Activity'] == 'PEAFOWL, HENS & COCKS', 'Sector'] = '112390H'
    df.loc[df['Activity'] == 'PHEASANTS', 'Sector'] = '112390J'
    df.loc[df['Activity'] == 'PIGEONS & SQUAB', 'Sector'] = '112390K'
    df.loc[df['Activity'] == 'POULTRY, OTHER', 'Sector'] = '112390L'
    df.loc[df['Activity'] == 'QUAIL', 'Sector'] = '112390M'
    df.loc[df['Activity'] == 'RHEAS', 'Sector'] = '112390N'

    # sheep and goat farming: 1124
    df.loc[df['Activity'] == 'SHEEP & GOATS TOTALS', 'Sector'] = '1124'

    # sheep farming: 11241
    df.loc[df['Activity'] == 'SHEEP, INCL LAMBS', 'Sector'] = '112410A'
    df.loc[df['Activity'] == 'SHEEP, INCL LAMBS, HAIR SHEEP OR WOOL-HAIR CROSSES',
           'Sector'] = '112410B'

    # goat farming: 11242
    df.loc[df['Activity'] == 'GOATS', 'Sector'] = '11242'

    # animal aquaculture: 1125
    df.loc[df['Activity'] == 'AQUACULTURE TOTALS', 'Sector'] = '1125'

    # other animal production: 1129

    # apiculture: 11291
    df.loc[df['Activity'] == 'HONEY', 'Sector'] = '112910A'
    df.loc[df['Activity'] == 'HONEY, BEE COLONIES', 'Sector'] = '112910B'

    # horse and other equine production: 11292
    df.loc[df['Activity'] == 'EQUINE, (HORSES & PONIES) & (MULES & BURROS & ' \
                             'DONKEYS)', 'Sector'] = '11292'
    df.loc[df['Activity'] == 'EQUINE, HORSES & PONIES', 'Sector'] = '112920A'
    df.loc[df['Activity'] == 'EQUINE, MULES & BURROS & DONKEYS',
           'Sector'] = '112920B'

    # fur-bearing animal and rabbit production: 11293
    df.loc[df['Activity'] == 'MINK, LIVE', 'Sector'] = '112930A'
    df.loc[df['Activity'] == 'RABBITS, LIVE', 'Sector'] = '112930B'

    # all other animal production: 11299
    df.loc[df['Activity'] == 'ALPACAS', 'Sector'] = '112990A'
    df.loc[df['Activity'] == 'BISON', 'Sector'] = '112990B'
    df.loc[df['Activity'] == 'DEER', 'Sector'] = '112990C'
    df.loc[df['Activity'] == 'ELK', 'Sector'] = '112990D'
    df.loc[df['Activity'] == 'LLAMAS', 'Sector'] = '112990E'

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2012', '2017', '2022']
    # datasource
    datasource = 'USDA_CoA_Livestock'
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
