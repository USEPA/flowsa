# write_Crosswalk_USDA_CoA_Livestock.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""
Create a crosswalk linking the downloaded USDA_CoA_Livestock to NAICS_12. Created by selecting unique Activity Names and
manually assigning to NAICS

NAICS8 are unofficial and are not used again after initial aggregation to NAICS6. NAICS8 are based
on NAICS definitions from the Census.

7/8 digit NAICS align with USDA ERS FIWS
"""
from flowsa.common import datapath
from scripts.common_scripts import unique_activity_names, order_crosswalk


def assign_naics(df):
    """manually assign each ERS activity to a NAICS_2012 code"""
    # assign sector source name
    df['SectorSourceName'] = 'NAICS_2012_Code'

    # cattle ranching and farming: 1121
    df.loc[df['Activity'] == 'CATTLE, INCL CALVES', 'Sector'] = '1121'

    # dual-purpose cattle ranching and farming: 11213
    df.loc[df['Activity'] == 'CATTLE, (EXCL COWS)', 'Sector'] = '112130A'
    df.loc[df['Activity'] == 'CATTLE, COWS', 'Sector'] = '112130B'
    df.loc[df['Activity'] == 'CATTLE, COWS, BEEF', 'Sector'] = '112130B1'
    df.loc[df['Activity'] == 'CATTLE, COWS, MILK', 'Sector'] = '112130B2'

    # hog and pig farming: 1122
    df.loc[df['Activity'] == 'HOGS', 'Sector'] = '1122'


    # poultry and egg production: 1123
    df.loc[df['Activity'] == 'POULTRY TOTALS', 'Sector'] = '1123'

    # chicken egg production: 11231
    df.loc[df['Activity'] == 'CHICKENS, LAYERS', 'Sector'] = '11231'

    # broilers and other meat-type chicken production: 11232
    df.loc[df['Activity'] == 'CHICKENS, BROILERS', 'Sector'] = '112320A'
    df.loc[df['Activity'] == 'CHICKENS, PULLETS, REPLACEMENT', 'Sector'] = '112320B'
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
    df.loc[df['Activity'] == 'PHEASANTS', 'Sector'] = '1123900J'
    df.loc[df['Activity'] == 'PIGEONS & SQUAB', 'Sector'] = '112390K'
    df.loc[df['Activity'] == 'POULTRY, OTHER', 'Sector'] = '112390L'
    df.loc[df['Activity'] == 'QUAIL', 'Sector'] = '112390M'
    df.loc[df['Activity'] == 'RHEAS', 'Sector'] = '112390N'


    # sheep and goat farming: 1124
    df.loc[df['Activity'] == 'SHEEP & GOATS TOTALS', 'Sector'] = '1124'

    # sheep farming: 11241
    df.loc[df['Activity'] == 'SHEEP, INCL LAMBS', 'Sector'] = '11241'

    # goat farming: 11242
    df.loc[df['Activity'] == 'GOATS', 'Sector'] = '11242'

    # animal aquaculture: 1125
    df.loc[df['Activity'] == 'AQUACULTURE TOTALS', 'Sector'] = '1125'
    # # part of Finfish farming
    # df.loc[df['Activity'] == 'FOOD FISH, CATFISH', 'Sector'] = '112511A'
    # df.loc[df['Activity'] == 'FOOD FISH, TROUT', 'Sector'] = '112511B'
    # df.loc[df['Activity'] == 'FOOD FISH, (EXCL CATFISH & TROUT)', 'Sector'] = '112511C'
    # df.loc[df['Activity'] == 'BAITFISH', 'Sector'] = '112511D'
    # df.loc[df['Activity'] == 'ORNAMENTAL FISH', 'Sector'] = '112511E'
    # df.loc[df['Activity'] == 'SPORT FISH', 'Sector'] = '112511F'
    # # part of Shellfish farming
    # df.loc[df['Activity'] == 'CRUSTACEANS', 'Sector'] = '112512A'
    # df.loc[df['Activity'] == 'MOLLUSKS', 'Sector'] = '112512B'
    # df.loc[df['Activity'] == 'AQUACULTURE, OTHER', 'Sector'] = '112519'

    # other animal production: 1129

    # apiculture: 11291
    df.loc[df['Activity'] == 'HONEY', 'Sector'] = '112910A'
    df.loc[df['Activity'] == 'HONEY, BEE COLONIES', 'Sector'] = '112910B'

    # horse and other equine production: 11292
    df.loc[df['Activity'] == 'EQUINE, (HORSES & PONIES) & (MULES & BURROS & DONKEYS)', 'Sector'] = '11292'
    df.loc[df['Activity'] == 'EQUINE, HORSES & PONIES', 'Sector'] = '112920A'
    df.loc[df['Activity'] == 'EQUINE, MULES & BURROS & DONKEYS', 'Sector'] = '112920B'

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
    years = ['2012', '2017']
    # flowclass
    flowclass = ['Other']
    # datasource
    datasource = 'USDA_CoA_Livestock'
    # df of unique ers activity names
    df = unique_activity_names(flowclass, years, datasource)
    # add manual naics 2012 assignments
    df = assign_naics(df)
    # drop any rows where naics12 is 'nan' (because level of detail not needed or to prevent double counting)
    df.dropna(subset=["Sector"], inplace=True)
    # assign sector type
    df['SectorType'] = None
    # sort df
    df = order_crosswalk(df)
    # save as csv
    df.to_csv(datapath + "activitytosectormapping/" + "Crosswalk_" + datasource + "_toNAICS.csv", index=False)
