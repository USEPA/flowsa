# write_Crosswalk_USGS_WU_Coef.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""
Create a crosswalk linking the USDA Irrigation and Water Management Surveyto NAICS_12. Created by selecting unique
Activity Names and manually assigning to NAICS

NAICS8 are unofficial and are not used again after initial aggregation to NAICS6. NAICS8 are based
on NAICS definitions from the Census.

7/8 digit NAICS align with USDA ERS FIWS

"""
import pandas as pd
from flowsa.common import datapath
from scripts.common_scripts import unique_activity_names, order_crosswalk

def assign_naics(df):
    """manually assign each ERS activity to a NAICS_2012 code"""
    # assign sector source name
    df['SectorSourceName'] = 'NAICS_2012_Code'

    # cattle ranching and farming: 1121

    # beef cattle ranching and farming including feedlots: 11211
    df.loc[df['Activity'] == 'Beef and other cattle, including calves', 'Sector'] = '11211'

    # dairy cattle and milk production: 11212
    df.loc[df['Activity'] == 'Dairy cows', 'Sector'] = '11212'

    # hog and pig farming: 1122
    df.loc[df['Activity'] == 'Hogs and pigs', 'Sector'] = '1122'


    # poultry and egg production: 1123

    # chicken egg production: 11231
    df.loc[df['Activity'] == 'Laying hens', 'Sector'] = '11231'

    # broilers and other meat-type chicken production: 11232
    df.loc[df['Activity'] == 'Broilers and other chickens', 'Sector'] = '11232'

    # turkey production: 11233
    df.loc[df['Activity'] == 'Turkeys', 'Sector'] = '11233'

    # poultry hatcheries: 11234

    # other poultry production: 11239, manually add row
    df = df.append(pd.DataFrame([['USGS_WU_Coef', 'Broilers and other chickens', 'NAICS_2012_Code', '11239']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']),
                                ignore_index=True, sort=True)


    # sheep and goat farming: 1124

    # sheep farming: 11241
    df.loc[df['Activity'] == 'Sheep and lambs', 'Sector'] = '11241'

    # goat farming: 11242
    df.loc[df['Activity'] == 'Goats', 'Sector'] = '11242'

    # animal aquaculture: 1125

    # other animal production: 1129

    # apiculture: 11291

    # horse and other equine production: 11292
    df.loc[df['Activity'] == 'Horses (including ponies, mules, burrows, and donkeys)', 'Sector'] = '11292'

    # fur-bearing animal and rabbit production: 11293, manually add row
    df = df.append(pd.DataFrame([['USGS_WU_Coef', 'Broilers and other chickens', 'NAICS_2012_Code', '11293']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']),
                                ignore_index=True, sort=True)


    # all other animal production: 11299, manually add row
    df = df.append(pd.DataFrame([['USGS_WU_Coef', 'Sheep and lambs', 'NAICS_2012_Code', '11299']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']),
                                ignore_index=True, sort=True)

    return df


if __name__ == '__main__':
    # select unique activity names from file
    years = ['2005']
    # flowclass
    flowclass = ['Water']
    # datasource
    datasource = 'USGS_WU_Coef'
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
