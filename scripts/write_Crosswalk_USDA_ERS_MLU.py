# write_Crosswalk_UDSA_ERS_MLU.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk linking the downloaded USDA_ERS_MLU to NAICS_12. Created by selecting unique Activity Names and
manually assigning to NAICS

"""

from flowsa.common import datapath
from scripts.common_scripts import unique_activity_names, order_crosswalk

def assign_naics(df):
    """manually assign each ERS activity to a NAICS_2012 code"""

    # assign sector source name
    df['SectorSourceName'] = 'NAICS_2012_Code'

    df.loc[df['Activity'] == 'All special uses of land', 'Sector'] = ''
    # df.loc[df['Activity'] == 'Cropland idled', 'Sector'] = ''
    df.loc[df['Activity'] == 'Cropland used for crops', 'Sector'] = '111'
    df.loc[df['Activity'] == 'Cropland used for pasture', 'Sector'] = '112'
    df.loc[df['Activity'] == 'Farmsteads, roads, and miscellaneous farmland', 'Sector'] = ''
    df.loc[df['Activity'] == 'Forest-use land (all)', 'Sector'] = ''
    df.loc[df['Activity'] == 'Forest-use land grazed', 'Sector'] = '112'
    df.loc[df['Activity'] == 'Forest-use land not grazed', 'Sector'] = ''
    df.loc[df['Activity'] == 'Grassland pasture and range', 'Sector'] = '112'
    df.loc[df['Activity'] == 'Land in defense and industrial areas', 'Sector'] = ''
    df.loc[df['Activity'] == 'Land in rural parks and wildlife areas', 'Sector'] = ''
    df.loc[df['Activity'] == 'Land in rural transportation facilities', 'Sector'] = ''
    df.loc[df['Activity'] == 'Land in urban areas', 'Sector'] = ''
    df.loc[df['Activity'] == 'Other land', 'Sector'] = ''
    df.loc[df['Activity'] == 'Total cropland', 'Sector'] = '11'
    # df.loc[df['Activity'] == 'Total land', 'Sector'] = ''

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2007', '2012']
    # class
    flowclass = ['Land']
    # datasource
    datasource = 'USDA_ERS_MLU'
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
