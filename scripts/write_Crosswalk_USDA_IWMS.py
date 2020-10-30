# write_UDSA_IWMS_crosswalk.py (scripts)
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

    # assigning iwms activity items to naics 12,
    df.loc[df['Activity'] == 'BEANS, DRY EDIBLE, INCL CHICKPEAS', 'Sector'] = '11113'

    df.loc[df['Activity'] == 'CORN, GRAIN', 'Sector'] = '111150A'
    df.loc[df['Activity'] == 'CORN, SILAGE', 'Sector'] = '111150B'

    df.loc[df['Activity'] == 'COTTON', 'Sector'] = '11192'

    # a number of naics are the generalized "crops, other", so manually add each row
    df.loc[df['Activity'] == 'CROPS, OTHER', 'Sector'] = '11191'  # tobacco farming
    df = df.append(pd.DataFrame([['USDA_IWMS', 'CROPS, OTHER', 'NAICS_2012_Code', '11193']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']),
                                ignore_index=True, sort=True)  # sugarcane farming
    df = df.append(pd.DataFrame([['USDA_IWMS', 'CROPS, OTHER', 'NAICS_2012_Code', '11112']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']),
                                ignore_index=True, sort=True)  # oilseed (except soybean) farming
    df = df.append(pd.DataFrame([['USDA_IWMS', 'CROPS, OTHER','NAICS_2012_Code',  '111991']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']),
                                ignore_index=True, sort=True)  # SUGARBEETS
    df = df.append(pd.DataFrame([['USDA_IWMS', 'CROPS, OTHER', 'NAICS_2012_Code', '111998']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']),
                                ignore_index=True, sort=True)

    df.loc[df['Activity'] == 'HAY & HAYLAGE, (EXCL ALFALFA)', 'Sector'] = '111940A'
    df.loc[df['Activity'] == 'HAY & HAYLAGE, ALFALFA', 'Sector'] = '111940B'

    df.loc[df['Activity'] == 'HORTICULTURE TOTALS', 'Sector'] = '1114'

    df.loc[df['Activity'] == 'PASTURELAND', 'Sector'] = '112'

    # aggregates to fruit and tree nut farming: 1113
    df.loc[df['Activity'] == 'ORCHARDS', 'Sector'] = '111331'
    df = df.append(pd.DataFrame([['USDA_IWMS', 'ORCHARDS', 'NAICS_2012_Code', '111332']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']
                                ), ignore_index=True, sort=True)
    df = df.append(pd.DataFrame([['USDA_IWMS', 'ORCHARDS', 'NAICS_2012_Code', '111333']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']
                                ), ignore_index=True, sort=True)

    df.loc[df['Activity'] == 'BERRY TOTALS', 'Sector'] = '111334'

    df = df.append(pd.DataFrame([['USDA_IWMS', 'ORCHARDS', 'NAICS_2012_Code', '111335']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']
                                ), ignore_index=True, sort=True)
    df = df.append(pd.DataFrame([['USDA_IWMS', 'ORCHARDS', 'NAICS_2012_Code', '111336']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']
                                ), ignore_index=True, sort=True)
    df = df.append(pd.DataFrame([['USDA_IWMS', 'ORCHARDS', 'NAICS_2012_Code', '111339']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']
                                ), ignore_index=True, sort=True)

    df.loc[df['Activity'] == 'PEANUTS', 'Sector'] = '111992'

    df.loc[df['Activity'] == 'RICE', 'Sector'] = '11116'

    # seven types of other small grains, so manually add 6 rows
    df.loc[df['Activity'] == 'SMALL GRAINS, OTHER', 'Sector'] = '111199A'  # BARLEY
    df = df.append(pd.DataFrame([['USDA_IWMS', 'SMALL GRAINS, OTHER', 'NAICS_2012_Code', '111199B']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']),
                                ignore_index=True, sort=True)  # BUCKWHEAT
    df = df.append(pd.DataFrame([['USDA_IWMS', 'SMALL GRAINS, OTHER', 'NAICS_2012_Code', '111199C']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']),
                                ignore_index=True, sort=True)    # MILLET, PROSO
    df = df.append(pd.DataFrame([['USDA_IWMS', 'SMALL GRAINS, OTHER', 'NAICS_2012_Code', '111199D']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']),
                                ignore_index=True, sort=True)    # OATS
    df = df.append(pd.DataFrame([['USDA_IWMS', 'SMALL GRAINS, OTHER', 'NAICS_2012_Code', '111199E']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']),
                                ignore_index=True, sort=True)   # RYE
    df = df.append(pd.DataFrame([['USDA_IWMS', 'SMALL GRAINS, OTHER', 'NAICS_2012_Code', '111199I']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']),
                                ignore_index=True, sort=True)    # TRITICALE
    df = df.append(pd.DataFrame([['USDA_IWMS', 'SMALL GRAINS, OTHER', 'NAICS_2012_Code', '111199J']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']),
                                ignore_index=True, sort=True)    # WILD RICE

    # three types of sorghum, so manually add two rows
    df.loc[df['Activity'] == 'SORGHUM, GRAIN', 'Sector'] = '111199F'  # grain
    df = df.append(pd.DataFrame([['USDA_IWMS', 'SORGHUM, GRAIN', 'NAICS_2012_Code', '111199G']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']),
                                ignore_index=True, sort=True)  # syrup
    df = df.append(pd.DataFrame([['USDA_IWMS', 'SORGHUM, GRAIN', 'NAICS_2012_Code', '111199H']],
                                columns=['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector']),
                                ignore_index=True, sort=True)  # silage

    df.loc[df['Activity'] == 'SOYBEANS', 'Sector'] = '11111'

    df.loc[df['Activity'] == 'VEGETABLE TOTALS', 'Sector'] = '1112'

    df.loc[df['Activity'] == 'WHEAT', 'Sector'] = '11114'

    # df.loc[df['Activity'] == 'LETTUCE', 'Sector'] = ''
    # df.loc[df['Activity'] == 'POTATOES', 'Sector'] = ''
    # df.loc[df['Activity'] == 'SWEET CORN', 'Sector'] = ''
    # df.loc[df['Activity'] == 'TOMATOES', 'Sector'] = ''

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2013', '2018']
    # flowclass
    flowclass = ['Water']
    # datasource
    datasource = 'USDA_IWMS'
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
