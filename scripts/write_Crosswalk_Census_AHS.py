# write_Crosswalk_Census_AHS.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk linking the downloaded Census_AHS to NAICS_12. Created by selecting unique Activity Names and
manually assigning to NAICS

"""

from flowsa.common import datapath
from scripts.common_scripts import unique_activity_names, order_crosswalk

def assign_naics(df):
    """manually assign each ERS activity to a NAICS_2012 code"""

    # assign sector source name
    df['SectorSourceName'] = 'NAICS_2012_Code'

    df.loc[df['Activity'] == 'Asphalt Competitive Leases', 'Sector'] = ''
    df.loc[df['Activity'] == 'Class III Reinstatement Leases, Public Domain', 'Sector'] = ''
    df.loc[df['Activity'] == 'Coal Licenses, Exploration Licenses', 'Sector'] = ''

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2011', '2013', '2015', '2017']
    # assign flowclass
    flowcass = ['Land']
    # datasource
    datasource = 'Census_AHS'
    # df of unique ers activity names
    df = unique_activity_names(flowcass, years, datasource)
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
