# write_Crosswalk_EIA_CBECS_Land.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk linking the downloaded EIA_CBECS_Land to NAICS_12. Created by selecting unique Activity Names and
manually assigning to NAICS

"""

from flowsa.common import datapath
from scripts.common_scripts import unique_activity_names, order_crosswalk

def assign_naics(df):
    """manually assign each ERS activity to a NAICS_2012 code"""

    # assign sector source name
    df['SectorSourceName'] = 'NAICS_2012_Code'

    df.loc[df['Activity'] == 'All buildings', 'Sector'] = ''
    df.loc[df['Activity'] == 'Education', 'Sector'] = ''
    df.loc[df['Activity'] == 'Enclosed and strip malls', 'Sector'] = ''
    df.loc[df['Activity'] == 'Food sales', 'Sector'] = ''
    df.loc[df['Activity'] == 'Food service', 'Sector'] = ''
    df.loc[df['Activity'] == 'Health care', 'Sector'] = ''
    df.loc[df['Activity'] == 'Health care In-Patient', 'Sector'] = ''
    df.loc[df['Activity'] == 'Health care Out-Patient', 'Sector'] = ''
    df.loc[df['Activity'] == 'Inpatient', 'Sector'] = ''
    df.loc[df['Activity'] == 'Lodging', 'Sector'] = ''
    df.loc[df['Activity'] == 'Mercantile', 'Sector'] = ''
    df.loc[df['Activity'] == 'Office', 'Sector'] = ''
    df.loc[df['Activity'] == 'Other', 'Sector'] = ''
    df.loc[df['Activity'] == 'Outpatient', 'Sector'] = ''
    df.loc[df['Activity'] == 'Public assembly', 'Sector'] = ''
    df.loc[df['Activity'] == 'Public order and safety', 'Sector'] = ''
    df.loc[df['Activity'] == 'Religious worship', 'Sector'] = ''
    df.loc[df['Activity'] == 'Retail (other than mall)', 'Sector'] = ''
    df.loc[df['Activity'] == 'Service', 'Sector'] = ''
    df.loc[df['Activity'] == 'Vacant', 'Sector'] = ''
    df.loc[df['Activity'] == 'Warehouse and storage', 'Sector'] = ''

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2012']
    # assign flowclass
    flowcass = ['Land']
    # assign datasource
    datasource = 'EIA_CBECS_Land'
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
