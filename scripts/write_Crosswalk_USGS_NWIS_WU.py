# write_Crosswalk_USGS_NWIS_WU.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""
Create a crosswalk linking the downloaded USGS_NWIS_WU to NAICS_12. Created by selecting unique Activity Names and
manually assigning to NAICS

"""
import pandas as pd
from flowsa.common import datapath
from scripts.common_scripts import unique_activity_names, order_crosswalk


def assign_naics(df):
    """manually assign each ERS activity to a NAICS_2012 code"""

    df.loc[df['Activity'] == 'Aquaculture', 'Sector'] = '1125'

    # df.loc[df['Activity'] == 'Commercial', 'Sector'] = ''

    df.loc[df['Activity'] == 'Domestic', 'Sector'] = 'F010'

    df.loc[df['Activity'] == 'Hydroelectric Power', 'Sector'] = '221111'

    df.loc[df['Activity'] == 'Industrial', 'Sector'] = '1133'
    df = df.append(pd.DataFrame([['Industrial', '23']], columns=['Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['Industrial', '31']], columns=['Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['Industrial', '32']], columns=['Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['Industrial', '33']], columns=['Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['Industrial', '48839']], columns=['Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['Industrial', '5111']], columns=['Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['Industrial', '51222']], columns=['Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['Industrial', '51223']], columns=['Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['Industrial', '54171']], columns=['Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['Industrial', '56291']], columns=['Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['Industrial', '81149']], columns=['Activity', 'Sector']), sort=True)

    df.loc[df['Activity'] == 'Irrigation', 'Sector'] = '111'
    df = df.append(pd.DataFrame([['Irrigation', '112']], columns=['Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['Irrigation', '71391']], columns=['Activity', 'Sector']), sort=True)

    df.loc[df['Activity'] == 'Irrigation Crop', 'Sector'] = '111'
    df = df.append(pd.DataFrame([['Irrigation Crop', '112']], columns=['Activity', 'Sector']), sort=True)

    df.loc[df['Activity'] == 'Irrigation Golf Courses', 'Sector'] = '71391'

    df.loc[df['Activity'] == 'Irrigation Total', 'Sector'] = '111'
    df = df.append(pd.DataFrame([['Irrigation Total', '71391']], columns=['Activity', 'Sector']), sort=True)

    df.loc[df['Activity'] == 'Livestock', 'Sector'] = '1121'
    df = df.append(pd.DataFrame([['Livestock', '1122']], columns=['Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['Livestock', '1123']], columns=['Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['Livestock', '1124']], columns=['Activity', 'Sector']), sort=True)
    df = df.append(pd.DataFrame([['Livestock', '1129']], columns=['Activity', 'Sector']), sort=True)

    df.loc[df['Activity'] == 'Mining', 'Sector'] = '21'
    df = df.append(pd.DataFrame([['Mining', '54136']], columns=['Activity', 'Sector']), sort=True)

    df.loc[df['Activity'] == 'Public', 'Sector'] = '221310'
    df.loc[df['Activity'] == 'Public Supply', 'Sector'] = '221310'

    df = df.append(pd.DataFrame([['Thermoelectric Power', '2211']], columns=['Activity', 'Sector']), sort=True)
    df.loc[df['Activity'] == 'Thermoelectric Power Closed-loop cooling', 'Sector'] = '221100A'
    df.loc[df['Activity'] == 'Thermoelectric Power Once-through cooling', 'Sector'] = '221100B'

    # df.loc[df['Activity'] == 'Total', 'Sector'] = ''
    # df.loc[df['Activity'] == 'Total Groundwater', 'Sector'] = ''
    # df.loc[df['Activity'] == 'Total Surface', 'Sector'] = ''

    # assign sector source name
    df['SectorSourceName'] = 'NAICS_2012_Code'

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2010', '2015']
    # flowclass
    flowclass = ['Water']
    # datasource
    datasource = 'USGS_NWIS_WU'
    # df of unique ers activity names
    df = unique_activity_names(flowclass, years, datasource)
    # add manual naics 2012 assignments
    df = assign_naics(df)
    # drop any rows where naics12 is 'nan' (because level of detail not needed or to prevent double counting)
    df.dropna(subset=["Sector"], inplace=True)
    # assign sector type
    df['SectorType'] = 'I'
    # assign sector type
    df['ActivitySourceName'] = 'USGS_NWIS_WU'
    # sort df
    df = order_crosswalk(df)
    # save as csv
    df.to_csv(datapath + "activitytosectormapping/" + "Crosswalk_" + datasource + "_toNAICS.csv", index=False)
