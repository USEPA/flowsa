# write_Crosswalk_StatCan_IWS_MI.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""
Create a crosswalk linking Statistics Canada to NAICS

"""
import pandas as pd
from flowsa.common import datapath, fbaoutputpath

def unique_activity_names(datasource, years):
    """read in the ers parquet files, select the unique activity names"""
    df = []
    for y in years:
        df = pd.read_parquet(fbaoutputpath + datasource + "_" + str(y) + ".parquet", engine="pyarrow")
        df.append(df)
    df = df[['SourceName', 'ActivityConsumedBy']]
    # rename columns
    df = df.rename(columns={"SourceName": "ActivitySourceName",
                            "ActivityConsumedBy": "Activity"})
    df = df.drop_duplicates()
    return df



if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2011', '2015']
    # df of unique ers activity names
    df = unique_activity_names('StatCan_IWS_MI', years)
    # add manual naics 2012 assignments
    # Activity and Sector are the same
    df['Sector'] = df['Activity'].copy()
    # modify the sector for activity = '31-33'
    df.loc[df['Activity'] == '31-33', 'Sector'] = '31'
    df = df.append(pd.DataFrame([['StatCan_IWS_MI', '31-33', '32']], columns=['ActivitySourceName', 'Activity', 'Sector']))
    df = df.append(pd.DataFrame([['StatCan_IWS_MI', '31-33', '33']], columns=['ActivitySourceName', 'Activity', 'Sector']))
    # drop 'Other' and nan
    df = df[~df['Activity'].isin(['Other', 'nan'])]
    # Add additional columns
    df['SectorSourceName'] = "NAICS_2012_Code"
    df['SectorType'] = "I"
    # reorder
    df = df[['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector', 'SectorType']]
    # sort df
    df = df.sort_values(['Activity', 'Sector'])
    # reset index
    df.reset_index(drop=True, inplace=True)
    # save as csv
    df.to_csv(datapath + "activitytosectormapping/" + "Crosswalk_StatCan_IWS_MI_toNAICS.csv", index=False)
