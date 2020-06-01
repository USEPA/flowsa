# write_Crosswalk_Census_CBP.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""
Create a crosswalk for Census CBP to NAICS 2012. Downloaded data is already provided in NAICS
"""
import pandas as pd
from flowsa.common import datapath, outputpath

def unique_activity_names(datasource, years):
    """read in the ers parquet files, select the unique activity names"""
    df = []
    for y in years:
        df = pd.read_parquet(outputpath + datasource + "_" + str(y) + ".parquet", engine="pyarrow")
        df.append(df)
    df = df[['SourceName', 'ActivityProducedBy']]
    # rename columns
    df = df.rename(columns={"SourceName": "ActivitySourceName",
                            "ActivityProducedBy": "Activity"})
    df = df.drop_duplicates()
    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2012']
    # df of unique ers activity names
    df = unique_activity_names('Census_CBP', years)
    # Activity and Sector are the same
    df['Sector'] = df['Activity'].copy()
    # Add additional columns
    df['SectorSourceName'] = "NAICS_Code_2012"
    df['SectorType'] = "I"
    # reorder
    df = df[['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector', 'SectorType']]
    # sort df
    df = df.sort_values(['Activity'])
    # reset index
    df.reset_index(drop=True, inplace=True)
    # save as csv
    df.to_csv(datapath + "activitytosectormapping/" + "Crosswalk_Census_CBP_toNAICS.csv", index=False)
