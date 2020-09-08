# write_Crosswalk_Blackhurst.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk linking BEA to NAICS

"""
import pandas as pd
from flowsa.common import datapath, fbaoutputpath, load_bea_crosswalk

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


def assign_naics(df):

    cw_load = load_bea_crosswalk()
    cw = cw_load[['BEA_2012_Detail_Code', 'NAICS_2012_Code']].drop_duplicates().reset_index(drop=True)
    # least aggregate level that applies is 5 digits
    cw = cw[cw['NAICS_2012_Code'].apply(lambda x: len(str(x)) == 5)].reset_index(drop=True)

    cw = cw.sort_values(['BEA_2012_Detail_Code', 'NAICS_2012_Code'])

    df = pd.merge(df, cw, left_on='Activity', right_on='BEA_2012_Detail_Code')
    df = df.drop(columns=["BEA_2012_Detail_Code"])
    df = df.rename(columns={"NAICS_2012_Code": "Sector"})
    df['SectorSourceName'] = 'NAICS_2012_Code'

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2002']
    # df of unique ers activity names
    df = unique_activity_names('Blackhurst_IO', years)
    # add manual naics 2012 assignments
    df = assign_naics(df)
    # drop any rows where naics12 is 'nan' (because level of detail not needed or to prevent double counting)
    df.dropna(subset=["Sector"], inplace=True)
    # assign sector type
    df['SectorType'] = None
    # sort df
    df = df.sort_values('Sector')
    # reset index
    df.reset_index(drop=True, inplace=True)
    # set order
    df = df[['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector', 'SectorType']]
    # save as csv
    df.to_csv(datapath + "activitytosectormapping/" + "Crosswalk_Blackhurst_IO_toNAICS.csv", index=False)
