# write_Crosswalk_BLS_QCEW.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""
Create a crosswalk for BLS QCEW to NAICS 2012. Downloaded data is already provided in NAICS
"""
import pandas as pd
import numpy as np
from flowsa.common import datapath, fbaoutputpath
from flowsa.common import load_sector_length_crosswalk

def unique_activity_names(datasource, years):
    """read in the ers parquet files, select the unique activity names"""
    df = []
    for y in years:
        df = pd.read_parquet(fbaoutputpath + datasource + "_" + str(y) + ".parquet", engine="pyarrow")
        df.append(df)
    df = df[['SourceName', 'ActivityProducedBy']]
    # rename columns
    df = df.rename(columns={"SourceName": "ActivitySourceName",
                            "ActivityProducedBy": "Activity"})
    df = df.drop_duplicates()
    return df

def link_non_bls_naics_to_naics(df):
    """
    BLS has 6 digit naics that are not recognized by Census. Map bls data to naics
    :param df:
    :return:
    """

    # load sector crosswalk
    cw = load_sector_length_crosswalk()
    cw_sub = cw[['NAICS_6']].drop_duplicates()

    # subset the df to the 6 digit naics
    df_sub = df[df['Activity'].apply(lambda x: len(x) == 6)]

    # create a list of 6 digit bls activities that are not naics
    unmapped_df = pd.merge(df_sub, cw_sub, indicator=True, how='outer', left_on='Activity',
                           right_on='NAICS_6').query('_merge=="left_only"').drop('_merge', axis=1).reset_index(drop=True)
    act_list = unmapped_df['Activity'].values.tolist()

    # if in the activity list, the sector should be modified so last digit is 0

    df['Sector'] = np.where(df['Activity'].isin(act_list), df['Activity'].apply(lambda x: x[0:5]) + '0', df['Sector'])

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2012']
    # df of unique ers activity names
    df = unique_activity_names('BLS_QCEW', years)
    # Activity and Sector are the same
    df['Sector'] = df['Activity'].copy()
    # modify the sector for activity = '31-33'
    df.loc[df['Activity'] == '31-33', 'Sector'] = '31'
    df = df.append(pd.DataFrame([['BLS_QCEW', '31-33', '32']], columns=['ActivitySourceName', 'Activity', 'Sector']))
    df = df.append(pd.DataFrame([['BLS_QCEW', '31-33', '33']], columns=['ActivitySourceName', 'Activity', 'Sector']))
    df = link_non_bls_naics_to_naics(df)
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
    df.to_csv(datapath + "activitytosectormapping/" + "Crosswalk_BLS_QCEW_toNAICS.csv", index=False)
