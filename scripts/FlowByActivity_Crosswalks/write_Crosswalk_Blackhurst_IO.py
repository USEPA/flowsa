# write_Crosswalk_Blackhurst.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk linking Blackhurst IO vectors to NAICS

"""
import pandas as pd
import numpy as np
from flowsa.common import load_crosswalk
from flowsa.settings import datapath, log
from scripts.common_scripts import unique_activity_names, order_crosswalk
from flowsa.validation import replace_naics_w_naics_from_another_year


def assign_naics(df_load):
    """
    Function to assign NAICS codes to each dataframe activity
    :param df: df, a FlowByActivity subset that contains unique activity names
    :return: df with assigned Sector columns
    """

    cw_load = load_crosswalk('BEA')
    cw = cw_load[['BEA_2012_Detail_Code',
                  'NAICS_2012_Code']].drop_duplicates().reset_index(drop=True)

    # do not want any duplicated rows, so first apply the 6 digit naics but
    # if there are duplicates, drop and replace with 5 digit naics
    cw6 = cw[cw['NAICS_2012_Code'].apply(
        lambda x: len(str(x)) == 6)].reset_index(drop=True)

    df = pd.merge(df_load, cw6, left_on='Activity',
                  right_on='BEA_2012_Detail_Code',
                  how='left')
    df = df.drop(columns=["BEA_2012_Detail_Code"])
    df = df.rename(columns={"NAICS_2012_Code": "Sector"})
    # fill missing sector codes with activity, as some of the activities are
    # naics-like
    df['Sector'] = df['Sector'].fillna(df['Activity'])

    for l in range(5, 1, -1):
        cw_x = 'cw_' + str(l)
        vars()[cw_x] = cw[cw['NAICS_2012_Code'].apply(
            lambda x: len(str(x)) == l)].reset_index(drop=True)
        # check for duplicate values
        dup = df[df.duplicated(['Activity'])][[
            'Activity']].drop_duplicates().reset_index(drop=True)
        dup_list = dup['Activity'].to_list()

        df['Sector'] = np.where(df['Activity'].isin(dup_list), np.nan,
                                df['Sector'])
        df = df.drop_duplicates().reset_index(drop=True)

        df = pd.merge(df, vars()[cw_x], left_on='Activity',
                      right_on='BEA_2012_Detail_Code',
                      how='left')
        df['Sector'] = df['Sector'].fillna(df['NAICS_2012_Code'])
        df = df.drop(columns=['BEA_2012_Detail_Code', 'NAICS_2012_Code'])

    df['SectorSourceName'] = 'NAICS_2012_Code'

    # reset sector value for sand, gravel, clay
    df['Sector'] = np.where(df['Activity'] == '212320', '21232', df['Sector'])
    df['Sector'] = np.where(df['Activity'] == '212390', '21239', df['Sector'])

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    year = '2002'
    # datasource
    datasource = 'Blackhurst_IO'
    # df of unique ers activity names
    df = unique_activity_names(datasource, year)
    # add manual naics 2012 assignments
    df = assign_naics(df)
    # determine if any duplicates
    log.info(f"There are duplicates: "
             f"{df.duplicated(subset=['Activity']).any()}")
    # drop any rows where naics12 is 'nan'
    # (because level of detail not needed or to prevent double counting)
    df.dropna(subset=["Sector"], inplace=True)
    # assign sector type
    df['SectorType'] = None
    # sort df
    df = order_crosswalk(df)
    # save as csv
    df.to_csv(datapath + "activitytosectormapping/" +
              "Crosswalk_" + datasource + "_toNAICS.csv", index=False)
