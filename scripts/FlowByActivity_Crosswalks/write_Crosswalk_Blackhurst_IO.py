# write_Crosswalk_Blackhurst.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk linking Blackhurst IO vectors to NAICS

source: https://pubmed.ncbi.nlm.nih.gov/20141104/

"""
import pandas as pd
from flowsa.common import load_crosswalk
from flowsa.settings import datapath
from scripts.FlowByActivity_Crosswalks.common_scripts import unique_activity_names, order_crosswalk


def assign_naics(df_load):
    """
    Function to assign NAICS codes to each dataframe activity
    :param df: df, a FlowByActivity subset that contains unique activity names
    :return: df with assigned Sector columns
    """

    cw_load = load_crosswalk('NAICS_to_BEA_Crosswalk_2012')
    cw = cw_load[['BEA_2012_Detail_Code',
                  'NAICS_2012_Code']].drop_duplicates().reset_index(drop=True)
    # least aggregate level that applies is 5 digits
    cw = cw[
        cw['NAICS_2012_Code'].apply(lambda x: len(str(x)) == 6)].reset_index(
        drop=True)

    cw = cw.sort_values(['BEA_2012_Detail_Code', 'NAICS_2012_Code'])

    df = pd.merge(df_load, cw,
                  left_on='Activity',
                  right_on='BEA_2012_Detail_Code',
                  how='left')
    df = df.drop(columns=["BEA_2012_Detail_Code"])
    df = df.rename(columns={"NAICS_2012_Code": "Sector"})

    # reset sector value for sand, gravel, clay
    df.loc[df['Activity'] == '212320', 'Sector'] = '212321'
    df = pd.concat([df, pd.DataFrame(
        [['Blackhurst_IO','212320', '212322']], columns=[
            'ActivitySourceName', 'Activity', 'Sector'])],
                   ignore_index=True, sort=True)
    df = pd.concat([df, pd.DataFrame(
        [['Blackhurst_IO', '212320', '212324']],
        columns=['ActivitySourceName', 'Activity', 'Sector']
    )], ignore_index=True, sort=True)
    df = pd.concat([df, pd.DataFrame(
        [['Blackhurst_IO', '212320', '212325']],
        columns=['ActivitySourceName', 'Activity', 'Sector']
    )], ignore_index=True, sort=True)

    df.loc[df['Activity'] == '212390', 'Sector'] = '212391'
    df = pd.concat([df, pd.DataFrame(
        [['Blackhurst_IO', '212390', '212392']],
        columns=['ActivitySourceName', 'Activity', 'Sector'])],
                   ignore_index=True, sort=True)
    df = pd.concat([df, pd.DataFrame(
        [['Blackhurst_IO', '212390', '212393']],
        columns=['ActivitySourceName', 'Activity', 'Sector'])],
                   ignore_index=True, sort=True)
    df = pd.concat([df, pd.DataFrame(
        [['Blackhurst_IO', '212390', '212399']],
        columns=['ActivitySourceName', 'Activity', 'Sector'])],
                   ignore_index=True, sort=True)

    # drop two rows where Blackhurst's IO vectors do not align with the
    # NAICS to BEA mapping because a NAICS code is it's own activity rather
    # than a subset of an activity
    # 'iron ore mining' is it's own row
    df = df[~((df['Activity'] == '2122A0') & (df['Sector'] == '212210'))]
    # 'support activities for oil and gas operations" is its own row
    df = df[~((df['Activity'] == '21311A') & (df['Sector'] == '213112'))]

    # ensure the sector column has value for iron ore and support activities
    df.loc[df['Activity'] == '212210', 'Sector'] = '212210'
    df.loc[df['Activity'] == '213112', 'Sector'] = '213112'

    df['SectorSourceName'] = 'NAICS_2012_Code'

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
    # drop any rows where naics12 is 'nan'
    # (because level of detail not needed or to prevent double counting)
    df.dropna(subset=["Sector"], inplace=True)
    # assign sector type
    df['SectorType'] = None
    # subset to just the sectors used in water allocation m2. Must reexamine
    # crosswalk for additional BEA activities to ensure no data loss and
    # accurate mapping
    sector_list = ['21', '54136']
    df2 = df.loc[df['Sector'].str.startswith(
        tuple(sector_list))].reset_index(drop=True)
    # sort df
    df2 = order_crosswalk(df2)
    # save as csv
    df.to_csv(f"{datapath}/activitytosectormapping/NAICS_Crosswalk_"
              f"{datasource}.csv", index=False)
