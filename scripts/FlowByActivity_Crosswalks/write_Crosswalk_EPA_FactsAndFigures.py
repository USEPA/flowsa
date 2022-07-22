# write_Crosswalk_EPA_FactsAndFigures.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk assigning sectors to Facts and Figures Activiites

"""
import pandas as pd
from flowsa.settings import datapath
from scripts.FlowByActivity_Crosswalks.common_scripts import \
    unique_activity_names, order_crosswalk


def assign_naics(df):
    """
    Function to assign NAICS codes to each dataframe activity
    :param df: df, a FlowByActivity subset that contains unique activity names
    :return: df with assigned Sector columns
    """

    # assign sector source name
    df['SectorSourceName'] = 'NAICS_2012_Code'

    # assign sectors to activities
    # 562212 is the code for landfills, in flowsa, the 7-digit sector code
    # '5622121' represents MSW landfills, while '5622122' is industrial
    # waste landfills
    df.loc[df['Activity'] == 'Landfill', 'Sector'] = '5622121'

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2018']
    # datasource
    datasource = 'EPA_FactsAndFigures'
    # df of unique ers activity names
    df_list = []
    for y in years:
        dfy = unique_activity_names(datasource, y)
        df_list.append(dfy)
    df = pd.concat(df_list, ignore_index=True).drop_duplicates()
    # add manual naics 2012 assignments
    df = assign_naics(df)
    # drop any rows where naics12 is 'nan'
    # (because level of detail not needed or to prevent double counting)
    df.dropna(subset=["Sector"])
    # assign sector type
    df['SectorType'] = None
    # sort df
    df = order_crosswalk(df)
    # save as csv
    df.to_csv(f'{datapath}activitytosectormapping/NAICS_Crosswalk_'
              f'{datasource}.csv', index=False)
