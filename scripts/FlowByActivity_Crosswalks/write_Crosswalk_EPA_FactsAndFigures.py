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

    # assign sectors to materials
    # todo: will have to modify code when add additional data beyond landfill
    df.loc[df['FlowName'] == 'Food', 'Sector'] = '5622121F'
    df.loc[df['FlowName'] == 'Glass', 'Sector'] = ''
    df.loc[df['FlowName'] == 'Metals, Aluminum', 'Sector'] = ''
    df.loc[df['FlowName'] == 'Metals, Ferrous', 'Sector'] = ''
    df.loc[df['FlowName'] == 'Metals, Other Nonferrous', 'Sector'] = ''
    df.loc[df['FlowName'] == 'Miscellaneous Inorganic Wastes', 'Sector'] = ''
    df.loc[df['FlowName'] == 'Other', 'Sector'] = ''
    df.loc[df['FlowName'] == 'Paper and Paperboard', 'Sector'] = ''
    df.loc[df['FlowName'] == 'Plastics', 'Sector'] = ''
    df.loc[df['FlowName'] == 'Rubber and Leather', 'Sector'] = ''
    df.loc[df['FlowName'] == 'Textiles', 'Sector'] = ''
    df.loc[df['FlowName'] == 'Wood', 'Sector'] = ''
    df.loc[df['FlowName'] == 'Yard Trimmings', 'Sector'] = ''

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2018']
    # datasource
    datasource = 'EPA_FactsAndFigures'
    match_cols = ["FlowName"]
    # df of unique ers activity names
    df_list = []
    for y in years:
        dfy = unique_activity_names(datasource, y, match_cols=match_cols)
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
    df = order_crosswalk(df, match_cols=match_cols)
    # save as csv
    df.to_csv(f'{datapath}activitytosectormapping/NAICS_Crosswalk_'
              f'{datasource}.csv', index=False)
