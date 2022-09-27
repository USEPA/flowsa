# write_Crosswalk_EPA_FactsAndFigures.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk assigning sectors to Facts and Figures Activiites

"""
import pandas as pd
import numpy as np
from flowsa.settings import datapath
from scripts.FlowByActivity_Crosswalks.common_scripts import \
    unique_activity_names, order_crosswalk


def assign_naics(df):
    """
    Function to assign NAICS codes to each dataframe activity
    :param df: df, a FlowByActivity subset that contains unique activity names
    :return: df with assigned Sector columns
    """
    df = df[['Activity', 'ActivitySourceName']].reset_index(drop=True)
    # assign sector source name
    df['SectorSourceName'] = 'NAICS_2012_Code'

    # assign sectors
    df['Sector'] = np.where(df['Activity'].str.contains('Recycling'),
                            '5629201', np.nan)
    df.loc[df['Activity'].str.contains('Minimally processed'), 'Sector'] \
        = '311119'
    df.loc[df['Activity'].str.contains('Rendering'), 'Sector'] = \
        '562BIO'
    df.loc[df['Activity'].str.contains('Anaerobic|Biofuels'), 'Sector'] \
        = '5622191'
    df.loc[df['Activity'].str.contains('Compost'), 'Sector'] = '5622192'
    df.loc[df['Activity'].str.contains('Landscape'), 'Sector'] = '115112'
    df.loc[df['Activity'].str.contains('Community'), 'Sector'] = \
        '624210'

    df['Sector'] = df['Sector'].replace({'nan': np.nan})

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2012']
    # datasource
    datasource = 'EPA_REI'
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
    df = (df
          .dropna(subset=["Sector"])
          .reset_index(drop=True)
          )
    # assign sector type
    df['SectorType'] = None
    # sort df
    df = order_crosswalk(df)
    # save as csv
    df.to_csv(f'{datapath}activitytosectormapping/NAICS_Crosswalk_'
              f'{datasource}.csv', index=False)
