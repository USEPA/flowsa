# write_Crosswalk_EPA_FactsAndFigures.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk assigning sectors to Facts and Figures Activiites

"""
import pandas as pd
import numpy as np
from flowsa.common import load_crosswalk
from flowsa.settings import datapath
from scripts.FlowByActivity_Crosswalks.common_scripts import \
    unique_activity_names, order_crosswalk


def assign_naics(df):
    """
    Function to assign NAICS codes to each dataframe activity
    :param df: df, a FlowByActivity subset that contains unique activity names
    :return: df with assigned Sector columns
    """
    # subset out recycling sectors
    dfr = df[df['ActivityCode'].str.startswith('RS')].reset_index(drop=True)
    # assign sectors
    dfr['Sector'] = np.where(dfr['Activity'].str.contains('Recycling'),
                            '5629201', np.nan)
    dfr.loc[dfr['Activity'].str.contains('Minimally processed'), 'Sector'] \
        = '311119'
    dfr.loc[dfr['Activity'].str.contains('Rendering'), 'Sector'] = \
        '324110'
    dfr.loc[dfr['Activity'].str.contains('Anaerobic|Biofuels'), 'Sector'] \
        = '5622191'
    dfr.loc[dfr['Activity'].str.contains('Compost'), 'Sector'] = '5622192'
    dfr.loc[dfr['Activity'].str.contains('Landscape'), 'Sector'] = '115112'
    dfr.loc[dfr['Activity'].str.contains('Community'), 'Sector'] = \
        '624210'

    # Special handling of post consumer waste
    dfr.loc[len(dfr)] = {'Activity': 'Estimate from Post-Consumer Waste',
                         'Sector': 'F01000'}

    dfr.loc[len(dfr)] = {'Activity': 'Exports of goods and services',
                         'Sector': 'F04000'}
    dfr.loc[len(dfr)] = {'Activity': 'Imports of goods and services',
                         'Sector': 'F05000'}

    # assign the remaining codes based on BEA crosswalk
    dfb = df[~df['ActivityCode'].str.startswith('RS')].reset_index(drop=True)

    # load bea crosswalk
    cw_load = load_crosswalk('NAICS_to_BEA_Crosswalk_2012')
    cw = (cw_load[['BEA_2012_Detail_Code', 'NAICS_2012_Code']]
          .drop_duplicates()
          .dropna()
          .reset_index(drop=True)
          .rename(columns={'BEA_2012_Detail_Code': 'ActivityCode',
                          'NAICS_2012_Code': 'Sector'})
          )
    # only keep naics6
    cw2 = cw[cw['Sector'].apply(lambda x: len(x) == 6)].reset_index(drop=True)
    # merge
    dfb2 = dfb.merge(cw2, how='left')

    # concat
    df2 = pd.concat([dfb2, dfr], ignore_index=True)
    df2['Sector'] = df2['Sector'].replace({'nan': np.nan})

    return df2


if __name__ == '__main__':
    # load primary factors df and subset cols
    datasource = 'EPA_REI'
    df_load = pd.read_csv('https://raw.githubusercontent.com/USEPA/HIO/main/data/REI_sourcedata/REI_primaryfactors.csv', dtype='str')
    df = df_load.iloc[:, 0:2]
    df = df.rename(columns={'Unnamed: 0': 'ActivityCode',
                            'Unnamed: 1': 'Activity'}).dropna()
    df['Activity'] = df['Activity'].str.strip()
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
    df['ActivitySourceName'] = datasource
    # assign sector source name
    df['SectorSourceName'] = 'NAICS_2012_Code'
    # sort df
    df = order_crosswalk(df)
    # save as csv
    df.to_csv(f'{datapath}/activitytosectormapping/NAICS_Crosswalk_'
              f'{datasource}.csv', index=False)
