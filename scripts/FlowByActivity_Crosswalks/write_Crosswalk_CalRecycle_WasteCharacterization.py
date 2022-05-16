# write_Crosswalk_CalRecycle_WasteCharacterization.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk for CalRecycle Waste Characterization to NAICS 2012.
"""
import pandas as pd
from flowsa.settings import datapath, externaldatapath
from scripts.FlowByActivity_Crosswalks.common_scripts import unique_activity_names, order_crosswalk
from flowsa.data_source_scripts.CalRecycle_WasteCharacterization import produced_by


def assign_naics(df):
    """
    Function to assign NAICS codes to each dataframe activity
    :param df: df, a FlowByActivity subset that contains unique activity names
    :return: df with assigned Sector columns
    """
    
    mapping = pd.read_csv(externaldatapath + "California_Commercial_bySector_2014_Mapping.csv",
                          dtype='str')
    mapping = mapping.melt(var_name='Activity',
                           value_name='Sector'
                           ).dropna().reset_index(drop=True)
    mapping['Sector'] = mapping['Sector'].astype(str)
    mapping['Activity'] = mapping['Activity'].map(lambda x: produced_by(x))
    df = df.merge(mapping, on='Activity')

    # append Multifamily Sector to PCE
    df = df.append({'Activity': 'Multifamily',
                    'ActivitySourceName': 'CalRecycle_WasteCharacterization',
                    'Sector': 'F010'},
                   ignore_index=True)
    
    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2014']
    # datasource
    datasource = 'CalRecycle_WasteCharacterization'
    # df of unique activity names
    df_list = []
    for y in years:
        dfy = unique_activity_names(datasource, y)
        df_list.append(dfy)
    df = pd.concat(df_list, ignore_index=True).drop_duplicates()

    df = assign_naics(df)
    # Add additional columns
    df['SectorSourceName'] = "NAICS_2012_Code"
    df['SectorType'] = 'I'
    # reorder
    df = order_crosswalk(df)
    # save as csv
    df.to_csv(datapath + "activitytosectormapping/" +
              "NAICS_Crosswalk_" + datasource + ".csv", index=False)
