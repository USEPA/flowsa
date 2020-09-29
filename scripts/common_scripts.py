# common_scripts.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

"""Common variables and functions used across flowsa"""
import pandas as pd
from flowsa.common import fbaoutputpath


def unique_activity_names(datasource, years):
    """read in the ers parquet files, select the unique activity names, return df with one column """
    # create single df representing all selected years

    df_u = []
    for y in years:
        df_load = pd.read_parquet(fbaoutputpath + datasource + "_" + str(y) + ".parquet", engine="pyarrow")
        df_u.append(df_load)
    df = pd.concat(df_u, ignore_index=True)

    column_activities = df[["ActivityConsumedBy", "ActivityProducedBy"]].values.ravel()
    unique_activities = pd.unique(column_activities)
    df_unique = unique_activities.reshape((-1, 1))
    df_unique = pd.DataFrame({'Activity': df_unique[:, 0]})
    df_unique = df_unique.loc[df_unique['Activity'].notnull()]
    df_unique = df_unique.loc[df_unique['Activity'] != 'None']
    df_unique.loc[:, 'ActivitySourceName'] = df['SourceName'].unique()

    # sort df
    df_unique = df_unique.sort_values(['Activity']).reset_index(drop=True)

    return df_unique


def order_crosswalk(df):
    """
    Set column order and sort crosswalks
    :param df: crosswalk
    :return:
    """
    # set column order
    df = df[['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector', 'SectorType']].reset_index(drop=True)
    # sort df
    df = df.sort_values(['Sector', 'Activity'])

    return df