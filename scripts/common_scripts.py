# common_scripts.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""Common variables and functions used within the 'scripts' folder"""

import flowsa
import pandas as pd


def unique_activity_names(datasource, year):
    """read in the ers parquet files, select the unique activity names, return df with one column """
    # create single df representing all selected years
    df = flowsa.getFlowByActivity(datasource, year)

    column_activities = df[["ActivityConsumedBy", "ActivityProducedBy"]].values.ravel()
    unique_activities = pd.unique(column_activities)
    df_unique = unique_activities.reshape((-1, 1))
    df_unique = pd.DataFrame({'Activity': df_unique[:, 0]})
    df_unique = df_unique.loc[df_unique['Activity'].notnull()]
    df_unique = df_unique.loc[df_unique['Activity'] != 'None']
    df_unique = df_unique.assign(ActivitySourceName=datasource)

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
    df = df[['ActivitySourceName', 'Activity', 'SectorSourceName', 'Sector', 'SectorType']]
    # sort df
    df = df.sort_values(['Sector', 'Activity']).reset_index(drop=True)

    return df