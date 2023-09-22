# common_scripts.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""Common variables and functions used within the 'scripts' folder"""


import pandas as pd
import flowsa
import flowsa.flowbyactivity


def unique_activity_names(datasource, year, **_):
    """
    read in the ers parquet files, select the unique activity names, return
    df with one column
    :param datasource: str, FBA datasource
    :param year: str, year of data to lead
    :parm **_: optional parameter of "match_cols", defining a list of
    additional column names by which to return unique activity names. See
    "write_crosswalk_EPA_FactsAndFigures.py" for example
    :return: df, with Activity column of unique activity names
    """

    # create single df representing all selected years
    df = flowsa.flowbyactivity.getFlowByActivity(datasource, year)

    # return additional columns used to return unique activity names,
    # if specified
    match_cols = _.get("match_cols")
    if match_cols is None:
        match_cols = []

    # define columns used to subset df
    subset_cols = match_cols + ["ActivityConsumedBy", "ActivityProducedBy"]

    df_subset = df[subset_cols].drop_duplicates()
    df_unique = df_subset.melt(id_vars=match_cols, var_name="Cols",
                               value_name="Activity").drop(
        columns="Cols").drop_duplicates()

    df_unique = df_unique.loc[df_unique['Activity'].notnull()]
    df_unique = df_unique.loc[df_unique['Activity'] != 'None']
    df_unique = df_unique.assign(ActivitySourceName=datasource)

    # sort df
    df_unique = df_unique.sort_values(['Activity']).reset_index(drop=True)

    return df_unique


def order_crosswalk(df, **_):
    """
    Set column order and sort crosswalks
    :param df: crosswalk
    :param **_: optional parameter of "match_cols", defining a list of
    additional column names by which to return unique activity names. See
    "write_crosswalk_EPA_FactsAndFigures.py" for example
    :return: df, ordered crosswalk
    """
    # return additional columns used to return unique activity names,
    # if specified
    match_cols = _.get("match_cols")
    if match_cols is None:
        match_cols = []
    # set column order
    col_order = ['ActivitySourceName'] + match_cols + \
                ['Activity', 'SectorSourceName', 'Sector', 'SectorType']
    df = df[col_order]
    # sort df
    df = df.sort_values(['Sector', 'Activity']).reset_index(drop=True)

    return df
