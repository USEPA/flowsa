# dataclean.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Common functions to clean and harmonize dataframes
"""

import pandas as pd
import numpy as np
from flowsa import (literature_values, settings, flowsa_log)


def clean_df(df, flowbyfields, drop_description=True):
    """
    Modify a dataframe to ensure all columns are present and column
    datatypes correct
    :param df: df, any format
    :param flowbyfields: list, flow_by_activity_fields or flow_by_sector_fields
    :param fill_na_dict: dict, fba_fill_na_dict or fbs_fill_na_dict
    :param drop_description: specify if want the Description column
         dropped, defaults to true
    :return: df, modified
    """
    df = df.reset_index(drop=True)
    # ensure correct data types
    df = add_missing_flow_by_fields(df, flowbyfields)
    # drop description field, if exists
    if 'Description' in df.columns and drop_description is True:
        df = df.drop(columns='Description')
    if flowbyfields == 'flow_by_sector_fields':
        # harmonize units across dfs
        df = standardize_units(df)

    return df


def add_missing_flow_by_fields(flowby_partial_df, flowbyfields):
    """
    Add in missing fields to have a complete and ordered df
    :param flowby_partial_df: Either flowbyactivity or flowbysector df
    :param flowbyfields: Either flow_by_activity_fields, flow_by_sector_fields,
           or flow_by_sector_collapsed_fields
    :return: df, with all required columns
    """
    # add required columns identified in schema.py
    for col, param in flowbyfields.items():
        for required, response in param[1].items():
            if response and col not in flowby_partial_df.columns:
                flowby_partial_df[col] = np.nan
    # convert all None, 'nan' to np.nan
    flowby_partial_df = (flowby_partial_df
                         .replace({'None': np.nan, 'nan': np.nan})
                         .infer_objects(copy=False)
                         )
    # convert data types to match those defined in flow_by_activity_fields
    for k, v in flowbyfields.items():
        if k in flowby_partial_df.columns:
            flowby_partial_df[k] = \
                flowby_partial_df[k].astype(v[0]['dtype'])
            if v[0]['dtype'] in ['string', 'str', 'object']:
                flowby_partial_df[k] = flowby_partial_df[k].fillna(np.nan).infer_objects(copy=False)
            else:
                flowby_partial_df[k] = flowby_partial_df[k].fillna(0).infer_objects(copy=False)
    # convert all None, 'nan' to np.nan
    with pd.option_context('future.no_silent_downcasting', True):
        flowby_partial_df = (flowby_partial_df
                             .replace({'None': np.nan, 'nan': np.nan})
                             .infer_objects(copy=False)
                             )
    # Resort it so order is correct
    cols = [e for e in flowbyfields.keys() if e in flowby_partial_df.columns]
    flowby_df = flowby_partial_df[cols]
    return flowby_df


def standardize_units(df):
    """
    Convert unit to standard using csv
    Timeframe is over one year
    This function is copied from the flowby.py fxn
    :param df: df, Either flowbyactivity or flowbysector
    :return: df, with standarized units
    """

    year = df['Year'][0]

    exchange_rate = (
        literature_values
        .get_Canadian_to_USD_exchange_rate(year)
    )

    conversion_table = pd.concat([
        pd.read_csv(settings.datapath / 'unit_conversion.csv'),
        pd.Series({'old_unit': 'Canadian Dollar',
                   'new_unit': 'USD',
                   'conversion_factor': 1 / exchange_rate}).to_frame().T
    ])

    standardized = (
        df
        .assign(Unit=df.Unit.str.strip())
        .merge(conversion_table, how='left',
               left_on='Unit', right_on='old_unit')
        .assign(Unit=lambda x: x.new_unit.mask(x.new_unit.isna(), x.Unit),
                conversion_factor=lambda x: x.conversion_factor.fillna(1).infer_objects(copy=False),
                FlowAmount=lambda x: x.FlowAmount * x.conversion_factor)
        .drop(columns=['old_unit', 'new_unit', 'conversion_factor'])
    )

    standardized_units = list(conversion_table.new_unit.unique())

    if any(~standardized.Unit.isin(standardized_units)):
        unstandardized_units = [unit for unit in standardized.Unit.unique()
                                if unit not in standardized_units]
        flowsa_log.log.warning(f'Some units not standardized by '
                               f'standardize_units(): {unstandardized_units}.')

    return standardized

