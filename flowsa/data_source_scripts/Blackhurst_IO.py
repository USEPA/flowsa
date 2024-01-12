# Blackhurst_IO.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Scrapes data from Blackhurst paper 'Direct and Indirect Water Withdrawals
for US Industrial Sectors' (Supplemental info)
Includes supporting functions for Blackhurst paper data.
"""

from tabula.io import read_pdf
import pandas as pd
from flowsa.location import US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.settings import externaldatapath


def bh_parse(*, df_list, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """

    # load pdf from externaldatapath directory
    pages = range(5, 13)
    bh_df_list = []
    for x in pages:
        bh_df = read_pdf(externaldatapath / 'Blackhurst_WatWithdrawalsforUSIndustrialSectorsSI.pdf',
                         pages=x, stream=True)[0]
        bh_df_list.append(bh_df)

    df = pd.concat(bh_df_list, sort=False)
    df = df.rename(columns={"I-O code": "ActivityConsumedBy",
                            "I-O description": "Description",
                            "gal/$M": "FlowAmount",
                            })
    # hardcode
    # original data in gal/million usd
    df.loc[:, 'FlowAmount'] = df['FlowAmount'] / 1000000
    df['Unit'] = 'gal/USD'
    df['SourceName'] = 'Blackhurst_IO'
    df['Class'] = 'Water'
    df['FlowName'] = 'Water Withdrawals IO Vector'
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, '2002')
    df['Year'] = '2002'
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df
