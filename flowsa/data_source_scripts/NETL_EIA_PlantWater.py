# NETL_EIA_PlantWater.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Importing EIA plant water withdrawal/consumption data from ElectricityLCI repo,
as eLCI conducted water withdrawal modifications
"""

import pandas as pd


def elci_parse(**kwargs):
    """
    Combine, parse, and format the provided dataframes
    :param kwargs: potential arguments include:
                   dataframe_list: list of dataframes to concat and format
                   args: dictionary, used to run flowbyactivity.py ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """

    from electricitylci.globals import data_dir

    # load arguments necessary for function
    args = kwargs['args']

    # load the csv file
    DATA_FILE = "NETL-EIA_powerplants_water_withdraw_consume_data_2016.csv"
    df = pd.read_csv(f"{data_dir}/{DATA_FILE}", index_col=0, low_memory=False)


    return df
