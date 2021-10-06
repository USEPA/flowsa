# NETL_EIA_PlantWater.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Importing EIA plant water withdrawal/consumption data from ElectricityLCI repo,
as eLCI conducted water withdrawal modifications
"""

import pandas as pd
import numpy as np


def elci_parse(**kwargs):
    """
    Combine, parse, and format the provided dataframes
    :param kwargs: potential arguments include:
                   dataframe_list: list of dataframes to concat and format
                   args: dictionary, used to run flowbyactivity.py ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """

    from electricitylci.globals import data_dir
    from flowsa.flowbyfunctions import aggregator

    # load the csv file
    DATA_FILE = "NETL-EIA_powerplants_water_withdraw_consume_data_2016.csv"
    df_load = pd.read_csv(f"{data_dir}/{DATA_FILE}", index_col=0, low_memory=False)

    # subset df
    df = df_load[['Year', 'Month', '860 Cooling Type 1', 'Generator Primary Technology',
                  'Water Consumption Intensity Rate (Gallons / MWh)',
                  'Water Consumption Intensity Adjusted (gal/MWh)',
                  'Water Withdrawal Intensity Rate (Gallons / MWh)',
                  'Water Withdrawal Intensity Adjusted (gal/MWh)',
                  'Total net generation (MWh)',
                  'Summer Capacity of Steam Turbines (MW)',
                  'Water Type', 'County', 'State_y'
                  ]]

    # melt df
    df2 = pd.melt(df, id_vars=['Year', 'Month', '860 Cooling Type 1',
                               'Generator Primary Technology', 'Water Type',
                               'County', 'State_y'
                               ], var_name='FlowName')
    # split flowname col into flowname and unit
    df2['Unit'] = df2['FlowName'].str.split('(').str[1]
    df2['Unit'] = df2['Unit'].apply(lambda x: x.replace(")", "").replace(" ", ""))
    df2['FlowName'] = df2['FlowName'].str.split('(').str[0]
    df2['FlowName'] = df2['FlowName'].str.strip()

    # Assign ACB and APB columns
    df2['ActivityConsumedBy'] = np.where(df2['FlowName'].isin(['Water Consumption Intensity Rate',
                                                               'Water Consumption Intensity Adjusted',
                                                               'Water Withdrawal Intensity Rate',
                                                               'Water Withdrawal Intensity Adjusted']),
                                         df2['Generator Primary Technology'],
                                         None)

    df2['ActivityProducedBy'] = np.where(df2['ActivityConsumedBy'].isnull(),
                                         df2['Generator Primary Technology'],
                                         None)

    return df2
