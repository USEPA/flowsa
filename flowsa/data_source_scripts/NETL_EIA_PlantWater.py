# NETL_EIA_PlantWater.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Importing EIA plant water withdrawal/consumption data from ElectricityLCI repo,
as eLCI conducted water withdrawal modifications
"""

import pandas as pd
import numpy as np
from flowsa.common import get_state_FIPS, get_county_FIPS, us_state_abbrev
from flowsa.flowbyfunctions import assign_fips_location_system


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
    args = kwargs['args']
    # load the csv file
    DATA_FILE = "NETL-EIA_powerplants_water_withdraw_consume_data_2016.csv"
    df_load = pd.read_csv(f"{data_dir}/{DATA_FILE}", index_col=0, low_memory=False)

    compartment_list = []

    # subset df
    df = df_load[['Year', 'Month', '860 Cooling Type 1', 'Generator Primary Technology',
                  'Water Consumption Intensity Rate (Gallons / MWh)',
                  'Water Consumption Intensity Adjusted (gal/MWh)',
                  'Water Withdrawal Intensity Rate (Gallons / MWh)',
                  'Water Withdrawal Intensity Adjusted (gal/MWh)',
                  'Total net generation (MWh)', 'Water Source Name',
                  'Summer Capacity of Steam Turbines (MW)',
                  'Water Type', 'County', 'State_y'
                  ]].copy(deep=True)
  #  df['Water Type'] = df.loc[:, 'Water Type']
    df['Water Type'] = df['Water Type'].fillna('Total')
    df['Water Source Name'] = df['Water Source Name'].fillna('Total')
    fips = get_county_FIPS()
    us_abb = pd.DataFrame(us_state_abbrev.items(), columns=['State', 'State_y'])
    fips = fips.merge(us_abb, left_on='State', right_on='State')
    df = df.merge(fips, left_on=['State_y', 'County'], right_on=['State_y', 'County'])

    surface = ['river', 'lake', 'reservoir', 'ocean', 'canal', 'aquifer', 'creek', 'pond', 'bay', 'neosho', 'stanton'
               'gulf', 'pool', 'waterway', 'trinity', 'harbor', 'mississippi', 'channel', 'laguna']
    total = ['municipal', 'wells', 'total', 'well', 'ground', 'muncpl']


    for index, row in df.iterrows():
        compartment_value = ""
        if "lake wells" in df.iloc[index]['Water Source Name'].lower():
          compartment_value = "surface"
        elif "river water" in df.iloc[index]['Water Source Name'].lower():
            compartment_value = "total"
        elif "sewage" in df.iloc[index]['Water Source Name'].lower():
            compartment_value = "sewage"
        else:
            for sur in surface:
                if sur in df.iloc[index]['Water Source Name'].lower():
                    compartment_value = "surface"
            for tot in total:
                if tot in df.iloc[index]['Water Source Name'].lower():
                    compartment_value = "total"
        compartment_list.append(compartment_value)



    # melt df
    df['Compartment'] = compartment_list
    df2 = pd.melt(df, id_vars=['Year', 'Month', '860 Cooling Type 1',
                               'Generator Primary Technology', 'Water Type',
                               'County', 'State_y', 'Water Source Name', 'State', 'FIPS', 'Compartment'
                               ], var_name='FlowName')

    # split flowname col into flowname and unit
    df2['Unit'] = df2['FlowName'].str.split('(').str[1]
    df2['Unit'] = df2['Unit'].apply(lambda x: x.replace(")", "").replace(" ", ""))
    df2['FlowName'] = df2['FlowName'].str.split('(').str[0]
    df2['FlowName'] = df2['FlowName'].str.strip()
    df2['FlowName'] = df2['FlowName'] + " " + df2['Water Type']
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

    df2['Class'] = np.where(np.char.find("Water", df2['FlowName']), "Water", "Energy")
   # df2['Compartment'] = np.where(np.char.find("Withdrawal", df2['FlowName']), "Water", "Air")
    df2['Source'] = args['source']
    df2['DataReliability'] = 1
    df2['DataCollection'] = 5
    df2['FlowType'] = "ELEMENTARY_FLOW"
    df2 = df2.rename(columns={"value": "FlowAmount", "FIPS": "Location"})
    df2 = df2.drop(columns=['Month', '860 Cooling Type 1', 'Generator Primary Technology', 'Water Type', 'County',
                      'State', 'State_y', 'Water Source Name'])
    df2 = assign_fips_location_system(df2, str(args["year"]))
    return df2
