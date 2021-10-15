# NETL_EIA_PlantWater.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Importing EIA plant water withdrawal/consumption data from ElectricityLCI repo,
as eLCI conducted water withdrawal modifications
"""

import pandas as pd
import numpy as np
from flowsa.common import get_state_FIPS, get_county_FIPS, us_state_abbrev, externaldatapath
from flowsa.flowbyfunctions import assign_fips_location_system


def elci_parse(**kwargs):
    """
    Combine, parse, and format the provided dataframes
    :param kwargs: potential arguments include:
                   dataframe_list: list of dataframes to concat and format
                   args: dictionary, used to run flowbyactivity.py ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """

    from flowsa.flowbyfunctions import aggregator
    args = kwargs['args']

    # load the csv file
    DATA_FILE = f"NETL-EIA_powerplants_water_withdraw_consume_data_{args['year']}.csv"
    df_load = pd.read_csv(f"{externaldatapath}{DATA_FILE}", index_col=0, low_memory=False)

    # subset df
    df = df_load[['Year', 'Month', '860 Cooling Type 1', 'Generator Primary Technology',
                  'Water Consumption Intensity Adjusted (gal/MWh)',
                  'Water Withdrawal Intensity Adjusted (gal/MWh)',
                  'Total net generation (MWh)',
                  'Total water discharge (million gallons) calc',
                  'Water Source Name', 'Water Type', 'County', 'State_y'
                  ]].copy(deep=True)

    # multiply to get total water rather than rate so can sum to national level
    cols_to_multiply = ['Water Consumption Intensity Adjusted (gal/MWh)',
                        'Water Withdrawal Intensity Adjusted (gal/MWh)']
    for c in cols_to_multiply:
        df[c] = df[c] * df['Total net generation (MWh)']
        # strip 'intensity' and '/MWh' from col name
        df = df.rename(columns={c: c.replace('Intensity Adjusted ', '').replace('/MWh', '')})

    # aggregate to annual
    df = df.drop(columns=['Month'])
    df2 = df.groupby(['Year', '860 Cooling Type 1', 'Generator Primary Technology',
                      'Water Source Name', 'Water Type', 'County', 'State_y']
                     ).agg({'Water Consumption (gal)': 'sum',
                            'Water Withdrawal (gal)': 'sum',
                            'Total net generation (MWh)': 'sum',
                            'Total water discharge (million gallons) calc': 'sum'}
                           ).reset_index()
    # drop rows where no water withdrawal data
    df3 = df2[df2['Water Withdrawal (gal)'] != 0].reset_index(drop=True)

    # make column lower case
    df3['Water Source Name'] = df3['Water Source Name'].apply(lambda x: x.lower())

    ground = 'wells|well|ground|gw|aquifer'
    surface = 'river|lake|reservoir|ocean|canal|creek|pond|bay|neosho|stanton|gulf|' \
              'pool|waterway|trinity|harbor|mississippi|channel|laguna|sound|water way|' \
              'coastal water authority|folsom south'
    public_supply = 'municipal|muncpl|municipality|potw|city|muncipality|' \
                    'municiple|city|west kern|wheeler ridge|sayreville'
    reclaimed = 'water treatment|chemicals|nwtp'
    storm = 'storm water'

    # assign compartments
    df3['Compartment'] = ''
    df3['Compartment'] = np.where(df3["Water Type"] == 'Reclaimed', "reclaimed", df3['Compartment'])
    df3['Compartment'] = np.where((df3['Compartment'] == '') &
                                  (df3['Water Source Name'].str.contains(ground)),
                                  "ground", df3['Compartment'])
    df3['Compartment'] = np.where((df3['Compartment'] == '') &
                                  (df3['Water Source Name'].str.contains(surface)),
                                  "surface", df3['Compartment'])
    df3['Compartment'] = np.where((df3['Compartment'] == '') &
                                  (df3['Water Source Name'].str.contains(public_supply)),
                                  "public supply", df3['Compartment'])
    df3['Compartment'] = np.where((df3['Compartment'] == '') &
                                  (df3['Water Source Name'].str.contains(reclaimed)),
                                  "reclaimed", df3['Compartment'])
    df3['Compartment'] = np.where((df3['Compartment'] == '') &
                                  (df3['Water Source Name'].str.contains(storm)),
                                  "stormwater", df3['Compartment'])
    df3['Compartment'] = np.where(df3["Water Source Name"] == 'lake wells', "surface", df3['Compartment'])


    df['Water Type'] = df['Water Type'].fillna('Total')
    df['Water Source Name'] = df['Water Source Name'].fillna('Total')
    fips = get_county_FIPS()
    us_abb = pd.DataFrame(us_state_abbrev.items(), columns=['State', 'State_y'])
    fips = fips.merge(us_abb, left_on='State', right_on='State')
    df = df.merge(fips, left_on=['State_y', 'County'], right_on=['State_y', 'County'])

    # melt df
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
    df2['ActivityConsumedBy'] = np.where(df2['FlowName'].isin(['Water Consumption',
                                                               'Water Withdrawal']),
                                         df2['Generator Primary Technology'],
                                         None)

    df2['ActivityProducedBy'] = np.where(df2['ActivityConsumedBy'].isnull(),
                                         df2['Generator Primary Technology'],
                                         None)

    df2['Class'] = np.where(np.char.find("Water", df2['FlowName']), "Water", "Energy")
    df2['Source'] = args['source']
    df2['DataReliability'] = 1
    df2['DataCollection'] = 5
    df2['FlowType'] = "ELEMENTARY_FLOW"
    df2 = df2.rename(columns={"value": "FlowAmount", "FIPS": "Location"})
    df2 = df2.drop(columns=['Month', '860 Cooling Type 1', 'Generator Primary Technology', 'Water Type', 'County',
                      'State', 'State_y', 'Water Source Name'])
    df2 = assign_fips_location_system(df2, str(args["year"]))

    return df2
