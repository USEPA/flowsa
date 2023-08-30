# NETL_EIA_PlantWater.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Importing EIA plant water withdrawal/consumption data from ElectricityLCI repo,
as eLCI conducted water withdrawal modifications
"""

import pandas as pd
import numpy as np
from flowsa.location import get_county_FIPS, us_state_abbrev
from flowsa.settings import externaldatapath
from flowsa.flowbyfunctions import assign_fips_location_system


def netl_eia_parse(*, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param dataframe_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # load the csv file
    DATA_FILE = f"NETL-EIA_powerplants_water_withdraw_consume_data_" \
                f"{year}.csv"
    df_load = pd.read_csv(externaldatapath / DATA_FILE,
                          index_col=0, low_memory=False)

    # subset df
    df = df_load[['Year', 'Month', '860 Cooling Type 1',
                  'Generator Primary Technology',
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
        df = df.rename(columns={
            c: c.replace('Intensity Adjusted ', '').replace('/MWh', '')})

    # aggregate to annual
    df = df.drop(columns=['Month', 'Total net generation (MWh)'])
    df2 = df.groupby(
        ['Year', '860 Cooling Type 1', 'Generator Primary Technology',
         'Water Source Name', 'Water Type', 'County', 'State_y']).agg(
        {'Water Consumption (gal)': 'sum', 'Water Withdrawal (gal)': 'sum',
         'Total water discharge (million gallons) calc': 'sum'}).reset_index()
    # drop 'calc' from column name
    df2 = df2.rename(columns={'Total water discharge (million gallons) calc':
                              'Total water discharge (million gallons)'})
    # drop rows where no water withdrawal data
    df3 = df2[df2['Water Withdrawal (gal)'] != 0].reset_index(drop=True)

    # make column lower case
    df3['Water Source Name'] = \
        df3['Water Source Name'].apply(lambda x: x.lower())

    ground = 'wells|well|ground|gw|aquifer'
    surface = 'river|lake|reservoir|ocean|canal|creek|pond|bay|neosho|' \
              'stanton|gulf|pool|waterway|trinity|harbor|mississippi|' \
              'channel|laguna|sound|water way|coastal water authority|' \
              'folsom south'
    public_supply = 'municipal|muncpl|municipality|potw|city|muncipality|' \
                    'municiple|city|west kern|wheeler ridge|sayreville'
    reclaimed = 'water treatment|chemicals|nwtp'
    storm = 'storm water'

    # assign compartments
    df3['Compartment'] = ''
    df3['Compartment'] = np.where(
        df3["Water Type"] == 'Reclaimed', "Reclaimed", df3['Compartment'])
    df3['Compartment'] = np.where(
        (df3['Compartment'] == '') & (df3['Water Source Name'].str.contains(
            ground)), "Ground", df3['Compartment'])
    df3['Compartment'] = np.where(
        (df3['Compartment'] == '') & (df3['Water Source Name'].str.contains(
            surface)), "Surface", df3['Compartment'])
    df3['Compartment'] = np.where(
        (df3['Compartment'] == '') & (df3['Water Source Name'].str.contains(
            public_supply)), "Public Supply", df3['Compartment'])
    df3['Compartment'] = np.where(
        (df3['Compartment'] == '') & (df3['Water Source Name'].str.contains(
            reclaimed)), "Reclaimed", df3['Compartment'])
    df3['Compartment'] = np.where(
        (df3['Compartment'] == '') & (df3['Water Source Name'].str.contains(
            storm)), "Stormwater", df3['Compartment'])
    df3['Compartment'] = np.where(df3["Water Source Name"] == 'lake wells',
                                  "Surface", df3['Compartment'])

    # assign fips
    fips = get_county_FIPS()
    us_abb = pd.DataFrame(us_state_abbrev.items(),
                          columns=['State', 'State_y'])
    fips = fips.merge(us_abb, left_on='State', right_on='State')
    df3 = df3.merge(fips, left_on=['State_y', 'County'],
                    right_on=['State_y', 'County'])

    # melt df
    df4 = pd.melt(df3,
                  id_vars=['Year', '860 Cooling Type 1',
                           'Generator Primary Technology', 'Water Type',
                           'County', 'State_y', 'Water Source Name', 'State',
                           'FIPS', 'Compartment'], var_name='FlowName')

    # Assign ACB and APB columns
    df4['ActivityConsumedBy'] = np.where(df4['FlowName'].isin(
            ['Water Consumption (gal)', 'Water Withdrawal (gal)']),
        df4['Generator Primary Technology'], None)
    df4['ActivityProducedBy'] = np.where(df4['ActivityConsumedBy'].isnull(),
                                         df4['Generator Primary Technology'],
                                         None)

    # split flowname col into flowname and unit
    df4['Unit'] = df4['FlowName'].str.split('(').str[1]
    df4['Unit'] = df4['Unit'].apply(lambda x: x.replace(")", "")).str.strip()
    df4['FlowName'] = df4['FlowName'].str.split('(').str[0]
    df4['FlowName'] = df4['FlowName'].str.strip()
    # update water flownames
    df4['FlowName'] = np.where(df4['FlowName'].str.contains('Water|water'),
                               df4['Water Type'] + " " + df4['FlowName'],
                               df4['FlowName'])
    df4['FlowName'] = df4['FlowName'].apply(lambda x: x.title())

    # modify compartment if consumptive
    df4['Compartment'] = np.where(df4["FlowName"].str.contains('Consumption'),
                                  "Air", df4['Compartment'])

    df4['Class'] = np.where(df4['FlowName'].str.contains('Water|discharge'),
                            "Water", "Energy")
    df4['SourceName'] = source
    df4['DataReliability'] = 1
    df4['DataCollection'] = 5
    df4['FlowType'] = "ELEMENTARY_FLOW"
    df4['Description'] = 'Cooling Type: ' + df4['860 Cooling Type 1']
    df4 = df4.rename(columns={"value": "FlowAmount", "FIPS": "Location"})
    df4 = df4.drop(
        columns=['860 Cooling Type 1', 'Generator Primary Technology',
                 'Water Type', 'County', 'State', 'State_y',
                 'Water Source Name'])
    df4 = assign_fips_location_system(df4, str(year))

    return df4


def clean_plantwater_fba(fba_df, **kwargs):
    """
    Function to clean netl eia plantwater fba
    :param fba_df: df, FBA format
    :param kwargs: dictionary, can include attr, a dictionary of parameters
        in the FBA method yaml
    :return: df, modified BLS QCEW data
    """

    # for USGS water allocation, the NETL "Brackish" PLUS "saline" is
    # equivalent to USGS "Saline". So rename in the NETL dataset
    sal_sub = fba_df[fba_df['FlowName'].str.contains('Saline')][
        ['FlowName', 'Flowable', 'Compartment', 'Context',
         'FlowUUID']].drop_duplicates()
    sal_sub = sal_sub.rename(columns={'FlowName': 'fn',
                                      'Flowable': 'f',
                                      'Context': 'ct',
                                      'FlowUUID': 'fd'})

    # match original df with saline subset based on compartment
    dfm = fba_df.merge(sal_sub, on=['Compartment'])

    # where brackish water, replace flowname/flowable/context/flowuuid
    dfm['Flowable'] = np.where(dfm['FlowName'] == 'Brackish Water Withdrawal',
                               dfm['f'], dfm['Flowable'])
    dfm['Context'] = np.where(dfm['FlowName'] == 'Brackish Water Withdrawal',
                              dfm['ct'], dfm['Context'])
    dfm['FlowUUID'] = np.where(dfm['FlowName'] == 'Brackish Water Withdrawal',
                               dfm['fd'], dfm['FlowUUID'])
    dfm['FlowName'] = np.where(dfm['FlowName'] == 'Brackish Water Withdrawal',
                               dfm['fn'], dfm['FlowName'])

    dfm.drop(columns=['fn', 'f', 'ct', 'fd'], inplace=True)

    return dfm
