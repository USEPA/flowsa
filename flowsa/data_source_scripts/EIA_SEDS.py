# EIA_SEDS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
EIA State Energy Data System
https://www.eia.gov/state/seds/
2010 - 2020
"""

import io
import pandas as pd
import numpy as np
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.location import get_all_state_FIPS_2, \
    us_state_abbrev, US_FIPS


def eia_seds_url_helper(*, build_url, config, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url
    text string to be replaced with info specific to the data year. This
    function does not parse the data, only modifies the urls from which
    data is obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :return: list, urls to call, concat, parse, format into
        Flow-By-Activity format
    """
    urls = []
    url = build_url
    csvs = config.get('csvs')
    for csv in csvs:
        urls.append(url + csv)
    return urls


def eia_seds_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin
    parsing df into FBA format
    :param resp: response, response from url call
    :return: pandas dataframe of original source data
    """
    with io.StringIO(resp.text) as fp:
        df = pd.read_csv(fp, encoding="ISO-8859-1")
    columns = ['Data_Status', 'State', 'MSN']
    columns.append(year)
    for col in df.columns:
        if col not in columns:
            df = df.drop(col, axis=1)
    return df

def eia_seds_parse(*, df_list, year, config, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    df = pd.concat(df_list, sort=False, ignore_index=True)

    fips = get_all_state_FIPS_2().reset_index(drop=True)
    # ensure capitalization of state names
    fips['State'] = fips['State'].apply(lambda x: x.title())
    fips['StateAbbrev'] = fips['State'].map(us_state_abbrev)
    # pad zeroes
    fips['FIPS_2'] = fips['FIPS_2'].apply(lambda x: x.ljust(3 + len(x), '0'))
    df = pd.merge(
        df, fips, how='left', left_on='State', right_on='StateAbbrev')
    # set us location code
    df.loc[df['State_x'] == 'US', 'FIPS_2'] = US_FIPS

    df = df.rename(columns={'FIPS_2': "Location"})
    assign_fips_location_system(df, year)
    df = df.drop(columns=['StateAbbrev', 'State_x', 'State_y'])
    
    ## Extract information for SEDS codes
    units = pd.read_excel(config['url']['activities_url'],
                          sheet_name='MSN descriptions',
                          header=10, usecols='B:D')
    units['FuelCode'] = units['MSN'].str[0:2]
    units['SectorCode'] = units['MSN'].str[2:4]
    units['UnitCode'] = units['MSN'].str[4:5]
    units = units.query("UnitCode not in ['D', 'K']")

    # get fuel names from Total Consumption and Industrial Consumption
    fuels = (units.query("SectorCode.isin(['TC', 'IC'])")
             .reset_index(drop=True))
    fuels['Fuel'] = (fuels.query(
        "Description.str.contains('total consumption')")
        .Description.str.split(' total consumption', expand=True)[0])
    fuels['FuelName2'] = (fuels.query(
        "Description.str.contains('consumed by')")
        .Description.str.split(' consumed by', expand=True)[0])
    fuels['Fuel'] = fuels['Fuel'].fillna(fuels['FuelName2'])
    fuels['Fuel'] = fuels['Fuel'].str.rstrip(',')
    fuels = (fuels[['Fuel','FuelCode']].dropna().sort_values(by='Fuel')
             .drop_duplicates(subset='FuelCode'))

    # get sector names
    sectors = units.copy()
    sectors['ActivityConsumedBy'] = (units.query(
        "Description.str.contains('consumed by')")
        .Description.str.split('consumed by the ', expand=True)[1]
        .str.strip())
    sectors = (sectors[['SectorCode', 'ActivityConsumedBy']].dropna()
               .sort_values(by='ActivityConsumedBy')
               .drop_duplicates(subset='SectorCode'))

    units = units.merge(fuels, how='left', on='FuelCode')
    units = units.merge(sectors, how='left', on='SectorCode')
    units = units.drop(columns=['FuelCode','SectorCode','UnitCode'])
    units['Description'] = units['MSN'] + ': ' + units['Description']

    df = df.merge(units, how='left', on='MSN')
    df = (df.rename(columns={year: "FlowAmount",
                            "Fuel": "FlowName"})
          .drop(columns=['Data_Status'])
          .dropna())

    # hard code data
    df['Class'] = np.where(df['Unit'].str.contains('Btu') |
                           df['Unit'].str.contains('watt'),
                           'Energy', 'Other')
    df['SourceName'] = 'EIA_SEDS'
    df['ActivityProducedBy'] = 'None'
    df['Year'] = year
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    # Fill in the rest of the Flow by fields so they show
    # "None" instead of nan.
    df['Compartment'] = 'None'
    df['MeasureofSpread'] = 'None'
    df['DistributionType'] = 'None'
    # Add DQ scores
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df

if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='EIA_SEDS', year=2020)
    fba = flowsa.getFlowByActivity('EIA_SEDS', 2020)
