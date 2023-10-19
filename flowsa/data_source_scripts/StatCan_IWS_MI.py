# Stat_Canada.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls Statistics Canada data on water intake and discharge for 3
digit NAICS from 2005 - 2015
"""

import io
import zipfile
import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system, aggregator,\
    load_fba_w_standardized_units
from flowsa.location import US_FIPS, call_country_code
from flowsa.common import fba_default_grouping_fields, \
    load_crosswalk, WITHDRAWN_KEYWORD
from flowsa.validation import compare_df_units


def sc_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe,
    begin parsing df into FBA format
    :param resp: response, response from url call
    :return: pandas dataframe of original source data
    """
    # Convert response to dataframe
    # read all files in the stat canada zip
    with zipfile.ZipFile(io.BytesIO(resp.content), "r") as f:
        # read in file names
        for name in f.namelist():
            # if filename does not contain "MetaData", then create dataframe
            if "MetaData" not in name:
                data = f.open(name)
                df = pd.read_csv(data, header=0)
    return df


def sc_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)
    # drop columns
    df = df.drop(columns=['COORDINATE', 'DECIMALS', 'DGUID', 'SYMBOL',
                          'TERMINATED', 'UOM_ID', 'SCALAR_ID', 'VECTOR'])
    # rename columns
    df = df.rename(
        columns={'GEO': 'Location',
                 'North American Industry Classification System (NAICS)':
                     'Description',
                 'REF_DATE': 'Year',
                 'STATUS': 'Spread',
                 'VALUE': "FlowAmount",
                 'Water use parameter': 'FlowName'})
    # extract NAICS as activity column. rename activity based on flowname
    df['Activity'] = df['Description'].str.extract('.*\[(.*)\].*')
    df.loc[df['Description'] == 'Total, all industries', 'Activity'] = '31-33'
    df.loc[df['Description'] == 'Other manufacturing industries',
           'Activity'] = 'Other'
    df['FlowName'] = df['FlowName'].str.strip()
    df.loc[df['FlowName'] == 'Water intake', 'ActivityConsumedBy'] = \
        df['Activity']
    df.loc[df['FlowName'].isin(
        ['Water discharge', 'Water recirculation']), 'ActivityProducedBy'] = \
        df['Activity']
    # create "unit" column
    df["Unit"] = "million " + df["UOM"] + "/year"
    # drop columns used to create unit and activity columns
    df = df.drop(columns=['SCALAR_FACTOR', 'UOM', 'Activity'])
    # Modify the assigned RSD letter values to numeric value
    df.loc[df['Spread'] == 'A', 'Spread'] = 2.5  # given range: 0.01 - 4.99%
    df.loc[df['Spread'] == 'B', 'Spread'] = 7.5  # given range: 5 - 9.99%
    df.loc[df['Spread'] == 'C', 'Spread'] = 12.5  # given range: 10 - 14.99%
    df.loc[df['Spread'] == 'D', 'Spread'] = 20  # given range: 15 - 24.99%
    df.loc[df['Spread'] == 'E', 'Spread'] = 37.5  # given range:25 - 49.99%
    df.loc[df['Spread'] == 'F', 'Spread'] = 75  # given range: > 49.99%
    df.loc[df['Spread'] == 'x', 'Spread'] = WITHDRAWN_KEYWORD
    # hard code data
    df['Class'] = 'Water'
    df['SourceName'] = 'StatCan_IWS_MI'
    # temp hardcode canada iso code
    df['Location'] = call_country_code('Canada')
    df['Year'] = df['Year'].astype(str)
    df['LocationSystem'] = "ISO"
    df["MeasureofSpread"] = 'RSD'
    df["DataReliability"] = 3
    df["DataCollection"] = 4

    # subset based on year
    df = df[df['Year'] == year]

    return df


def convert_statcan_data_to_US_water_use(df, attr, download_FBA_if_missing):
    """
    Use Canadian GDP data to convert 3 digit canadian water use to us water
    use:
    - canadian gdp
    - us gdp
    :param df: df, FBA format
    :param attr: dictionary, attribute data from method yaml for activity set
    :param download_FBA_if_missing: bool, True if would like to download
        missing FBAs from Data Commons, False if FBAs should be generated
        locally
    :return: df, FBA format, flowamounts converted
    """

    # load Canadian GDP data
    gdp = load_fba_w_standardized_units(
        datasource='StatCan_GDP', year=attr['allocation_source_year'],
        flowclass='Money', download_FBA_if_missing=download_FBA_if_missing)

    # drop 31-33
    gdp = gdp[gdp['ActivityProducedBy'] != '31-33']
    gdp = gdp.rename(columns={"FlowAmount": "USD"})

    # check units before merge
    compare_df_units(df, gdp)
    # merge df
    df_m = pd.merge(df, gdp[['USD', 'ActivityProducedBy']],
                    how='left', left_on='ActivityConsumedBy',
                    right_on='ActivityProducedBy')
    df_m['USD'] = df_m['USD'].fillna(0)
    df_m = df_m.drop(columns=["ActivityProducedBy_y"])
    df_m = df_m.rename(columns={"ActivityProducedBy_x": "ActivityProducedBy"})
    df_m = df_m[df_m['USD'] != 0]
    # # convert to kg/USD
    df_m.loc[:, 'FlowAmount'] = df_m['FlowAmount'] / df_m['USD']
    df_m.loc[:, 'Unit'] = 'kg/USD'

    df_m = df_m.drop(columns=["USD"])

    # convert Location to US
    df_m.loc[:, 'Location'] = US_FIPS
    df_m = assign_fips_location_system(
        df_m, str(attr['allocation_source_year']))

    # load us gdp
    # load Canadian GDP data
    us_gdp_load = load_fba_w_standardized_units(
        datasource='BEA_Detail_GrossOutput_IO', year=attr[
            'allocation_source_year'],
        flowclass='Money', download_FBA_if_missing=download_FBA_if_missing)

    # load bea crosswalk
    cw_load = load_crosswalk('NAICS_to_BEA_Crosswalk_2012')
    cw = cw_load[['BEA_2012_Detail_Code', 'NAICS_2012_Code']].drop_duplicates()
    cw = cw[cw['NAICS_2012_Code'].apply(
        lambda x: len(str(x)) == 3)].drop_duplicates().reset_index(drop=True)

    # merge
    us_gdp = pd.merge(
        us_gdp_load, cw, how='left', left_on='ActivityProducedBy',
        right_on='BEA_2012_Detail_Code')
    us_gdp = us_gdp.drop(
        columns=['ActivityProducedBy', 'BEA_2012_Detail_Code'])
    # rename columns
    us_gdp = us_gdp.rename(columns={'NAICS_2012_Code': 'ActivityProducedBy'})
    # agg by naics
    us_gdp = aggregator(us_gdp, fba_default_grouping_fields)
    us_gdp = us_gdp.rename(columns={'FlowAmount': 'us_gdp'})

    # determine annual us water use
    df_m2 = pd.merge(df_m, us_gdp[['ActivityProducedBy', 'us_gdp']],
                     how='left', left_on='ActivityConsumedBy',
                     right_on='ActivityProducedBy')

    df_m2.loc[:, 'FlowAmount'] = df_m2['FlowAmount'] * (df_m2['us_gdp'])
    df_m2.loc[:, 'Unit'] = 'kg'
    df_m2 = df_m2.rename(
        columns={'ActivityProducedBy_x': 'ActivityProducedBy'})
    df_m2 = df_m2.drop(columns=['ActivityProducedBy_y', 'us_gdp'])

    return df_m2
