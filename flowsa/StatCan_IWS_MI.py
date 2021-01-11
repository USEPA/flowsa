# Stat_Canada.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
'''
Pulls Statistics Canada data on water intake and discharge for 3 digit NAICS from 2005 - 2015
'''

import pandas as pd
import io
import zipfile
import pycountry
from flowsa.common import *

def sc_call(url, sc_response, args):
    # Convert response to dataframe
    # read all files in the stat canada zip
    with zipfile.ZipFile(io.BytesIO(sc_response.content), "r") as f:
        # read in file names
        for name in f.namelist():
            # if filename does not contain "MetaData", then create dataframe
            if "MetaData" not in name:
                data = f.open(name)
                df = pd.read_csv(data, header=0)
    return df



def sc_parse(dataframe_list, args):
    # concat dataframes
    df = pd.concat(dataframe_list, sort=False)
    # drop columns
    df = df.drop(columns=['COORDINATE', 'DECIMALS', 'DGUID', 'SYMBOL', 'TERMINATED', 'UOM_ID', 'SCALAR_ID', 'VECTOR'])
    # rename columns
    df = df.rename(columns={'GEO': 'Location',
                            'North American Industry Classification System (NAICS)': 'Description',
                            'REF_DATE': 'Year',
                            'STATUS': 'Spread',
                            'VALUE': "FlowAmount",
                            'Water use parameter': 'FlowName'})
    # extract NAICS as activity column. rename activity based on flowname
    df['Activity'] = df['Description'].str.extract('.*\[(.*)\].*')
    df.loc[df['Description'] == 'Total, all industries', 'Activity'] = '31-33'  # todo: change these activity names
    df.loc[df['Description'] == 'Other manufacturing industries', 'Activity'] = 'Other'
    df['FlowName'] = df['FlowName'].str.strip()
    df.loc[df['FlowName'] == 'Water intake', 'ActivityConsumedBy'] = df['Activity']
    df.loc[df['FlowName'].isin(['Water discharge', "Water recirculation"]), 'ActivityProducedBy'] = df['Activity']
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
    df.loc[df['Spread'] == 'x', 'Spread'] = withdrawn_keyword
    # hard code data
    df['Class'] = 'Water'
    df['SourceName'] = 'StatCan_IWS_MI'
    # temp hardcode canada iso code
    df['Location'] = call_country_code('Canada')
    df['Year'] = df['Year'].astype(str)
    df['LocationSystem'] = "ISO"
    df["MeasureofSpread"] = 'RSD'
    df["DataReliability"] = '3'
    df["DataCollection"] = '4'

    # subset based on year
    df = df[df['Year'] == args['year']]

    return df


def convert_statcan_data_to_US_water_use(df, attr):
    """
    Use Canadian GDP data to convert 3 digit canadian water use to us water
    use:
    - canadian gdp
    - us gdp
    :return:
    """
    import flowsa
    from flowsa.values_from_literature import get_Canadian_to_USD_exchange_rate
    from flowsa.flowbyfunctions import assign_fips_location_system, aggregator, fba_default_grouping_fields, harmonize_units
    from flowsa.common import US_FIPS, load_bea_crosswalk

    # load Canadian GDP data
    gdp = flowsa.getFlowByActivity(flowclass=['Money'], datasource='StatCan_GDP', years=[attr['allocation_source_year']])
    gdp = harmonize_units(gdp)
    # drop 31-33
    gdp = gdp[gdp['ActivityProducedBy'] != '31-33']
    gdp = gdp.rename(columns={"FlowAmount": "CanDollar"})

    # merge df
    df_m = pd.merge(df, gdp[['CanDollar', 'ActivityProducedBy']], how='left', left_on='ActivityConsumedBy',
                    right_on='ActivityProducedBy')
    df_m['CanDollar'] = df_m['CanDollar'].fillna(0)
    df_m = df_m.drop(columns=["ActivityProducedBy_y"])
    df_m = df_m.rename(columns={"ActivityProducedBy_x": "ActivityProducedBy"})
    df_m = df_m[df_m['CanDollar'] != 0]

    exchange_rate = get_Canadian_to_USD_exchange_rate(str(attr['allocation_source_year']))
    exchange_rate = float(exchange_rate)
    # convert to mgal/USD
    df_m.loc[:, 'FlowAmount'] = df_m['FlowAmount'] / (df_m['CanDollar'] / exchange_rate)
    df_m.loc[:, 'Unit'] = 'Mgal/USD'

    df_m = df_m.drop(columns=["CanDollar"])

    # convert Location to US
    df_m.loc[:, 'Location'] = US_FIPS
    df_m = assign_fips_location_system(df_m, str(attr['allocation_source_year']))

    # load us gdp
    # load Canadian GDP data
    us_gdp_load = flowsa.getFlowByActivity(flowclass=['Money'], datasource='BEA_GDP_GrossOutput_IO', years=[attr['allocation_source_year']])
    us_gdp_load = harmonize_units(us_gdp_load)
    # load bea crosswalk
    cw_load = load_bea_crosswalk()
    cw = cw_load[['BEA_2012_Detail_Code', 'NAICS_2012_Code']].drop_duplicates()
    cw = cw[cw['NAICS_2012_Code'].apply(lambda x: len(str(x)) == 3)].drop_duplicates().reset_index(drop=True)

    # merge
    us_gdp = pd.merge(us_gdp_load, cw, how='left', left_on='ActivityProducedBy', right_on='BEA_2012_Detail_Code')
    us_gdp = us_gdp.drop(columns=['ActivityProducedBy', 'BEA_2012_Detail_Code'])
    # rename columns
    us_gdp = us_gdp.rename(columns={'NAICS_2012_Code': 'ActivityProducedBy'})
    # agg by naics
    us_gdp = aggregator(us_gdp, fba_default_grouping_fields)
    us_gdp = us_gdp.rename(columns={'FlowAmount': 'us_gdp'})

    # determine annual us water use
    df_m2 = pd.merge(df_m, us_gdp[['ActivityProducedBy', 'us_gdp']], how='left', left_on='ActivityConsumedBy',
                     right_on='ActivityProducedBy')

    df_m2.loc[:, 'FlowAmount'] = df_m2['FlowAmount'] * (df_m2['us_gdp'])
    df_m2.loc[:, 'Unit'] = 'Mgal'
    df_m2 = df_m2.rename(columns={'ActivityProducedBy_x': 'ActivityProducedBy'})
    df_m2 = df_m2.drop(columns=['ActivityProducedBy_y', 'us_gdp'])

    return df_m2


def disaggregate_statcan_to_naics_6(df):

    return df
