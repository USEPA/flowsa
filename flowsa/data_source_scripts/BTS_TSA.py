# BTS_TSA.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

'''
Pulls BTS Transportation Satellite Account (TSA) data
'''
from io import BytesIO
import pandas as pd


def tsa_call(*, resp, year, **_):
    '''
    Convert response to pandas dataframe
    :param resp: response from url call
    :return: pandas dataframe of original source data
    '''
    df = pd.read_csv(BytesIO(resp.content))
    df = df[df.year == int(year)]
    return df


def tsa_parse(*, df_list, source, year, config, **_):
    df = pd.concat(df_list, sort=True)

    # Data on in-house production of transportation services for own use
    # comes from the Use table portion of the TSA
    use = (df[df.table_name.str.match('Use')
              & df.level_of_detail.str.match('Summary')]
           .drop(columns=df.columns.difference(
               list(config['parse']['rename_columns_use'].keys())
           ))
           .rename(columns=config['parse']['rename_columns_use'])
           .assign(SourceName=f'{source}.use'))
    in_house = use[use.Description.str.startswith('In-house')]

    # Data on for-hire production of transportation services (which may
    # be done as an ancillary or secondary activity), comes from the Make
    # table portion of the TSA
    make = (df[df.table_name.str.match('Make')
               & df.level_of_detail.str.match('Summary')]
            .drop(columns=df.columns.difference(
               list(config['parse']['rename_columns_make'].keys())
            ))
            .rename(columns=config['parse']['rename_columns_make'])
            .assign(SourceName=f'{source}.make'))
    for_hire = make[make.Description.str.startswith('For-hire')]

    df = pd.concat([in_house, for_hire])

    # Original units are in million USD
    df['FlowAmount'] = df['FlowAmount'] * 1000000

    # Add other columns as needed for complete FBA
    df['Class'] = 'Money'
    df['FlowName'] = 'Gross Output'
    df['Unit'] = 'USD'
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    # df['ActivityConsumedBy'] = ''
    # df['Compartment'] = ''
    df['Location'] = '00000'
    df['LocationSystem'] = 'FIPS_2015'
    # df['MeasureofSpread'] = ''
    df['DataReliability'] = 3  # temp
    df['DataCollection'] = 5  # temp

    return df
