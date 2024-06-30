# BLS_CES.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls Consumer Expenditure Survey data from  Bureau of Labor Statistics.
"""

import json
import io
import pandas as pd
import numpy as np
import itertools as it
from collections import OrderedDict
from esupy.remote import make_url_request
from flowsa.common import load_env_file_key
from flowsa.location import US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.flowbyactivity import FlowByActivity
from flowsa.flowsa_log import log
from flowsa.naics import industry_spec_key
from flowsa.settings import externaldatapath


def read_ces_item_codes():
    # https://download.bls.gov/pub/time.series/cx/cx.item
    df = pd.read_csv(externaldatapath / 'ces_items.csv')
    df = df.query('selectable == "T"')

    return df


def bls_ces_call(config, year):    
    """

    """
    headers = {'Content-type': 'application/json'}
    api_key = load_env_file_key('API_Key', config['api_name'])
    series = read_ces_item_codes()['item_code']
    series_dict0 = OrderedDict(config['series'])
    series_dict0['item'] = list(series)
    series_dict = OrderedDict((k, series_dict0[k]) for k in
                              ('prefix', 'seasonal', 'item',
                               'demographics', 'characteristics',
                               'process'))

    combinations = it.product(*(series_dict[Name] for Name in series_dict))
    series_list = ["".join(x) for x in list(combinations)]
    df_list = []
    # Do this in chunks of 50 per API limits
    for i in range(0, len(series_list), 50): 
        x = i 
        short_series = series_list[x:x+50] 
    
        data = json.dumps({"seriesid": short_series,
                           "startyear":2004, "endyear":2022,
                           "registrationkey": api_key})
    
        
        response = make_url_request(url=config['base_url'],
                                    method='POST',
                                    data=data, headers=headers)
        
        json_data = json.loads(response.content)
        for series in json_data['Results']['series']:
            data = series['data']
            df = pd.DataFrame(data=data[0:len(data)],
                              columns=data[0])
            df['series'] = series['seriesID']
            df_list.append(df)
    return df_list


def bls_ces_parse(*, df_list, config, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    df_list = bls_ces_call(config, year)
    # Concat dataframes
    df = pd.concat(df_list, sort=False)
    series_df = read_ces_item_codes()
    df = (df
          .assign(region = lambda x: x['series'].str[-3:].str[:2]) # 16th and 17th
          .assign(code = lambda x: x['series'].str[:11].str[-8:])
          .merge(series_df
                 .filter(['item_code', 'item_text'])
                 .rename(columns={'item_code':'code'}),
                 how='left', on='code')
          .rename(columns={'year':'Year',
                           'value':'FlowAmount'})
          .drop(columns=['period', 'periodName', 'latest'])
          )
                 

    return df

if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='BLS_CES', year='2004-2022')
    fba = flowsa.getFlowByActivity('BLS_CES', year=2020)
