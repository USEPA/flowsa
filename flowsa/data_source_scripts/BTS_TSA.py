# BEA.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

'''
Pulls BTS Transportation Satellite Account (TSA) data
'''
from io import BytesIO
import pandas as pd
from flowsa.common import US_FIPS, fbs_activity_fields
from flowsa.schema import activity_fields
from flowsa.settings import externaldatapath
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.fbs_allocation import allocation_helper


def tsa_call(*, resp, year, **_):
    '''
    Convert response to pandas dataframe
    :param resp: response from url call
    :return: pandas dataframe of original source data
    '''
    df = pd.read_csv(BytesIO(resp.content))
    return df[df.year == year]


def tsa_parse(*, df_list, source, year, config, **_):
    df = (pd.concat(df_list, sort=True)
          .rename(columns=config['parse']['rename_columns']))
