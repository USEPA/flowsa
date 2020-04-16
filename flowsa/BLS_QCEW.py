# BLS_QCEW.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
'''
Pulls Quarterly Census of Employment and Wages data in NAICS from Bureau of Labor Statistics
Writes out to various FlowBySector class files for these data items
EMP = Number of employees, Class = Employment
PAYANN = Annual payroll ($1,000), Class = Money
ESTAB = Number of establishments, Class = Other
This script is designed to run with a configuration parameter
--year = 'year' e.g. 2015
'''

import pandas as pd
import io
from flowsa.common import log, get_all_state_FIPS_2

def BLS_QCEW_URL_helper(build_url, config, arg):
    urls_bls = []
    FIPS_2 = get_all_state_FIPS_2()['FIPS_2']
    for c in FIPS_2:
        url = build_url
        url = url.replace('__areaFIPS__', c + '000')
        urls_bls.append(url)
    return urls_bls


def bls_qcew_call(url, qcew_response):
    # Convert response to dataframe
    df_bls = pd.read_csv(io.StringIO(qcew_response.content.decode('utf-8')))
    df_bls = df_bls[['area_fips', 'own_code', 'industry_code', 'year',
                     'annual_avg_estabs', 'annual_avg_emplvl', 'total_annual_wages']]
    return df_bls


def bls_qcew_parse(dataframe_list, args):
    # Concat dataframes
    df = pd.concat(dataframe_list, sort=False)
    # Keep owner_code = 1, 2, 3, 5
    df = df[df.own_code.isin([1, 2, 3, 5])]
    # Aggregate annual_avg_estabs and annual_avg_emplvl by area_fips, industry_code, year, flag
    df = df.groupby(['area_fips', 'industry_code', 'year'])[['annual_avg_estabs',
                                                             'annual_avg_emplvl',
                                                             'total_annual_wages']].sum().reset_index()
    # Rename fields
    df = df.rename(columns={'area_fips': 'FIPS',
                            'industry_code': 'ActivityProducedBy',
                            'year': 'Year',
                            'annual_avg_estabs': 'ESTAB',
                            'annual_avg_emplvl': 'EMP',
                            'total_annual_wages': 'PAYANN'})
    # Reformat FIPs to 5-digit
    df['FIPS'] = df['FIPS'].apply('{:0>5}'.format)
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    return df

qcew_flow_specific_metadata = \
    {'EMP': {'Class': 'Employment',
             'FlowName': 'Number of employees',
             'Unit': 'p'},
     'ESTAB': {'Class': 'Other',
               'FlowName': 'Number of establishments',
               'Unit': 'p'},
     'PAYANN': {'Class': 'Money',
                'FlowName': 'Annual payroll',
                'Unit': 'USD'},
     }