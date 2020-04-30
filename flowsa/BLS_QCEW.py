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
import zipfile
from flowsa.common import log, get_all_state_FIPS_2


def BLS_QCEW_URL_helper(build_url, config, args):
    urls = []
    FIPS_2 = get_all_state_FIPS_2()['FIPS_2']

    # the url for 2013 earlier is different than the base url (and is a zip file)
    if args["year"] < '2014':
        url = build_url
        url = url.replace('api', 'files')
        url = url.replace('a/area/__areaFIPS__.csv', 'csv/' + args["year"] + '_annual_by_area.zip')
        urls.append(url)
    else:
        for c in FIPS_2:
            url = build_url
            url = url.replace('__areaFIPS__', c + '000')
            urls.append(url)
    return urls

def bls_qcew_call(url, qcew_response, args):
    if args["year"] < '2014':
        # initiate dataframes list
        df_list = []
        # unzip folder that contains bls data in ~4000 csv files
        with zipfile.ZipFile(io.BytesIO(qcew_response.content), "r") as f:
            # read in file names
            for name in f.namelist():
                # Only want state info
                if "Statewide" in name:
                    data = f.open(name)
                    df_state = pd.read_csv(data, header=0)
                    df_list.append(df_state)
                    # concat data into single dataframe
                    df = pd.concat(df_list, sort=False)
                    df = df[['area_fips', 'own_code', 'industry_code', 'year',
                             'annual_avg_estabs_count', 'annual_avg_emplvl', 'total_annual_wages']]
                    # change column name to match format for 2014+
                    df = df.rename(columns={'annual_avg_estabs_count': 'annual_avg_estabs'})
            return df
    else:
        df = pd.read_csv(io.StringIO(qcew_response.content.decode('utf-8')))
        df = df[['area_fips', 'own_code', 'industry_code', 'year',
                 'annual_avg_estabs', 'annual_avg_emplvl', 'total_annual_wages']]
        return df


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
    # add hard code data
    df['SourceName'] = 'BLS_QCEW'
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

