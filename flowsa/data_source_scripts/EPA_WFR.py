# EPA_WFR.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Scrapes data from 2018 Wasted Food Report.
"""

import io
import tabula
import pandas as pd
# import numpy as np
from flowsa.common import US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system


def epa_wfr_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df_list = []
    result = pd.DataFrame()
    df = pd.DataFrame()
    pages = range(41, 43)
    for x in pages:
        df_l = tabula.read_pdf(io.BytesIO(resp.content),
                             pages=x, stream=True)
        if len(df_l[0].columns) == 12:
            df = df_l[0].set_axis(
                ['MANAGEMENT PATHWAY', 'MANUFACTURING/ PROCESSING', 'RESIDENTIAL', 'RETAIL', 'WHOLESALE', 'HOTELS',
                 'seven', 'eight', 'K-12 SCHOOLS',  'FOOD BANKS', 'INTERMEDIATE AMOUNT MANAGED1',
                 'TOTAL MANAGED BY EACH PATHWAY 2'], axis=1, inplace=False)
        else:
            df = df_l[0].set_axis(
                ['MANAGEMENT PATHWAY', 'MANUFACTURING/ PROCESSING', 'RESIDENTIAL', 'RETAIL', 'WHOLESALE', 'HOTELS',
                 'seven', 'K-12 SCHOOLS', 'FOOD BANKS', 'INTERMEDIATE AMOUNT MANAGED1',
                 'TOTAL MANAGED BY EACH PATHWAY 2'], axis=1, inplace=False)
        df = drop_rows(df)
        df_list.append(df)
    for d in df_list:
        result = result.append(d)
    result = fix_row_names(result)
    result = split_problem_column(result)



    return result


def epa_wfr_parse(*, df_list, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat list of dataframes (info on each page)
    df = pd.concat(df_list, sort=False)
    df = df.rename(columns={"I-O code": "ActivityConsumedBy",
                            "I-O description": "Description",
                            "gal/$M": "FlowAmount",
                            })
    # hardcode
    # original data in gal/million usd
    df.loc[:, 'FlowAmount'] = df['FlowAmount'] / 1000000
    df['Unit'] = 'gal/USD'
    df['SourceName'] = 'Blackhurst_IO'
    df['Class'] = 'Water'
    df['FlowName'] = 'Water Withdrawals IO Vector'
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, '2002')
    df['Year'] = '2002'
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df

def drop_rows(df):
    df = df.drop(index=[0, 1, 2, 3])
    df = df.dropna(axis=0, subset=['MANUFACTURING/ PROCESSING'])
    return df

def fix_row_names(df):
    acb = ['Food Donation', 'Animal Feed', 'Codigestion/ Anaerobic Digestion', 'Composting/ Aerobic Processes',
                   'Bio-based Materials/ Biochemical Processing', 'Land Application', 'Sewer/ Wastewater Treatment',
                   'Landfill', 'Controlled Combustion', 'Total Food Waste & Excess Food', 'Percent of Total']
    df['ActivityConsumedBy'] = acb
    df = df.reset_index()
    df = df.drop(columns=['index', 'MANAGEMENT PATHWAY'])
    df = df.drop(index=[9, 10])
    return df

def split_problem_column(df):
    restaurants_list = []
    sports_list = []
    hospitals_list = []
    nursing_list = []
    military_list = []
    office_list = []
    correctional_list = []
    colleges_list = []
    index = 0

    col_seven_list = df['seven'].tolist()
    col_eight_list = df['eight'].tolist()
    for i in col_seven_list:
        seven_array = i.split(" ")
        restaurants_list.append(seven_array[0])
        sports_list.append(seven_array[1])
        hospitals_list.append(seven_array[2])
        nursing_list.append(seven_array[3])
        military_list.append(seven_array[4])
        office_list.append(seven_array[5])
        correctional_list.append(seven_array[6])
        if len(seven_array) == 7:
            colleges_list.append(col_eight_list[index])
        else:
            colleges_list.append(seven_array[7])

        index = index + 1
    df['RESTAURANTS/ FOOD SERVICES'] = restaurants_list
    df['SPORTS VENUES'] = sports_list
    df['HOSPITALS'] = hospitals_list
    df['NURSING HOMES'] = nursing_list
    df['MILITARY INSTALLATIONS'] = military_list
    df['OFFICE BUILDINGS'] = office_list
    df['CORRECTIONAL FACILITIES'] = correctional_list
    df['COLLEGES & UNIVERSITIES'] = colleges_list
    df = df.drop(columns=['seven', 'eight', 'INTERMEDIATE AMOUNT MANAGED1',
                 'TOTAL MANAGED BY EACH PATHWAY 2'])
    return df

