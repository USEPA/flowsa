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
from string import ascii_letters, ascii_uppercase


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
    result_list = []
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
                 'seven', 'eight', 'K-12 SCHOOLS',  'FOOD BANKS', 'INTERMEDIATE AMOUNT MANAGED',
                 'TOTAL MANAGED BY EACH PATHWAY'], axis=1, inplace=False)
        else:
            df = df_l[0].set_axis(
                ['MANAGEMENT PATHWAY', 'MANUFACTURING/ PROCESSING', 'RESIDENTIAL', 'RETAIL', 'WHOLESALE', 'HOTELS',
                 'seven', 'K-12 SCHOOLS', 'FOOD BANKS', 'INTERMEDIATE AMOUNT MANAGED',
                 'TOTAL MANAGED BY EACH PATHWAY'], axis=1, inplace=False)
        df = drop_rows(df)
        df_list.append(df)
    for d in df_list:
        result = result.append(d)
    result = fix_row_names(result)
    result = split_problem_column(result)
    result = reorder_df(result)
    result_list.append(result)
    return result_list


def epa_wfr_parse(*, df_list, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat list of dataframes (info on each page)
    df = pd.concat(df_list, sort=False)

    # hardcode
    # original data in short tons
    df['Class'] = 'Other'
    df['SourceName'] = 'EPA_WFR'
    df['FlowName'] = 'Food Waste'
    df['FlowType'] = 'WASTE_FLOW'
    df['Compartment '] = 'None'
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, '2018')
    df['Year'] = '2018'
    df['Unit'] = 'short tons'
    df['Description'] = 'EXCESS FOOD AND FOOD WASTE MANAGED BY SECTOR (TONS)'
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
    t = str.maketrans('', '', ascii_uppercase)
    seven_array_corrected = []
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
        strip = i.translate(t)
        seven_array = strip.split(" ")
        seven_array = ' '.join(seven_array).split()
        if len(seven_array) == 8:
            for idx, val in enumerate(seven_array):
                seven_array[idx] = val
                if val[0] == ",":
                    value_str = seven_array[idx - 1]
                    value_str = value_str + val
                    seven_array[idx - 1] = value_str
                    seven_array[idx] = ""
            seven_array = ' '.join(seven_array).split()

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
    df = df.drop(columns=['seven', 'eight', 'INTERMEDIATE AMOUNT MANAGED',
                 'TOTAL MANAGED BY EACH PATHWAY'])

    return df

def reorder_df(df):
    df = df.melt(id_vars="ActivityConsumedBy", var_name="ActivityProducedBy", value_name="FlowAmount")
    indexResult = df[df['FlowAmount'] == '-'].index
    df = df.replace(',', '', regex=True)
    df.drop(indexResult, inplace=True)
    df = df.reset_index()
    df = df.drop(columns=['index'])
    return df