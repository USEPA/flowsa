# BTS_FAF.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls ORNL Freight Analysis Framework dataset
"""

import pandas as pd
import numpy as np
import zipfile
import tabula

import io
import requests
from esupy.remote import make_url_request
from flowsa.flowbyactivity import FlowByActivity
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.location import US_FIPS
from flowsa.settings import externaldatapath

def faf_call(*, resp, config, **_):
    """
    Convert response for calling url to pandas dataframe,
    begin parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    df_list = []
    ## do we need the state database?
    # url = 'https://www.bts.gov/sites/bts.dot.gov/files/2024-03/FAF4.5.1_csv_2013-2018.zip'
    # year = 2016
    # file = f'FAF4.5.1_{year}.csv'

    # resp = make_url_request(url)
    # with zipfile.ZipFile(io.BytesIO(resp.content), "r") as zf:
    #     with zf.open(file) as csvf:
    #         df = pd.read_csv(csvf)

    # zipf = "FAF5.6.1_State_2018-2023.zip"
    # file = 'FAF5.6.1_State_2018-2023.csv'
    # with zipfile.ZipFile(externaldatapath / zipf, "r") as zf:
    #     with zf.open(file) as csvf:
    #         df = pd.read_csv(csvf)
    # df_list.append(df)

    # url = https://faf.ornl.gov/faf5/Data/Download_Files/FAF5.6.1_State.zip
    zipf = "FAF5.6.1_State.zip"
    file = "FAF5.6.1_State.csv"
    with zipfile.ZipFile(externaldatapath / zipf, "r") as zf:
        with zf.open(file) as csvf:
            df = pd.read_csv(csvf)
    df_list.append(df)

    return df_list


def faf_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """

    df_list = faf_call(resp=None, config=None)

    # URL of the PDF
    pdf = "FAF5 User Guide.pdf"
    # pdf = 'https://faf.ornl.gov/faf5/data/FAF5%20User%20Guide.pdf'
    # Specify the page number
    page_number = 8  # Change this to the desired page number
    # Read the table from the specified page
    tables = tabula.read_pdf(externaldatapath / pdf, pages=page_number)
    # Convert the first table to a DataFrame
    codes = tables[0] if tables else pd.DataFrame()

    tables2 = tabula.read_pdf(externaldatapath / pdf, pages=7)
    modes = tables2[0] if tables2 else pd.DataFrame()
    modes = (modes
             .dropna(subset='Code')
             .replace('Multiple Modes and', 'Multiple Modes and Mail')
             )

    df0 = pd.concat(df_list)
    # https://faf.ornl.gov/faf5/data/FAF5%20User%20Guide.pdf
    df_list=[]
    for year in range(2017, 2024):
        col_list = [
             # 'fr_orig', 'dms_origst', 'dms_destst', 'fr_dest',
             # 'fr_inmode', 'fr_outmode', 'dist_band',
             'dms_mode', 'sctg2', 'trade_type',
             f'tons_{year}', f'current_value_{year}', f'tmiles_{year}']
        if year == 2017:
            col_list.append('value_2017')
            col_list.remove('current_value_2017')
            value_var = 'value_2017'
        else:
            value_var = f'current_value_{year}'
        df1 = (df0
               .filter(col_list)
               .query('trade_type == 1') # Domestic only
               )

        df1 = (df1
               # .drop(columns=['fr_orig', 'dms_origst', 'dms_destst']) # drop foreign source and state-to-state details
               .pivot_table(index=['sctg2', 'dms_mode'], # pivot on good (sctg2) and mode
                            aggfunc='sum')
               .reset_index()
               .merge(codes.rename(columns={'Code': 'sctg2'}), on='sctg2', how='left')
               .merge(modes.rename(columns={'Code': 'dms_mode'}), on='dms_mode', how='left')
               .drop(columns=['Description'])
               )
        df1 = (df1
               .melt(id_vars=['sctg2', 'dms_mode', 'Mode', 'Commodity Description'],
                     value_vars=[f'tons_{year}', value_var, f'tmiles_{year}'],
                     value_name='FlowAmount')
               .assign(Year = str(year))
               .assign(Unit = lambda x: x['variable'].replace(
                   {f'tons_{year}': 'tons',
                    value_var: 'current value',
                    f'tmiles_{year}': "ton-miles"}))
               # Dollars and ton miles in millions, tons in thousands
               .assign(FlowAmount = lambda x: np.where(x.Unit == "tons",
                    x.FlowAmount * 1000, x.FlowAmount * 1000000))
               .drop(columns=['variable'])
               .rename(columns={'Commodity Description': 'FlowName',
                                'Mode': 'ActivityProducedBy'})
               )
        df_list.append(df1)
    df = pd.concat(df_list)

    df = (df
          .assign(Class = "Other")
          .assign(SourceName = 'BTS_FAF')
          .assign(Compartment = None)
          .assign(FlowType = "TECHNOSPHERE_FLOW")
          # .assign(Description = '')
          .assign(Location = US_FIPS)
          .pipe(assign_fips_location_system, 2024)
    # Temp data quality
          .assign(DataReliability = 5)
          .assign(DataCollection = 5)
          .drop(columns=['sctg2', 'dms_mode'])
          )

    return df

def move_flow_to_ACB(fba: FlowByActivity, **_) -> FlowByActivity:
    """clean_fba_before_activity_sets fxn
    """
    ## this moves the flow name (SCTG description) to ACB
    fba = (fba
            .assign(ActivityConsumedBy = fba['FlowName'])
            )
    return fba


if __name__ == "__main__":
    import flowsa
    flowsa.generateflowbyactivity.main(source='BTS_FAF', year='2017-2023')
    fba = flowsa.getFlowByActivity('BTS_FAF', 2017)
