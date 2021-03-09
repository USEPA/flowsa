# BEA.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Supporting functions for BEA data
"""
import pandas as pd
import io
import zipfile
import pycountry
from flowsa.common import *
from flowsa.flowbyfunctions import assign_fips_location_system

def bea_make_call(url, response_load, args):

    # code not working, temporarily loading data as csv
    df = []

    # # read all files in the stat canada zip
    # with zipfile.ZipFile(io.BytesIO(response_load.content), "r") as f:
    #     # read in file names
    #     for name in f.namelist():
    #         # if filename does not contain "MetaData", then create dataframe
    #         if 'After Redef Make Use & DR- Detail/IOUseDetail.txt' in name:
    #             data = f.open(name)
    #             df = pd.read_csv(data, sep=" ", header=0)


    return df


def bea_make_parse(dataframe_list, args):
    # concat dataframes - tmp load from uploaded csv
    # df = pd.concat(dataframe_list, sort=False)
    df_load = pd.read_csv(datapath + "BEA_Make_Table_after_Redef_2002.csv", dtype="str")
    # strip whitespace
    df = df_load.apply(lambda x: x.str.strip())
    # drop rows of data
    df = df[df['Industry'] == df['Commodity']].reset_index(drop=True)
    # drop columns
    df = df.drop(columns=['Commodity', 'CommodityDescription'])
    # rename columns
    df = df.rename(columns={'Industry': 'ActivityProducedBy',
                            'IndustryDescription': 'Description',
                            'ProVal': 'FlowAmount',
                            'IOYear': 'Year'})
    df.loc[:, 'FlowAmount'] = df['FlowAmount'].astype(float) * 1000000
    # hard code data
    df['Class'] = 'Money'
    df['SourceName'] = 'BEA_Make_Table'
    df['Unit'] = 'USD'
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, args['year'])
    df['FlowName'] = 'Gross Output Producer Value After Redef'
    return df

def subset_BEA_Use(df,attr):
    commodity = attr['clean_parameter']
    df = df.loc[df['ActivityProducedBy']==commodity]
    
    # set column to None to enable generalizing activity column later
    df.loc[:,'ActivityProducedBy'] = None
    
    return df