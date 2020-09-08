# write_FBA_Blackhurst_IO_Data.py (scripts)
# !/usr/bin/env python3
# coding=utf-8


"""
Scrapes data from Blackhurst paper 'Direct and Indirect Water Withdrawals for US Industrial Sectors' (Supplemental info)
"""

import tabula
import io
from flowsa.common import *
from flowsa.flowbyactivity import store_flowbyactivity
from flowsa.flowbyfunctions import add_missing_flow_by_fields, flow_by_activity_fields, assign_fips_location_system


# Read pdf into list of DataFrame
def read_pdf_by_page(pages, response):
    df_list = []
    for x in pages:
        df = tabula.read_pdf(io.BytesIO(response.content), pages=x, stream=True)[0]
        df_list.append(df)
    return df_list

def build_blackhurst_FBA():
    # pdf from url
    pdf_url = "https://pubs.acs.org/doi/suppl/10.1021/es903147k/suppl_file/es903147k_si_001.pdf"
    # define pages to extract data from
    pages = range(5, 13)
    # make url request (common.py fxn)
    response = make_http_request(pdf_url)
    # create list of df, extracting data on pages 5-12
    dataframe_list = read_pdf_by_page(pages, response)
    # concat list of dataframes (info on each page)
    df = pd.concat(dataframe_list, sort=False)
    df = df.rename(columns={"I-O code": "ActivityConsumedBy",
                            "I-O description": "Description",
                            "gal/$M": "FlowAmount",
                            })
    # hardcode
    df['Unit'] = 'gal/$M'
    df['SourceName'] = 'Blackhurst_IO_Data'
    df['Class'] = 'Water'
    df['FlowName'] = 'Water Withdrawals IO Vector'
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, '2002')
    df['Year'] = '2002'

    # add missing dataframe fields (also converts columns to desired datatype)
    flow_df = add_missing_flow_by_fields(df, flow_by_activity_fields)
    parquet_name = 'Blackhurst_IO_Data_2002'
    store_flowbyactivity(flow_df, parquet_name)

    return None


def convert_blackhurst_data_to_gallons(df):

    # load the bea make table

    return None
