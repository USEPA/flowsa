# write_Blackhurst_IO_data.py (scripts)
# !/usr/bin/env python3
# coding=utf-8


"""
Scrapes data from Blackhurst paper 'Direct and Indirect Water Withdrawals for US Industrial Sectors' (Supplemental info)
"""

import tabula
import io
from flowsa.common import *

# pdf from url
pdf_url = "https://pubs.acs.org/doi/suppl/10.1021/es903147k/suppl_file/es903147k_si_001.pdf"

# define pages to extract data from
pages = range(5, 13)

# Read pdf into list of DataFrame
def read_pdf_by_page(pages):
    df_list = []
    for x in pages:
        df = tabula.read_pdf(io.BytesIO(response.content), pages=x, stream=True)[0]
        df_list.append(df)
    return df_list

if __name__ == '__main__':
    # make url request (common.py fxn)
    response = make_http_request(pdf_url)
    # create list of df, extracting data on pages 5-12
    dataframe_list = read_pdf_by_page(pages)
    # concat list of dataframes (info on each page)
    df = pd.concat(dataframe_list, sort=False)
    # save data to csv
    df.to_csv(datapath + "Blackhurst_IO.csv", index=False)
