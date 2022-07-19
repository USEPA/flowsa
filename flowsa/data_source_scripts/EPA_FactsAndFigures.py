# EPA_FactsAndFigures.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Scrapes data from EPA's Facts and Figures Data table PDF. Includes
supporting functions.
"""

import io
import tabula
import pandas as pd
import numpy as np
from flowsa.location import US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system, \
    load_fba_w_standardized_units
from flowsa.allocation import \
    proportional_allocation_by_location_and_activity
from flowsa.sectormapping import add_sectors_to_flowbyactivity
from flowsa.validation import compare_df_units


def ff_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    # only pulling table 1 for now, written expecting to import additional
    # tables.

    # create list of tables to import, dictionary of headers and table number
    if year == 2018:
        headers = {"Table 1. Materials Generated* in the Municipal Waste "
                   "Stream, 1960 to 2018": [5]}

    for h in headers:
        pages = headers[h]
        pdf_pages = []
        for page_number in pages:
            pdf_page = tabula.read_pdf(io.BytesIO(resp.content),
                                       pages=page_number,
                                       stream=True,
                                       guess=True)[0]

            if page_number == 5:
                # skip the first few rows and drop nan rows
                pg = pdf_page.loc[2:20]
                #todo: hardcode metals back in
                pg = pg.dropna()
                # assign column headers
                pg.columns = pdf_page.loc[1, ]
                # split column
                pg[['1990', '2000', '2005']] = \
                    pg['1990 2000 2005'].str.split(' ', expand=True)
                pg = pg.drop(columns=['1990 2000 2005'])

            pdf_pages.append(pg)

    df = pd.concat(pdf_pages, ignore_index=True)

    return df


def ff_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # concat list of dataframes (info on each page)
    df = pd.concat(df_list, sort=False)
    # df = df.rename(columns={"I-O code": "ActivityConsumedBy",
    #                         "I-O description": "Description",
    #                         "gal/$M": "FlowAmount",
    #                         })
    # hardcode
    # original data in gal/million usd
    # df.loc[:, 'FlowAmount'] = df['FlowAmount'] / 1000000
    # df['Unit'] = 'gal/USD'
    df['SourceName'] = 'EPA_FactsAndFigures'
    df['Class'] = 'Other'
    # df['FlowName'] = 'Water Withdrawals IO Vector'
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, year)
    df['Year'] = str(year)
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df
