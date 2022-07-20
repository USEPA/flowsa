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

    if year == '2018':
        pages = [9]
    pdf_pages = []
    for page_number in pages:
        pdf_page = tabula.read_pdf(io.BytesIO(resp.content),
                                   pages=page_number,
                                   stream=True,
                                   guess=True)[0]

        if page_number == 9:
            # skip the first few rows
            pg = pdf_page.loc[2:19].reset_index(drop=True)
            # assign column headers
            pg.columns = pdf_page.loc[1, ]
            pg.columns.values[0] = "ActivityProducedBy"
            # split column
            pg[['2000', '2005', '2010']] = \
                pg['2000 2005 2010'].str.split(' ', expand=True)
            pg = pg.drop(columns=['2000 2005 2010'])
            # drop nas and harcode metals and inorganic wastes back in
            pg['ActivityProducedBy'] = np.where(
                pg['ActivityProducedBy'].str.contains(
                    "Ferrous|Aluminum|Other Nonferrous"),
                'Metals, ' + pg['ActivityProducedBy'],
                pg['ActivityProducedBy'])
            pg['ActivityProducedBy'] = np.where(
                pg['ActivityProducedBy'] == 'Wastes',
                'Miscellaneous Inorganic ' + pg['ActivityProducedBy'],
                pg['ActivityProducedBy'])
            pg = pg.dropna()
            # melt df and rename cols to standardize before merging with
            # additional tables
            pg = pg.melt(id_vars="ActivityProducedBy", var_name="Year",
                         value_name="FlowAmount")
            pg['Unit'] = "Thousands of Tons"
            pg["ActivityConsumedBy"] = "Landfill"
            pg['FlowName'] = 'Materials Landfilled'
            pg["Description"] = "Table 4. Materials Landfilled in the " \
                                "Municipal Waste Stream"
            # drop rows with totals to avoid duplication
            pg = pg[~pg["ActivityProducedBy"].str.contains(
                'Total')].reset_index(drop=True)
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
    # subset by df
    df = df[df["Year"] == year]
    # remove non alphanumeric characters
    df["ActivityProducedBy"] = df["ActivityProducedBy"].str.replace(
        '[^a-zA-Z0-9, ]', '', regex=True)
    df['SourceName'] = 'EPA_FactsAndFigures'
    df['Class'] = 'Other'
    df['FlowType'] = "WASTE_FLOW"
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, year)
    df['Year'] = str(year)
    df["FlowAmount"] = df["FlowAmount"].str.replace(',', '', regex=True)
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df
