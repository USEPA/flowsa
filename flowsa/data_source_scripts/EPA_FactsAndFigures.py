# EPA_FactsAndFigures.py (scripts)
# !/usr/bin/env python3
# coding=utf-8
"""
Scrapes data from EPA's Facts and Figures Data table PDF. Includes
supporting functions.
"""

import io
from tabula.io import read_pdf
import pandas as pd
import numpy as np
from flowsa.location import US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system


def ff_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """

    if year == '2018':
        pages = [6, 8, 9]
    pdf_pages = []
    for page_number in pages:
        pdf_page = read_pdf(io.BytesIO(resp.content),
                            pages=page_number,
                            stream=True,
                            guess=True)[0]
        if page_number == 6:
            # skip the first few rows
            pg = pdf_page.loc[2:33].reset_index(drop=True)
            # assign column headers
            pg.columns = pdf_page.loc[0, ]
            pg.columns.values[0] = "FlowName"
            pg['FlowName'] = pg['FlowName'].str.replace("–", "-")
            # split column
            pg[['2000', '2005']] = \
                pg['2000 2005'].str.split(' ', expand=True)
            pg = pg.drop(columns=['2000 2005'])
            # manually address errors generated in df generation - correct 2018
            # values for other food management
            pg.loc[24, "2018"] = "1840"
            pg.loc[26, "2018"] = "5260"
            pg.loc[30, "2018"] = "3740"
            # drop rows with na for 2018
            pg = pg.dropna(subset=['2018']).reset_index(drop=True)
            # assign activity based on location in data table
            pg.loc[0:11, "ActivityConsumedBy"] = "Recycled"
            pg.loc[12:15, "ActivityConsumedBy"] = "Composted"
            pg.loc[16:21, "ActivityConsumedBy"] = pg[
                "FlowName"].str.replace("Food - ", '')
            pg["ActivityConsumedBy"] = pg["ActivityConsumedBy"].str.title()
            pg['FlowName'] = pg['FlowName'].str.replace(
                "( -).*", "", regex=True)
            # melt df and rename cols to standardize before merging with
            # additional tables
            pg = pg.melt(id_vars=["FlowName", "ActivityConsumedBy"],
                         var_name="Year", value_name="FlowAmount")
            pg["Description"] = "Table 2. Materials Recycled, Composted and " \
                                "Managed by Other Food Pathways in the " \
                                "Municipal Waste Stream"

        if page_number in [8, 9]:
            # skip the first few rows
            pg = pdf_page.loc[2:19].reset_index(drop=True)
            # assign column headers
            pg.columns = pdf_page.loc[1, ]
            pg.columns.values[0] = "FlowName"
            # split column
            pg[['2000', '2005', '2010']] = \
                pg['2000 2005 2010'].str.split(' ', expand=True)
            pg = pg.drop(columns=['2000 2005 2010'])
            pg = pg.dropna(subset=['2018']).reset_index(drop=True)
            # melt df and rename cols to standardize before merging with
            # additional tables
            pg = pg.melt(id_vars="FlowName", var_name="Year",
                         value_name="FlowAmount")
            pg = pg.dropna(subset=["FlowAmount"]).reset_index(drop=True)
            if page_number == 8:
                pg["ActivityConsumedBy"] = "Combusted with Energy Recovery"
                pg["Description"] = "Table 3. Materials Combusted with " \
                                    "Energy Recovery* in the Municipal " \
                                    "Waste Stream"
            if page_number == 9:
                pg["ActivityConsumedBy"] = "Landfilled"
                pg["Description"] = "Table 4. Materials Landfilled in the " \
                                    "Municipal Waste Stream"
        # following code used for page 6, 9
        # drop nas and harcode metals and inorganic wastes back in
        pg["FlowName"] = np.where(pg["FlowName"].str.contains(
                "Ferrous|Aluminum|Other Nonferrous"),
            'Metals, ' + pg["FlowName"], pg["FlowName"])
        pg["FlowName"] = np.where(
            pg["FlowName"] == "Wastes",
            "Miscellaneous Inorganic " + pg["FlowName"], pg["FlowName"])
        # Revise Activity names
        pg["ActivityConsumedBy"] = np.where(
            pg["ActivityConsumedBy"] == "Bio-Based",
            "Bio-Based Materials/Biochemical Processing",
            pg["ActivityConsumedBy"])
        pg["ActivityConsumedBy"] = np.where(
            pg["ActivityConsumedBy"] == "Codigestion/Anaerobic",
            "Codigestion/Anaerobic Digestion", pg["ActivityConsumedBy"])
        pg["ActivityConsumedBy"] = np.where(
            pg["ActivityConsumedBy"] == "Sewer/Wastewater",
            "Sewer/Wastewater Treatment", pg["ActivityConsumedBy"])
        # drop rows with totals to avoid duplication
        pg = pg[~pg["FlowName"].str.contains('Total')].reset_index(
            drop=True)
        pg['Unit'] = "Thousands of Tons"

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
    df["FlowName"] = df["FlowName"].str.replace('[^a-zA-Z0-9, ]', '',
                                                regex=True)
    # strip trailing white spaces
    df["FlowName"] = df["FlowName"].str.strip()
    df['SourceName'] = 'EPA_FactsAndFigures'
    df['Class'] = 'Other'
    df['FlowType'] = "WASTE_FLOW"
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, year)
    df['Year'] = str(year)
    df["FlowAmount"] = df["FlowAmount"].str.replace(',', '', regex=True)
    # Facts and Figures defines "Neg." as "Less than 5,000 tons or 0.05
    # percent," so replace with 0
    df["FlowAmount"] = df["FlowAmount"].str.replace('Neg.', '0', regex=True)
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df
