# USGS_MYB_Stone_Dimension.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Projects
/
FLOWSA
/

FLOWSA-314

Import USGS Mineral Yearbook data

Description

Table T1
SourceName: USGS_MYB_Stone_Dimension
https://www.usgs.gov/centers/nmic/dimension-stone-statistics-and-information

Minerals Yearbook, xls file, tab T10:
Data for: Stone Dimension

Years = 2013+
There was no data for import or export.
"""
import io
import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.data_source_scripts.USGS_MYB_Common import *

SPAN_YEARS = "2013-2017"


def usgs_stonedis_url_helper(*, build_url, **_):
    """
    This helper function uses the "build_url" input from flowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    :param build_url: string, base url
    :return: list, urls to call, concat, parse, format into Flow-By-Activity
        format
    """

    url = build_url
    return [url]


def usgs_stonedis_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """

    df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')

    df_data_1 = pd.DataFrame(df_raw_data_two.loc[6:9]).reindex()
    df_data_1 = df_data_1.reset_index()
    del df_data_1["index"]

    if len(df_data_1. columns) == 11:
        df_data_1.columns = ["Production", "space_1", "year_1", "space_2",
                             "year_2", "space_3", "year_3", "space_4",
                             "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(SPAN_YEARS, year))
    for col in df_data_1.columns:
        if col not in col_to_use:
            del df_data_1[col]

    return df_data_1


def usgs_stonedis_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity", ]
    dataframe = pd.DataFrame()
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Quantity":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption, value":
                prod = "imports"
            elif df.iloc[index]["Production"].strip() == "Exports, value":
                prod = "exports"
            if df.iloc[index]["Production"].strip() in row_to_use:
                remove_digits = str.maketrans('', '', digits)
                product = df.iloc[index][
                    "Production"].strip().translate(remove_digits)
                data = usgs_myb_static_varaibles()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Thousand Metric Tons"
                col_name = usgs_myb_year(SPAN_YEARS, year)
                data["Description"] = "Stone Dimension"
                data["ActivityProducedBy"] = "Stone Dimension"
                data['FlowName'] = "Stone Dimension " + prod

                data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = dataframe.append(data, ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe
