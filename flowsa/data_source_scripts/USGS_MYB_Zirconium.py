# USGS_MYB_Zirconium.py (flowsa)
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

SourceName: USGS_MYB_Zirconium
https://www.usgs.gov/centers/nmic/zirconium-and-hafnium-statistics-and-information

Minerals Yearbook, xls file, tab T1

Data for: Zirconium and Hafnium; zirconium, ores and concentrates

Years = 2013+
"""
import io
import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.data_source_scripts.USGS_MYB_Common import *
from flowsa.common import WITHDRAWN_KEYWORD

SPAN_YEARS = "2013-2017"


def usgs_zirconium_url_helper(*, build_url, **_):
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


def usgs_zirconium_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data_one = pd.DataFrame(df_raw_data.loc[6:10]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    df_data_two = pd.DataFrame(df_raw_data.loc[24:24]).reindex()
    df_data_two = df_data_two.reset_index()
    del df_data_two["index"]

    if len(df_data_one.columns) > 11:
        for x in range(11, len(df_data_one.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data_one[col_name]
            del df_data_two[col_name]

    if len(df_data_one. columns) == 11:
        df_data_one.columns = ["Production", "space_1", "year_1", "space_2",
                               "year_2", "space_3", "year_3",
                               "space_4", "year_4", "space_5", "year_5"]
        df_data_two.columns = ["Production", "space_1", "year_1", "space_2",
                               "year_2", "space_3", "year_3",
                               "space_4", "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(SPAN_YEARS, year))
    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]
            del df_data_two[col]

    frames = [df_data_one, df_data_two]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]
    return df_data


def usgs_zirconium_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Imports for consumption3", "Concentrates", "Exports",
                  "Hafnium, unwrought, including powder, "
                  "imports for consumption"]
    dataframe = pd.DataFrame()
    name = usgs_myb_name(source)

    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == \
                    "Imports for consumption3":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == "Concentrates":
                product = "production"
            elif df.iloc[index]["Production"].strip() == "Exports":
                product = "exports"

            if df.iloc[index]["Production"].strip() == \
                    "Hafnium, unwrought, including powder, imports for " \
                    "consumption":
                prod = "imports"
                des = df.iloc[index]["Production"].strip()
            else:
                des = name

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_varaibles()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                data['FlowName'] = name + " " + product
                data["Description"] = des
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(SPAN_YEARS, year)
                if str(df.iloc[index][col_name]) == "--" or \
                        str(df.iloc[index][col_name]) == "(3)":
                    data["FlowAmount"] = str(0)
                elif str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = dataframe.append(data, ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe
