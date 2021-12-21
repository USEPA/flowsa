# USGS_MYB_Zeolites.py (flowsa)
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
SourceName: USGS_MYB_Zeolites
https://www.usgs.gov/centers/nmic/zeolites-statistics-and-information

Minerals Yearbook, xls file, tab T1:

Data for: Zeolites; zeolites, natural
All of the Export and Import values were originally <1,000
The '<' has been striped for the Parquet files.

Years = 2014+
"""
import io
import pandas as pd
from flowsa.common import WITHDRAWN_KEYWORD
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.data_source_scripts.USGS_MYB_Common import *

SPAN_YEARS = "2014-2018"


def usgs_zeolites_url_helper(*, build_url, **_):
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


def usgs_zeolites_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                             sheet_name='T1')
    df_data_one = pd.DataFrame(df_raw_data_one.loc[4:7]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    if len(df_data_one. columns) == 12:
        df_data_one.columns = ["Production", "Unit", "space_2",  "year_1",
                               "space_3", "year_2", "space_4", "year_3",
                               "space_5", "year_4", "space_6", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(SPAN_YEARS, year))

    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]

    frames = [df_data_one]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_zeolites_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Production", "Exportse", "Importse"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(SPAN_YEARS, year)
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Production":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == "Importse":
                prod = "import"
            elif df.iloc[index]["Production"].strip() == "Exportse":
                prod = "export"

            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_varaibles()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                flow_amount = str(df.iloc[index][col_name])
                if str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                flow_amount = flow_amount.replace("<", "")
                flow_amount = flow_amount.replace(",", "")
                data["FlowAmount"] = flow_amount
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = dataframe.append(data, ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe
