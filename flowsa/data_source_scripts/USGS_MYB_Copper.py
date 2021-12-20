# USGS_MYB_Copper.py (flowsa)
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

SourceName: USGS_MYB_Copper
https://www.usgs.gov/centers/nmic/copper-statistics-and-information

Minerals Yearbook, xls file, tab T1:

Data for: Copper; mine

Years = 2010+
"""
import io
import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.data_source_scripts.USGS_MYB_Common import *

SPAN_YEARS = "2011-2015"


def usgs_copper_url_helper(*, build_url, **_):
    """
    This helper function uses the "build_url" input from flowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :param args: dictionary, arguments specified when running flowbyactivity.py
        flowbyactivity.py ('year' and 'source')
    :return: list, urls to call, concat, parse, format into Flow-By-Activity
        format
    """
    url = build_url
    return [url]


def usgs_copper_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin
    parsing df into FBA format
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data_1 = pd.DataFrame(df_raw_data.loc[12:12]).reindex()
    df_data_1 = df_data_1.reset_index()
    del df_data_1["index"]

    df_data_2 = pd.DataFrame(df_raw_data.loc[30:31]).reindex()
    df_data_2 = df_data_2.reset_index()
    del df_data_2["index"]

    if len(df_data_1. columns) == 12:
        df_data_1.columns = ["Production", "Unit", "space_1", "year_1",
                             "space_2", "year_2", "space_3", "year_3",
                             "space_4", "year_4", "space_5", "year_5"]
        df_data_2.columns = ["Production", "Unit", "space_1", "year_1",
                             "space_2", "year_2", "space_3", "year_3",
                             "space_4", "year_4", "space_5", "year_5"]

    col_to_use = ["Production", "Unit"]
    col_to_use.append(usgs_myb_year(SPAN_YEARS, year))
    for col in df_data_1.columns:
        if col not in col_to_use:
            del df_data_1[col]
    for col in df_data_2.columns:
        if col not in col_to_use:
            del df_data_2[col]
    frames = [df_data_1, df_data_2]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]
    return df_data


def usgs_copper_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    for df in df_list:
        for index, row in df.iterrows():
            remove_digits = str.maketrans('', '', digits)
            product = df.iloc[index][
                "Production"].strip().translate(remove_digits)
            data = usgs_myb_static_varaibles()
            data["SourceName"] = source
            data["Year"] = str(year)
            if product == "Total":
                prod = "production"
            elif product == "Exports, refined":
                prod = "exports"
            elif product == "Imports, refined":
                prod = "imports"

            data["ActivityProducedBy"] = "Copper; Mine"
            data['FlowName'] = name + " " + prod
            data["Unit"] = "Metric Tons"
            col_name = usgs_myb_year(SPAN_YEARS, year)
            data["Description"] = "Copper; Mine"
            data["FlowAmount"] = str(df.iloc[index][col_name])
            dataframe = dataframe.append(data, ignore_index=True)
            dataframe = assign_fips_location_system(
                dataframe, str(year))
    return dataframe
