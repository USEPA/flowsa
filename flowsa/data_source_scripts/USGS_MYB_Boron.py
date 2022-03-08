# USGS_MYB_Boron.py (flowsa)
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

SourceName: USGS_MYB_Boron
https://www.usgs.gov/centers/nmic/boron-statistics-and-information

Minerals Yearbook, xls file, tab T1

Data for: Boron

Years = 2014+
"""
import io
import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.data_source_scripts.USGS_MYB_Common import *
from flowsa.common import WITHDRAWN_KEYWORD

SPAN_YEARS = "2014-2018"


def usgs_boron_url_helper(*, build_url, **_):
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


def usgs_boron_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data_one = pd.DataFrame(df_raw_data.loc[8:8]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    df_data_two = pd.DataFrame(df_raw_data.loc[21:22]).reindex()
    df_data_two = df_data_two.reset_index()
    del df_data_two["index"]

    df_data_three = pd.DataFrame(df_raw_data.loc[27:28]).reindex()
    df_data_three = df_data_three.reset_index()
    del df_data_three["index"]

    if len(df_data_one. columns) == 11:
        df_data_one.columns = ["Production", "space_1", "year_1", "space_2",
                               "year_2", "space_3", "year_3", "space_4",
                               "year_4", "space_5", "year_5"]
        df_data_two.columns = ["Production", "space_1", "year_1", "space_2",
                               "year_2", "space_3", "year_3", "space_4",
                               "year_4", "space_5", "year_5"]
        df_data_three.columns = ["Production", "space_1", "year_1", "space_2",
                                 "year_2", "space_3", "year_3", "space_4",
                                 "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(SPAN_YEARS, year))
    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]
            del df_data_two[col]
            del df_data_three[col]

    frames = [df_data_one, df_data_two, df_data_three]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]
    return df_data


def usgs_boron_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["B2O3 content", "Quantity"]
    prod = ""
    name = usgs_myb_name(source)
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(SPAN_YEARS, year)

    for df in df_list:
        for index, row in df.iterrows():

            if df.iloc[index]["Production"].strip() == "B2O3 content" or \
                    df.iloc[index]["Production"].strip() == "Quantity":
                product = "production"

            if df.iloc[index]["Production"].strip() == "Colemanite:4":
                des = "Colemanite"
            elif df.iloc[index]["Production"].strip() == "Ulexite:4":
                des = "Ulexite"

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_varaibles()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                if des == name:
                    data['FlowName'] = name + " " + product
                else:
                    data['FlowName'] = name + " " + product + " " + des
                data["Description"] = des
                data["ActivityProducedBy"] = name
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
