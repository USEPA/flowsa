# USGS_MYB_Clay.py (flowsa)
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

SourceName: USGS_MYB_Clay
https://www.usgs.gov/centers/nmic/clays-statistics-and-information

Minerals Yearbook, xls file, tab T1

Data for: Clay; Ball Clay, Bentonite, Fire Clay, Fuller's Clay, Kaolin

Years = 2016+
"""
import io
import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.data_source_scripts.USGS_MYB_Common import *

SPAN_YEARS = "2015-2016"


def usgs_clay_url_helper(*, build_url, **_):
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


def usgs_clay_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df_raw_data_ball = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                              sheet_name='T3')
    df_data_ball = pd.DataFrame(df_raw_data_ball.loc[19:19]).reindex()
    df_data_ball = df_data_ball.reset_index()
    del df_data_ball["index"]

    df_raw_data_bentonite = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                                   sheet_name='T4 ')
    df_data_bentonite = pd.DataFrame(
        df_raw_data_bentonite.loc[28:28]).reindex()
    df_data_bentonite = df_data_bentonite.reset_index()
    del df_data_bentonite["index"]

    df_raw_data_common = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                                sheet_name='T5 ')
    df_data_common = pd.DataFrame(df_raw_data_common.loc[40:40]).reindex()
    df_data_common = df_data_common.reset_index()
    del df_data_common["index"]

    df_raw_data_fire = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                              sheet_name='T6 ')
    df_data_fire = pd.DataFrame(df_raw_data_fire.loc[12:12]).reindex()
    df_data_fire = df_data_fire.reset_index()
    del df_data_fire["index"]

    df_raw_data_fuller = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                                sheet_name='T7 ')
    df_data_fuller = pd.DataFrame(df_raw_data_fuller.loc[17:17]).reindex()
    df_data_fuller = df_data_fuller.reset_index()
    del df_data_fuller["index"]

    df_raw_data_kaolin = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                                sheet_name='T8 ')
    df_data_kaolin = pd.DataFrame(df_raw_data_kaolin.loc[18:18]).reindex()
    df_data_kaolin = df_data_kaolin.reset_index()
    del df_data_kaolin["index"]

    df_raw_data_export = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                                sheet_name='T13')
    df_data_export = pd.DataFrame(df_raw_data_export.loc[6:15]).reindex()
    df_data_export = df_data_export.reset_index()
    del df_data_export["index"]

    df_raw_data_import = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                                sheet_name='T14')
    df_data_import = pd.DataFrame(df_raw_data_import.loc[6:13]).reindex()
    df_data_import = df_data_import.reset_index()
    del df_data_import["index"]

    df_data_ball.columns = ["Production", "space_1", "year_1", "space_2",
                            "value_1", "space_3", "year_2", "space_4",
                            "value_2"]
    df_data_bentonite.columns = ["Production", "space_1", "year_1", "space_2",
                                 "value_1", "space_3", "year_2", "space_4",
                                 "value_2"]
    df_data_common.columns = ["Production", "space_1", "year_1", "space_2",
                              "value_1", "space_3", "year_2", "space_4",
                              "value_2"]
    df_data_fire.columns = ["Production", "space_1", "year_1", "space_2",
                            "value_1", "space_3", "year_2", "space_4",
                            "value_2"]
    df_data_fuller.columns = ["Production", "space_1", "year_1", "space_2",
                              "value_1", "space_3", "year_2", "space_4",
                              "value_2"]
    df_data_kaolin.columns = ["Production", "space_1", "year_1", "space_2",
                              "value_1", "space_3", "year_2", "space_4",
                              "value_2"]
    df_data_export.columns = ["Production", "space_1", "year_1", "space_2",
                              "value_1", "space_3", "year_2", "space_4",
                              "value_2", "space_5", "extra"]
    df_data_import.columns = ["Production", "space_1", "year_1", "space_2",
                              "value_1", "space_3", "year_2", "space_4",
                              "value_2", "space_5", "extra"]

    df_data_ball["type"] = "Ball clay"
    df_data_bentonite["type"] = "Bentonite"
    df_data_common["type"] = "Common clay"
    df_data_fire["type"] = "Fire clay"
    df_data_fuller["type"] = "Fuller’s earth"
    df_data_kaolin["type"] = "Kaolin"
    df_data_export["type"] = "export"
    df_data_import["type"] = "import"

    col_to_use = ["Production", "type"]
    col_to_use.append(usgs_myb_year(SPAN_YEARS, year))
    for col in df_data_import.columns:
        if col not in col_to_use:
            del df_data_import[col]
            del df_data_export[col]

    for col in df_data_ball.columns:
        if col not in col_to_use:
            del df_data_ball[col]
            del df_data_bentonite[col]
            del df_data_common[col]
            del df_data_fire[col]
            del df_data_fuller[col]
            del df_data_kaolin[col]

    frames = [df_data_import, df_data_export, df_data_ball, df_data_bentonite,
              df_data_common, df_data_fire, df_data_fuller, df_data_kaolin]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]
    return df_data


def usgs_clay_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Ball clay", "Bentonite", "Fire clay", "Kaolin",
                  "Fuller’s earth", "Total", "Grand total",
                  "Artificially activated clay and earth",
                  "Clays, not elsewhere classified",
                  "Clays, not elsewhere classified"]
    dataframe = pd.DataFrame()
    for df in df_list:
        for index, row in df.iterrows():
            if df.iloc[index]["type"].strip() == "import":
                product = "imports"
            elif df.iloc[index]["type"].strip() == "export":
                product = "exports"
            else:
                product = "production"

            if str(df.iloc[index]["Production"]).strip() in row_to_use:
                data = usgs_myb_static_varaibles()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "Metric Tons"
                if product == "production":
                    data['FlowName'] = \
                        df.iloc[index]["type"].strip() + " " + product
                    data["Description"] = df.iloc[index]["type"].strip()
                    data["ActivityProducedBy"] = df.iloc[index]["type"].strip()
                else:
                    data['FlowName'] = \
                        df.iloc[index]["Production"].strip() + " " + product
                    data["Description"] = df.iloc[index]["Production"].strip()
                    data["ActivityProducedBy"] = \
                        df.iloc[index]["Production"].strip()

                col_name = usgs_myb_year(SPAN_YEARS, year)
                if str(df.iloc[index][col_name]) == "--" or \
                        str(df.iloc[index][col_name]) == "(3)" or \
                        str(df.iloc[index][col_name]) == "(2)":
                    data["FlowAmount"] = str(0)
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = dataframe.append(data, ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe
