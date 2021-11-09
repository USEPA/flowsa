# USGS_MYB_Lime.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import io
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.data_source_scripts.USGS_MYB_Common import *

"""
Projects
/
FLOWSA
/

FLOWSA-314

Import USGS Mineral Yearbook data

Description

Table T1

SourceName: USGS_MYB_Lime
https://www.usgs.gov/centers/nmic/lime-statistics-and-information

Minerals Yearbook, xls file, tab T10:
United States, sulfide ore, concentrate


Data for: Lime; lime

Years = 2014+
"""
SPAN_YEARS = "2014-2018"


def usgs_lime_url_helper(build_url, config, args):
    """
    This helper function uses the "build_url" input from flowbyactivity.py, which
    is a base url for data imports that requires parts of the url text string
    to be replaced with info specific to the data year.
    This function does not parse the data, only modifies the urls from which data is obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :param args: dictionary, arguments specified when running flowbyactivity.py
        flowbyactivity.py ('year' and 'source')
    :return: list, urls to call, concat, parse, format into Flow-By-Activity format
    """
    url = build_url
    return [url]


def usgs_lime_call(url, usgs_response, args):
    """
    Convert response for calling url to pandas dataframe, begin parsing df into FBA format
    :param kwargs: url: string, url
    :param kwargs: response_load: df, response from url call
    :param kwargs: args: dictionary, arguments specified when running
        flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    # df_raw_data = pd.io.excel.read_excel(io.BytesIO(usgs_response.content), sheet_name='T10')# .dropna()


    df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(usgs_response.content), sheet_name='T1')  # .dropna()

    df_data_1 = pd.DataFrame(df_raw_data_two.loc[16:16]).reindex()
    df_data_1 = df_data_1.reset_index()
    del df_data_1["index"]

    df_data_2 = pd.DataFrame(df_raw_data_two.loc[28:32]).reindex()
    df_data_2 = df_data_2.reset_index()
    del df_data_2["index"]

    if len(df_data_1.columns) > 12:
        for x in range(12, len(df_data_1.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data_1[col_name]
            del df_data_2[col_name]

    if len(df_data_1. columns) == 12:
        df_data_1.columns = ["Production", "Unit", "space_1", "year_1", "space_2", "year_2", "space_3",
                           "year_3", "space_4", "year_4", "space_5", "year_5"]
        df_data_2.columns =  ["Production", "Unit", "space_1", "year_1", "space_2", "year_2", "space_3",
                           "year_3", "space_4", "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(SPAN_YEARS, args["year"]))
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


def usgs_lime_parse(dataframe_list, args):
    """
    Combine, parse, and format the provided dataframes
    :param dataframe_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """
    data = {}
    row_to_use = ["Total", "Quantity"]
    import_export = ["Exports:7", "Imports for consumption:7"]
    name = usgs_myb_name(args["source"])
    des = name
    dataframe = pd.DataFrame()
    for df in dataframe_list:
        prod = "production"
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Exports:7":
                prod = "exports"
            elif df.iloc[index]["Production"].strip() == "Imports for consumption:7":
                prod = "imports"
            if df.iloc[index]["Production"].strip() in row_to_use:
                remove_digits = str.maketrans('', '', digits)
                product = df.iloc[index]["Production"].strip().translate(remove_digits)
                data = usgs_myb_static_varaibles()
                data["SourceName"] = args["source"]
                data["Year"] = str(args["year"])
                data["Unit"] = "Thousand Metric Tons"
                col_name = usgs_myb_year(SPAN_YEARS, args["year"])
                data["Description"] = des
                data["ActivityProducedBy"] = name
                if product.strip() == "Total":
                    data['FlowName'] = name + " " + prod
                elif product.strip() == "Quantity":
                    data['FlowName'] = name + " " + prod

                data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = dataframe.append(data, ignore_index=True)
                dataframe = assign_fips_location_system(dataframe, str(args["year"]))
    return dataframe

