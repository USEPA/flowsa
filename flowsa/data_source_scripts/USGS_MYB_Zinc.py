# USGS_MYB_Zinc.py (flowsa)
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

Table T1 and T9
SourceName: USGS_MYB_Zinc
https://www.usgs.gov/centers/nmic/zinc-statistics-and-information

Minerals Yearbook, xls file, tab T1 and T9:

Data for: Zinc; mine, zinc in concentrate

Years = 2014+
"""
SPAN_YEARS = "2013-2017"


def usgs_zinc_url_helper(build_url, config, args):
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


def usgs_zinc_call(url, usgs_response, args):
    """
    Convert response for calling url to pandas dataframe, begin parsing df into FBA format
    :param kwargs: url: string, url
    :param kwargs: response_load: df, response from url call
    :param kwargs: args: dictionary, arguments specified when running
        flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(usgs_response.content), sheet_name='T1')# .dropna()
    df_data_two = pd.DataFrame(df_raw_data_two.loc[9:20]).reindex()
    df_data_two = df_data_two.reset_index()
    del df_data_two["index"]

    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(usgs_response.content), sheet_name='T9')  # .dropna()
    df_data_one = pd.DataFrame(df_raw_data_one.loc[53:53]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    if len(df_data_one.columns) > 11:
        for x in range(11, len(df_data_one.columns)):
            col_name = "Unnamed: " + str(x)
            del df_data_one[col_name]



    if len(df_data_two. columns) == 12:
        df_data_two.columns = ["Production",  "unit", "space_1", "year_1", "space_2", "year_2", "space_3",
                               "year_3", "space_4", "year_4", "space_5", "year_5"]
    if len(df_data_one.columns) == 11:
        df_data_one.columns = ["Production", "space_1", "year_1", "space_2", "year_2", "space_3",
                               "year_3", "space_4", "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(SPAN_YEARS, args["year"]))

    for col in df_data_two.columns:
        if col not in col_to_use:
            del df_data_two[col]

    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]

    frames = [df_data_one, df_data_two]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data


def usgs_zinc_parse(dataframe_list, args):
    """
    Combine, parse, and format the provided dataframes
    :param dataframe_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """
    data = {}
    row_to_use = ["Quantity", "Ores and concentrates, zinc content", "United States"]
    import_export = ["Exports:", "Imports for consumption:", "Recoverable zinc:"]
    prod = ""
    name = usgs_myb_name(args["source"])
    dataframe = pd.DataFrame()
    for df in dataframe_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Exports:":
                prod = "exports"
            elif df.iloc[index]["Production"].strip() == "Imports for consumption:":
                prod = "imports"
            elif df.iloc[index]["Production"].strip() == "Recoverable zinc:":
                prod = "production"
            elif df.iloc[index]["Production"].strip() == "United States":
                prod = "production"

            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data = usgs_myb_static_varaibles()
                data["SourceName"] = args["source"]
                data["Year"] = str(args["year"])
                data["Unit"] = "Metric Tons"
                col_name = usgs_myb_year(SPAN_YEARS, args["year"])
                data["FlowAmount"] = str(df.iloc[index][col_name])

                if product.strip() == "Quantity":
                    data["Description"] = "zinc in concentrate"
                    data["ActivityProducedBy"] = "zinc in concentrate "
                    data['FlowName'] = "zinc in concentrate " + prod
                elif product.strip() == "Ores and concentrates, zinc content":
                    data["Description"] = "Ores and concentrates, zinc content"
                    data["ActivityProducedBy"] = "Ores and concentrates, zinc content"
                    data['FlowName'] = "Ores and concentrates, zinc content " + prod
                elif product.strip() == "United States":
                    data["Description"] = "Zinc; Mine"
                    data["ActivityProducedBy"] = name + " " + prod
                    data['FlowName'] = "Zinc; Mine"

                dataframe = dataframe.append(data, ignore_index=True)
                dataframe = assign_fips_location_system(dataframe, str(args["year"]))
    return dataframe

