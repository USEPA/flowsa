# USGS_MYB_SodaAsh.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
SourceName: USGS_MYB_SodaAsh
https://www.usgs.gov/centers/nmic/soda-ash-statistics-and-information

Minerals Yearbook, xls file, tab "T4"
REPORTED CONSUMPTION OF SODA ASH IN THE UNITED STATES, BY END USE, BY QUARTER1

tab "T1"
Production Input and exports.

Interested in annual data, not quarterly
Years = 2010+
https://s3-us-west-2.amazonaws.com/prd-wret/assets/palladium/production/mineral-pubs/soda-ash/myb1-2010-sodaa.pdf
"""

import io
import math
from string import digits
import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.data_source_scripts.USGS_MYB_Common import *

SPAN_YEARS = "2013-2017"
SPAN_YEARS_T4 = ["2016", "2017"]

def description(value, code):
    """
    Create string for column based on row description
    :param value: str, description column for a row
    :param code: str, NAICS code
    :return: str, to use as column value
    """
    glass_list = ["Container", "Flat", "Fiber", "Other", "Total"]
    other_list = ["Total domestic consumption4"]
    export_list = ["Canada"]
    return_val = ""
    if value in glass_list:
        return_val = "Glass " + value
        if math.isnan(code):
            return_val = value
        if value == "Total":
            return_val = "Glass " + value
    elif value in other_list:
        return_val = "Other " + value
    elif value in export_list:
        return_val = "Exports " + value
    else:
        return_val = value
    return_val = usgs_myb_remove_digits(return_val)
    return return_val


def soda_url_helper(build_url, config, args):
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


def soda_call(url, response_load, args):
    """
    Convert response for calling url to pandas dataframe, begin parsing df into FBA format
    :param kwargs: url: string, url
    :param kwargs: response_load: df, response from url call
    :param kwargs: args: dictionary, arguments specified when running
        flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    col_to_use = ["Production", "NAICS code", "End use"]
    col_to_use.append(usgs_myb_year(SPAN_YEARS, args["year"]))

    if str(args["year"]) in SPAN_YEARS_T4:
        df_raw_data = pd.io.excel.read_excel(io.BytesIO(response_load.content), sheet_name='T4')# .dropna()
        df_data_one = pd.DataFrame(df_raw_data.loc[7:25]).reindex()
        df_data_one = df_data_one.reset_index()
        del df_data_one["index"]
        if len(df_data_one.columns) == 23:
            df_data_one.columns = ["NAICS code", "space_1", "End use", "space_2", "y1_q1", "space_3", "y1_q2",
                                   "space_4", "y1_q3", "space_5", "y1_q4", "space_6", "year_4", "space_7", "y2_q1",
                                   "space_8", "y2_q2", "space_9", "y2_q3", "space_10", "y2_q4", "space_11", "year_5"]

    df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(response_load.content), sheet_name='T1')  # .dropna()
    df_data_two = pd.DataFrame(df_raw_data_two.loc[6:18]).reindex()
    df_data_two = df_data_two.reset_index()
    del df_data_two["index"]

    if len(df_data_two.columns) == 11:
        df_data_two.columns = ["Production", "space_1", "year_1", "space_2", "year_2", "space_3",
                               "year_3", "space_4", "year_4", "space_5", "year_5"]

    if str(args["year"]) in SPAN_YEARS_T4:
        for col in df_data_one.columns:
            if col not in col_to_use:
                del df_data_one[col]

    for col in df_data_two.columns:
        if col not in col_to_use:
            del df_data_two[col]

    if str(args["year"]) in SPAN_YEARS_T4:
        frames = [df_data_one, df_data_two]
    else:
        frames = [df_data_two]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]
    return df_data


def soda_parse(dataframe_list, args):
    """
    Combine, parse, and format the provided dataframes
    :param dataframe_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """
    total_glass = 0

    data = {}
    row_to_use = ["Quantity", "Quantity2"]
    prod = ""
    name = usgs_myb_name(args["source"])
    des = name
    col_name = usgs_myb_year(SPAN_YEARS, args["year"])
    dataframe = pd.DataFrame()
    for df in dataframe_list:
        for index, row in df.iterrows():
            data = usgs_myb_static_varaibles()
            data["Unit"] = "Thousand metric tons"
            data["FlowAmount"] = str(df.iloc[index][col_name])
            data["SourceName"] = args["source"]
            data["Year"] = str(args["year"])
            data['FlowName'] = name

            if str(df.iloc[index]["Production"]) != "nan":
                des = name
                if df.iloc[index]["Production"].strip() == "Exports:":
                    prod = "exports"
                elif df.iloc[index]["Production"].strip() == "Imports for consumption:":
                    prod = "imports"
                elif df.iloc[index]["Production"].strip() == "Production:":
                    prod = "production"
                if df.iloc[index]["Production"].strip() in row_to_use:
                    product = df.iloc[index]["Production"].strip()
                    data["SourceName"] = args["source"]
                    data["Year"] = str(args["year"])
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                    if str(df.iloc[index][col_name]) == "W":
                        data["FlowAmount"] = withdrawn_keyword
                    data["Description"] = des
                    data["ActivityProducedBy"] = name
                    data['FlowName'] = name + " " + prod
                    dataframe = dataframe.append(data, ignore_index=True)
                    dataframe = assign_fips_location_system(dataframe, str(args["year"]))
            else:
                data["Class"] = "Chemicals"
                data["Context"] = None
                data["Compartment"] = "air"
                data["Description"] = ""
                data['ActivityConsumedBy'] = description(df.iloc[index]["End use"], df.iloc[index]["NAICS code"])
                data['FlowName'] = name  + " " + description(df.iloc[index]["End use"], df.iloc[index]["NAICS code"])
                if df.iloc[index]["End use"].strip() == "Glass:":
                    total_glass = int(df.iloc[index]["NAICS code"])
                elif data['ActivityConsumedBy'] == "Glass Total":
                    data["Description"] = total_glass

                if not math.isnan(df.iloc[index][col_name]):
                    data["FlowAmount"] = int(df.iloc[index][col_name])
                    data["ActivityProducedBy"] = None
                    if not math.isnan(df.iloc[index]["NAICS code"]):
                        des_str = str(df.iloc[index]["NAICS code"])
                        data["Description"] = des_str
                if df.iloc[index]["End use"].strip() != "Glass:":
                    dataframe = dataframe.append(data, ignore_index=True)
                    dataframe = assign_fips_location_system(dataframe, str(args["year"]))
    return dataframe
