# USGS_MYB_ManufacturedAbrasive.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import pandas as pd
import numpy as np
import io
from flowsa.common import *
from string import digits
from flowsa.flowbyfunctions import assign_fips_location_system , fba_default_grouping_fields, aggregator
import math


"""


Projects
/
FLOWSA
/

FLOWSA-224

USGS Silicon Carbide Statistics and Information






Description

Table T2: ESTIMATED PRODUCTION OF CRUDE SILICON CARBIDE AND FUSED ALUMINUM OXIDE IN THE UNITED STATES AND CANAD


Data for: Silicon carbid


SourceName: USGS_MYB_ManufacturedAbrasive
https://www.usgs.gov/centers/nmic/manufactured-abrasives-statistics-and-information 

Minerals Yearbook, xls file, tab T2: 
ESTIMATED PRODUCTION OF CRUDE SILICON CARBIDE AND FUSED ALUMINUM OXIDE IN THE UNITED STATES AND CANADA

Data for: Silicon carbid

Years = 2010+
"""

def year_name(year):
    if int(year) != 2017:
        return_val = "year_1"
    else:
        return_val = "year_2"
    return return_val


def usgs_url_helper(build_url, config, args):
    """Used to substitute in components of usgs urls"""
    # URL Format, replace __year__ and __format__, either xls or xlsx.
    url = build_url
    year = str(args["year"])
    file_2017 = ["2016", "2017"]
    if year in file_2017:
        url = url.replace("__url_text__", config["url_texts"]["2017"])
        url = url.replace("__file_year__", "2017")
        url = url.replace("__format__",  config["formats"]["2017"])
    else:
        year_string = str(int(args["year"]) + 1)
        url = url.replace("__url_text__", config["url_texts"][year_string])
        url = url.replace("__file_year__", year_string)
        url = url.replace("__format__", config["formats"][year_string])
    return [url]


def usgs_call(url, usgs_response, args):
    """TODO."""
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(usgs_response.content), sheet_name='T2')# .dropna()
    df_data = pd.DataFrame(df_raw_data.loc[6:7]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data. columns) == 9:
        df_data.columns = ["Product", "space_1", "quality_year_1", "space_2", "value_year_1", "space_3",
                           "quality_year_2", "space_4", "value_year_2"]
    elif len(df_data. columns) == 13:
        df_data.columns = ["Product", "space_1", "quality_year_1", "space_2", "value_year_1", "space_3",
                           "quality_year_2", "space_4", "value_year_2", "space_5", "space_6", "space_7", "space_8"]

    col_to_use = ["Product"]
    col_to_use.append("quality_" + year_name(args["year"]))
    col_to_use.append("value_" + year_name(args["year"]))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_parse(dataframe_list, args):
    """Parsing the USGS data into flowbyactivity format."""
    data = {}
    row_to_use = ["Silicon carbide"]
    dataframe = pd.DataFrame()
    for df in dataframe_list:
        for index, row in df.iterrows():
            remove_digits = str.maketrans('', '', digits)
            product = df.iloc[index]["Product"].strip().translate(remove_digits)
            if product in row_to_use:
                data["Class"] = "Chemicals"
                data['FlowType'] = "Elementary Flows"
                data["Location"] = "00000"
                data["Compartment"] = " "
                data["SourceName"] = "USGS_MYB_ManufacturedAbrasives"
                data["Year"] = str(args["year"])

                data['FlowName'] = "Silicon carbide"
                data["Context"] = ""
                data["ActivityProducedBy"] = "Silicon carbide"
                data["ActivityConsumedBy"] = None

                for i in range(len(df. columns) - 1):
                    if i == 0:
                        data["Unit"] = "Metric Tons"
                        col_name = "quality_" + year_name(args["year"])
                    else:
                        data["Unit"] = "Thousands"
                        col_name = "value_" + year_name(args["year"])

                    col_name_str = col_name.replace("_", " ")
                    col_name_array = col_name.split("_")
                    data["Description"] = product + " " + col_name_array[0]
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                    dataframe = dataframe.append(data, ignore_index=True)
                    dataframe = assign_fips_location_system(dataframe, str(args["year"]))
    return dataframe

