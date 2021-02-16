# USGS_MYB_Copper.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import io
from flowsa.common import *
from string import digits
from flowsa.flowbyfunctions import assign_fips_location_system


"""


Projects
/
FLOWSA
/

FLOWSA-224

USGS Silicon Carbide Statistics and Information






Description

Table T1


Data for: Copper Mine


SourceName: USGS_MYB_Copper
https://www.usgs.gov/centers/nmic/copper-statistics-and-information

Minerals Yearbook, xls file, tab T1: 


Data for: Copper; mine

Years = 2010+
"""

def year_name_copper(year):
    if int(year) < 2012:
        return_val = "year_1"
    elif int(year) == 2012:
        return_val = "year_2"
    elif int(year) == 2013:
        return_val = "year_3"
    elif int(year) == 2014:
        return_val = "year_4"
    elif int(year) == 2015:
        return_val = "year_5"
    return return_val


def usgs_copper_url_helper(build_url, config, args):
    """Used to substitute in components of usgs urls"""
    # URL Format, replace __year__ and __format__, either xls or xlsx.
    url = build_url
    year = str(args["year"])
    last_file = ["2011", "2012", "2013", "2014", "2015"]
    if year in last_file:
        url = url.replace("__file_year__", "2015")
        url = url.replace("__format__",  config["formats"]["2015"])
    else:
        year_string = str(int(args["year"]) + 4)
        url = url.replace("__file_year__", year_string)
        url = url.replace("__format__", config["formats"][year_string])
    return [url]


def usgs_copper_call(url, usgs_response, args):
    """TODO."""
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(usgs_response.content), sheet_name='T1')# .dropna()
    df_data_1 = pd.DataFrame(df_raw_data.loc[12:12]).reindex()
    df_data_1 = df_data_1.reset_index()
    del df_data_1["index"]

    df_data_2 = pd.DataFrame(df_raw_data.loc[30:31]).reindex()
    df_data_2 = df_data_2.reset_index()
    del df_data_2["index"]

    if len(df_data_1. columns) == 12:
        df_data_1.columns = ["Production", "Unit", "space_1", "year_1", "space_2", "year_2", "space_3",
                           "year_3", "space_4", "year_4", "space_5", "year_5"]
        df_data_2.columns = ["Production", "Unit", "space_1", "year_1", "space_2", "year_2", "space_3",
                           "year_3", "space_4", "year_4", "space_5", "year_5"]

    col_to_use = ["Production", "Unit"]
    col_to_use.append(year_name_copper(args["year"]))
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


def usgs_copper_parse(dataframe_list, args):
    """Parsing the USGS data into flowbyactivity format."""
    data = {}
    dataframe = pd.DataFrame()
    for df in dataframe_list:
        for index, row in df.iterrows():
            remove_digits = str.maketrans('', '', digits)
            product = df.iloc[index]["Production"].strip().translate(remove_digits)

            data["Class"] = "Geological"
            data['FlowType'] = "ELEMENTARY_FLOWS"
            data["Location"] = "00000"
            data["Compartment"] = "ground"
            data["SourceName"] = "USGS_MYB_Copper"
            data["Year"] = str(args["year"])
            if product == "Total":
                data['FlowName'] = "production"
            elif product == "Exports, refined":
                data['FlowName'] = "exports"
            elif product == "Imports, refined":
                data['FlowName'] = "imports"
            data["Context"] = None
            data["ActivityProducedBy"] = "Copper; Mine"
            data["ActivityConsumedBy"] = None
            data["Unit"] = "Metric Tons"
            col_name = year_name_copper(args["year"])
            data["Description"] = "Copper; Mine"
            data["FlowAmount"] = str(df.iloc[index][col_name])
            dataframe = dataframe.append(data, ignore_index=True)
            dataframe = assign_fips_location_system(dataframe, str(args["year"]))
    return dataframe

