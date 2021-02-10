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


SourceName: USGS_MYB_ManufacturedAbrasive
https://www.usgs.gov/centers/nmic/copper-statistics-and-information

Minerals Yearbook, xls file, tab T1: 
ESTIMATED PRODUCTION OF CRUDE SILICON CARBIDE AND FUSED ALUMINUM OXIDE IN THE UNITED STATES AND CANADA

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
    df_data = pd.DataFrame(df_raw_data.loc[12:12]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data. columns) == 12:
        df_data.columns = ["Production", "Unit", "space_1", "year_1", "space_2", "year_2", "space_3",
                           "year_3", "space_4", "year_4", "space_5", "year_5"]
    #elif len(df_data. columns) == 13:
    #    df_data.columns = ["Product", "space_1", "quality_year_1", "space_2", "value_year_1", "space_3",
    #                       "quality_year_2", "space_4", "value_year_2", "space_5", "space_6", "space_7", "space_8"]

    col_to_use = ["Production", "Unit"]
    col_to_use.append(year_name_copper(args["year"]))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_copper_parse(dataframe_list, args):
    """Parsing the USGS data into flowbyactivity format."""
    data = {}
    row_to_use = ["Silicon carbide"]
    dataframe = pd.DataFrame()
    for df in dataframe_list:
        for index, row in df.iterrows():
            remove_digits = str.maketrans('', '', digits)
            product = df.iloc[index]["Production"].strip().translate(remove_digits)

            data["Class"] = "Geological"
            data['FlowType'] = "Elementary Flows"
            data["Location"] = "00000"
            data["Compartment"] = " "
            data["SourceName"] = "USGS_MYB_Copper"
            data["Year"] = str(args["year"])

            data['FlowName'] = "Copper; Mine"
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

