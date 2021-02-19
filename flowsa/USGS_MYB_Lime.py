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
def year_name_lime(year):
    if int(year) == 2014:
        return_val = "year_1"
    elif int(year) == 2015:
        return_val = "year_2"
    elif int(year) == 2016:
        return_val = "year_3"
    elif int(year) == 2017:
        return_val = "year_4"
    elif int(year) == 2018:
        return_val = "year_5"
    return return_val

def usgs_lime_url_helper(build_url, config, args):
    """Used to substitute in components of usgs urls"""
    # URL Format, replace __year__ and __format__, either xls or xlsx.
    url = build_url
    return [url]


def usgs_lime_call(url, usgs_response, args):
    """Calls the excel sheet for nickel and removes extra columns"""
 #   df_raw_data = pd.io.excel.read_excel(io.BytesIO(usgs_response.content), sheet_name='T10')# .dropna()


    df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(usgs_response.content), sheet_name='T1')  # .dropna()

    df_data_1 = pd.DataFrame(df_raw_data_two.loc[16:16]).reindex()
    df_data_1 = df_data_1.reset_index()
    del df_data_1["index"]

    df_data_2 = pd.DataFrame(df_raw_data_two.loc[28:32]).reindex()
    df_data_2 = df_data_2.reset_index()
    del df_data_2["index"]

    if len(df_data_1. columns) == 16:
        df_data_1.columns = ["Production", "Unit", "space_1", "year_1", "space_2", "year_2", "space_3",
                           "year_3", "space_4", "year_4", "space_5", "year_5", "space_6", "space_7", "space_8",
                           "space_9"]
        df_data_2.columns =  ["Production", "Unit", "space_1", "year_1", "space_2", "year_2", "space_3",
                           "year_3", "space_4", "year_4", "space_5", "year_5", "space_6", "space_7", "space_8",
                           "space_9"]

    col_to_use = ["Production"]
    col_to_use.append(year_name_lime(args["year"]))
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
    """Parsing the USGS data into flowbyactivity format."""
    data = {}
    row_to_use = ["Total", "Quantity"]
    import_export = ["Exports:7", "Imports for consumption:7"]
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

                data["Class"] = "Geological"
                data['FlowType'] = "ELEMENTARY_FLOWS"
                data["Location"] = "00000"
                data["Compartment"] = "ground"
                data["SourceName"] = "USGS_MYB_Lime"
                data["Year"] = str(args["year"])

                data["Context"] = None
                data["ActivityConsumedBy"] = None
                data["Unit"] = "Thousand Metric Tons"
                col_name = year_name_lime(args["year"])
                data["Description"] = "Lime"
                data["ActivityProducedBy"] = "Lime"
                if product.strip() == "Total":
                    data['FlowName'] = "Lime " + prod
                elif product.strip() == "Quantity":
                    data['FlowName'] = "Lime " + prod

                data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = dataframe.append(data, ignore_index=True)
                dataframe = assign_fips_location_system(dataframe, str(args["year"]))
    return dataframe

