# USGS_MYB_Fluorspar.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import io
from flowsa.common import *
from string import digits
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.USGS_MYB_Common import *

"""
Projects
/
FLOWSA
/

FLOWSA-314

Import USGS Mineral Yearbook data

Description

Table T1 and T9
SourceName: USGS_MYB_Fluorspar
https://www.usgs.gov/centers/nmic/fluorspar-statistics-and-information

Minerals Yearbook, xls file, tab T1: There is no Production.

Data for: Fluorspar; fluorspar equivalent from phosphate rock

Years = 2013+
"""
SPAN_YEARS = "2013-2017"
SPAN_YEARS_INPORTS = ["2016", "2017"]

def usgs_fluorspar_url_helper(build_url, config, args):
    """Used to substitute in components of usgs urls"""
    # URL Format, replace __year__ and __format__, either xls or xlsx.
    url = build_url
    return [url]


def usgs_fluorspar_call(url, usgs_response, args):
    """Calls the excel sheet for nickel and removes extra columns"""
    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(usgs_response.content), sheet_name='T1')# .dropna()

    if args['year'] in SPAN_YEARS_INPORTS:
        df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(usgs_response.content), sheet_name='T2')
        df_raw_data_three = pd.io.excel.read_excel(io.BytesIO(usgs_response.content), sheet_name='T7')
        df_raw_data_four = pd.io.excel.read_excel(io.BytesIO(usgs_response.content), sheet_name='T8')

    df_data_one = pd.DataFrame(df_raw_data_one.loc[5:9]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]
    if args['year'] in SPAN_YEARS_INPORTS:
        df_data_two = pd.DataFrame(df_raw_data_two.loc[7:8]).reindex()
        df_data_three = pd.DataFrame(df_raw_data_three.loc[19:19]).reindex()
        df_data_four = pd.DataFrame(df_raw_data_four.loc[11:11]).reindex()
        if len(df_data_two.columns) == 13:
            df_data_two.columns = ["Production", "space_1", "not_1", "space_2", "not_2", "space_3", "not_3",
                                   "space_4", "not_4", "space_5", "year_4", "space_6", "year_5"]
        if len(df_data_three.columns) == 9:
            df_data_three.columns = ["Production", "space_1", "year_4", "space_2", "not_1", "space_3", "year_5",
                                     "space_4", "not_2"]
            df_data_four.columns = ["Production", "space_1", "year_4", "space_2", "not_1", "space_3", "year_5",
                                    "space_4", "not_2"]

    if len(df_data_one. columns) == 13:
        df_data_one.columns = ["Production", "space_1",  "unit",  "space_2",  "year_1", "space_3", "year_2",
                               "space_4", "year_3", "space_5", "year_4", "space_6", "year_5"]
    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(SPAN_YEARS, args["year"]))

    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]
    if args['year'] in SPAN_YEARS_INPORTS:
        for col in df_data_two.columns:
            if col not in col_to_use:
                del df_data_two[col]
        for col in df_data_three.columns:
            if col not in col_to_use:
                del df_data_three[col]
        for col in df_data_four.columns:
            if col not in col_to_use:
             del df_data_four[col]

    if args['year'] in SPAN_YEARS_INPORTS:
        # aluminum fluoride
        # cryolite
        df_data_two["type"] = "data_two"
        df_data_three["type"] = "Aluminum Fluoride"
        df_data_four["type"] = "Cryolite"
        frames = [df_data_one, df_data_two, df_data_three, df_data_four]
    else:
        frames = [df_data_one]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data



def usgs_fluorspar_parse(dataframe_list, args):
    """Parsing the USGS data into flowbyactivity format."""
    data = {}
    row_to_use = ["Quantity", "Quantity3", "Total", "Hydrofluoric acid", "Metallurgical"]
    prod = ""
    name = usgs_myb_name(args["source"])
    dataframe = pd.DataFrame()
    for df in dataframe_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Exports:3":
                prod = "exports"
                des = name
            elif df.iloc[index]["Production"].strip() == "Imports for consumption:3":
                prod = "imports"
                des = name

            if str(df.iloc[index]["type"]).strip() == "data_two":
                prod = "imports"
                des = df.iloc[index]["Production"].strip()
            elif str(df.iloc[index]["type"]).strip() == "Aluminum Fluoride" or str(df.iloc[index]["type"]).strip() == "Cryolite":
                prod = "imports"
                des = df.iloc[index]["type"].strip()

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_varaibles()
                data["SourceName"] = args["source"]
                data["Year"] = str(args["year"])
                data["Unit"] = "Metric Tons"
                col_name = usgs_myb_year(SPAN_YEARS, args["year"])
                if str(df.iloc[index][col_name]) == "W":
                    data["FlowAmount"] = withdrawn_keyword
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                data["Description"] = des
                data["ActivityProducedBy"] = name
                data['FlowName'] = name + " " + prod
                dataframe = dataframe.append(data, ignore_index=True)
                dataframe = assign_fips_location_system(dataframe, str(args["year"]))
    return dataframe

