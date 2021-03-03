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

Table T1 and T9
SourceName: USGS_MYB_Diatomite
https://www.usgs.gov/centers/nmic/diatomite-statistics-and-information

Minerals Yearbook, xls file, tab T1 and T9: 

Data for: Diatomite; diatomite

Years =  2014+ 
"""
def year_name_diatomite(year):
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

def usgs_diatomite_url_helper(build_url, config, args):
    """Used to substitute in components of usgs urls"""
    # URL Format, replace __year__ and __format__, either xls or xlsx.
    url = build_url
    return [url]


def usgs_diatomite_call(url, usgs_response, args):
    """Calls the excel sheet for nickel and removes extra columns"""

    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(usgs_response.content), sheet_name='T1')  # .dropna()
    df_data_one = pd.DataFrame(df_raw_data_one.loc[7:10]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    if len(df_data_one.columns) == 10:
        df_data_one.columns = ["Production", "year_1", "space_2", "year_2", "space_3",
                           "year_3", "space_4", "year_4", "space_5", "year_5"]
    col_to_use = ["Production"]
    col_to_use.append(year_name_diatomite(args["year"]))

    for col in df_data_one.columns:
        if col not in col_to_use:
            del df_data_one[col]

    frames = [df_data_one]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]

    return df_data



def usgs_diatomite_parse(dataframe_list, args):
    """Parsing the USGS data into flowbyactivity format."""
    data = {}
    row_to_use = ["Quantity", "Exports2", "Imports for consumption2"]
    prod = ""
    dataframe = pd.DataFrame()
    for df in dataframe_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Exports2":
                prod = "exports"
            elif df.iloc[index]["Production"].strip() == "Imports for consumption2":
                prod = "imports"
            elif df.iloc[index]["Production"].strip() == "Quantity":
                prod = "production"

            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data["Class"] = "Geological"
                data['FlowType'] = "ELEMENTARY_FLOWS"
                data["Location"] = "00000"
                data["Compartment"] = "ground"
                data["SourceName"] = "USGS_MYB_Diatomite"
                data["Year"] = str(args["year"])
                data["Context"] = None
                data["ActivityConsumedBy"] = None
                data["Unit"] = "Thousand metric tons"
                col_name = year_name_diatomite(args["year"])
                data["FlowAmount"] = str(df.iloc[index][col_name])

                if product.strip() == "Quantity":
                    data["Description"] = "diatomite"
                    data["ActivityProducedBy"] = "diatomite"
                    data['FlowName'] = "diatomite" + prod
                dataframe = dataframe.append(data, ignore_index=True)
                dataframe = assign_fips_location_system(dataframe, str(args["year"]))
    return dataframe

