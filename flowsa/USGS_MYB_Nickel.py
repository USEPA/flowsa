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


SourceName: USGS_MYB_Nickel
https://www.usgs.gov/centers/nmic/nickel-statistics-and-information

Minerals Yearbook, xls file, tab T10: 
United States, sulfide ore, concentrate


Data for: Nickel; mine

Years = 2014+
"""
def year_name_nickel(year):
    if int(year) == 2012:
        return_val = "year_1"
    elif int(year) == 2013:
        return_val = "year_2"
    elif int(year) == 2014:
        return_val = "year_3"
    elif int(year) == 2015:
        return_val = "year_4"
    elif int(year) == 2016:
        return_val = "year_5"
    return return_val

def usgs_nickel_url_helper(build_url, config, args):
    """Used to substitute in components of usgs urls"""
    # URL Format, replace __year__ and __format__, either xls or xlsx.
    url = build_url
    return [url]


def usgs_nickel_call(url, usgs_response, args):
    """Calls the excel sheet for nickel and removes extra columns"""
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(usgs_response.content), sheet_name='T10')# .dropna()
    df_data = pd.DataFrame(df_raw_data.loc[36:36]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data. columns) == 12:
        df_data.columns = ["Production", "space_1", "year_1", "space_2", "year_2", "space_3",
                           "year_3", "space_4", "year_4", "space_5", "year_5", "space_6"]

    col_to_use = ["Production"]
    col_to_use.append(year_name_nickel(args["year"]))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_nickel_parse(dataframe_list, args):
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
            data["SourceName"] = "USGS_MYB_Nickel"
            data["Year"] = str(args["year"])

            data['FlowName'] = "Nickel; Mine"
            data["Context"] = None
            data["ActivityProducedBy"] = "Nickel; Mine"
            data["ActivityConsumedBy"] = None
            data["Unit"] = "Metric Tons"
            col_name = year_name_nickel(args["year"])

            data["Description"] = product
            data["FlowAmount"] = str(df.iloc[index][col_name])
            dataframe = dataframe.append(data, ignore_index=True)
            dataframe = assign_fips_location_system(dataframe, str(args["year"]))
    return dataframe

