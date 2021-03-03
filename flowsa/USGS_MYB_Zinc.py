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
SourceName: USGS_MYB_Zinc
https://www.usgs.gov/centers/nmic/zinc-statistics-and-information

Minerals Yearbook, xls file, tab T1 and T9: 

Data for: Zinc; mine, zinc in concentrate

Years = 2014+
"""
def year_name_zinc(year):
    if int(year) == 2013:
        return_val = "year_1"
    elif int(year) == 2014:
        return_val = "year_2"
    elif int(year) == 2015:
        return_val = "year_3"
    elif int(year) == 2016:
        return_val = "year_4"
    elif int(year) == 2017:
        return_val = "year_5"
    return return_val

def usgs_zinc_url_helper(build_url, config, args):
    """Used to substitute in components of usgs urls"""
    # URL Format, replace __year__ and __format__, either xls or xlsx.
    url = build_url
    return [url]


def usgs_zinc_call(url, usgs_response, args):
    """Calls the excel sheet for nickel and removes extra columns"""
    df_raw_data_two = pd.io.excel.read_excel(io.BytesIO(usgs_response.content), sheet_name='T1')# .dropna()
    df_data_two = pd.DataFrame(df_raw_data_two.loc[9:20]).reindex()
    df_data_two = df_data_two.reset_index()
    del df_data_two["index"]

    df_raw_data_one = pd.io.excel.read_excel(io.BytesIO(usgs_response.content), sheet_name='T9')  # .dropna()
    df_data_one = pd.DataFrame(df_raw_data_one.loc[53:53]).reindex()
    df_data_one = df_data_one.reset_index()
    del df_data_one["index"]

    if len(df_data_two. columns) == 12:
        df_data_two.columns = ["Production",  "unit", "space_1", "year_1", "space_2", "year_2", "space_3",
                           "year_3", "space_4", "year_4", "space_5", "year_5",]
    if len(df_data_one.columns) == 12:
        df_data_one.columns = ["Production", "space_1", "year_1", "space_2", "year_2", "space_3",
                           "year_3", "space_4", "year_4", "space_5", "year_5", "space_6"]
    col_to_use = ["Production"]
    col_to_use.append(year_name_zinc(args["year"]))

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
    """Parsing the USGS data into flowbyactivity format."""
    data = {}
    row_to_use = ["Quantity", "Ores and concentrates, zinc content", "United States"]
    import_export = ["Exports:", "Imports for consumption:", "Recoverable zinc:"]
    prod = ""
    dataframe = pd.DataFrame()
    for df in dataframe_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Exports:":
                prod = "exports"
            elif df.iloc[index]["Production"].strip() == "Imports for consumption:":
                prod = "imports"
            elif df.iloc[index]["Production"].strip() == "Recoverable zinc:":
                prod = "production"

            if df.iloc[index]["Production"].strip() in row_to_use:
                product = df.iloc[index]["Production"].strip()
                data["Class"] = "Geological"
                data['FlowType'] = "ELEMENTARY_FLOWS"
                data["Location"] = "00000"
                data["Compartment"] = "ground"
                data["SourceName"] = "USGS_MYB_Zinc"
                data["Year"] = str(args["year"])
                data["Context"] = None
                data["ActivityConsumedBy"] = None
                data["Unit"] = "Metric Tons"
                col_name = year_name_zinc(args["year"])
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
                    data["ActivityProducedBy"] = "Zinc; Mine"

                dataframe = dataframe.append(data, ignore_index=True)
                dataframe = assign_fips_location_system(dataframe, str(args["year"]))
    return dataframe

