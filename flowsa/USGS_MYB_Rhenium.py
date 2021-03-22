# USGS_MYB_Rhenium.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import io
from flowsa.common import *
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

Table T1

SourceName: USGS_MYB_Rhenium
https://www.usgs.gov/centers/nmic/lead-statistics-and-information

Minerals Yearbook, xls file, tab T1: SALIENT RHENIUM STATISTICS
data for:


Production, mine, rhenium conten, Total, rhenium content
No export Data.
Years = 2014+
"""
SPAN_YEARS = "2014-2018"

def usgs_rhenium_url_helper(build_url, config, args):
    """Used to substitute in components of usgs urls"""

    url = build_url
    return [url]


def usgs_rhenium_call(url, usgs_response, args):
    """TODO."""
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(usgs_response.content), sheet_name='T1')# .dropna()
    df_data = pd.DataFrame(df_raw_data.loc[5:13]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]

    if len(df_data. columns) == 15:
        df_data.columns = ["Production", "space_1", "year_1", "space_2", "year_2", "space_3", "year_3",
                           "space_4", "year_4", "space_5", "year_5", "space_6", "space_7", "space_8", "space_9"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(SPAN_YEARS, args["year"]))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]
    return df_data


def usgs_rhenium_parse(dataframe_list, args):
    """Parsing the USGS data into flowbyactivity format."""
    data = {}
    row_to_use = ["Total, rhenium content", "Production, mine, rhenium content2"]
    dataframe = pd.DataFrame()
    name = usgs_myb_name(args["source"])
    des = name
    for df in dataframe_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Total, rhenium content":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == "Production, mine, rhenium content2":
                product = "production"
            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_varaibles()
                data["SourceName"] = args["source"]
                data["Year"] = str(args["year"])
                data["Unit"] = "kilograms"
                data['FlowName'] = name + " " + product
                data["Description"] = des
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(SPAN_YEARS, args["year"])
                if str(df.iloc[index][col_name]) == "--":
                    data["FlowAmount"] = str(0)
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = dataframe.append(data, ignore_index=True)
                dataframe = assign_fips_location_system(dataframe, str(args["year"]))
    return dataframe

