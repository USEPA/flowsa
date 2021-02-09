# USGS_MYB_SodaAsh.py (flowsa)
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
SourceName: USGS_MYB_SodaAsh
https://www.usgs.gov/centers/nmic/soda-ash-statistics-and-information

Minerals Yearbook, xls file, tab "T4"
REPORTED CONSUMPTION OF SODA ASH IN THE UNITED STATES, BY END USE, BY QUARTER1

Interested in annual data, not quarterly
Years = 2010+
https://s3-us-west-2.amazonaws.com/prd-wret/assets/palladium/production/mineral-pubs/soda-ash/myb1-2010-sodaa.pdf
"""

def description(value, code):
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
    remove_digits = str.maketrans('', '', digits)
    return_val = return_val.translate(remove_digits)
    return return_val


def usgs_url_helper(build_url, config, args):
    """Used to substitute in components of usgs urls"""
    # URL Format, replace __year__ and __format__, either xls or xlsx.
    url = build_url
    year = str(args["year"])
    url = url.replace("__url_text__", config["url_texts"][year])
    url = url.replace("__year__", year)
    url = url.replace("__format__", config["formats"][year])
    return [url]


def usgs_call(url, usgs_response, args):
    """TODO."""
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(usgs_response.content), sheet_name='T4')# .dropna()
    df_data = pd.DataFrame(df_raw_data.loc[7:25]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]
    df_data.columns = ["NAICS code", "space_1", "End use", "space_2", "2009", "space_3","First quarter", "space_4",
                       "Second quarter", "space_5", "Third quarter", "space_6", "Fourth quarter", "space_7", "Total"]
    for col in df_data.columns:
        if "space_" in str(col):
            del df_data[col]
        elif "quarter" in str(col):
            del df_data[col]
        elif "2009" in str(col):
            del df_data[col]
    return df_data


def usgs_parse(dataframe_list, args):
    total_glass = 0
    """Parsing the USGS data into flowbyactivity format."""
    data = {}
    dataframe = pd.DataFrame()
    for df in dataframe_list:


        data["Class"] = "Chemicals"
        data['FlowType'] = "Elementary Type"
        data["Location"] = "00000"
        data["Compartment"] = " "
        data["SourceName"] = "USGS_MYB_SodaAsh"
        data["Year"] = str(args["year"])
        data["Unit"] = "Thousand metric tons"
        data['FlowName'] = "Soda Ash"
        data["Context"] = "air"

        for index, row in df.iterrows():
            data["Description"] = ""
            data['ActivityConsumedBy'] = description(df.iloc[index]["End use"], df.iloc[index]["NAICS code"])

            if df.iloc[index]["End use"].strip() == "Glass:":
                total_glass = int(df.iloc[index]["NAICS code"])
            elif data['ActivityConsumedBy'] == "Glass Total":
                data["Description"] = total_glass

            if not math.isnan(df.iloc[index]["Total"]):
                data["FlowAmount"] = int(df.iloc[index]["Total"])
                data["ActivityProducedBy"] = None
                if not math.isnan(df.iloc[index]["NAICS code"]):
                    des_str = str(df.iloc[index]["NAICS code"])
                    data["Description"] = des_str
            if df.iloc[index]["End use"].strip() != "Glass:":
                dataframe = dataframe.append(data, ignore_index=True)
                dataframe = assign_fips_location_system(dataframe, str(args["year"]))
    return dataframe

