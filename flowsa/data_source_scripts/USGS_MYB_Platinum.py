# USGS_MYB_Platinum.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Projects
/
FLOWSA
/

FLOWSA-314

Import USGS Mineral Yearbook data

Description

Table T1

SourceName: USGS_MYB_Platinum
https://www.usgs.gov/centers/nmic/platinum-group-metals-statistics-and-information

Minerals Yearbook, xls file, tab T1: SALIENT PLATINUM STATISTICS
data for:

Primary lead, refined content, domestic ores and base bullion
Palladium, Pd content:, Platinum, Pt content:

Platinum group metals; iridium
Platinum group metals; osmium
Platinum group metals; rhodium
Platinum group metals; ruthenium
There is no production value for iridium, osmium, rhodium, ruthenium
There is no export value for Osmium or Ruthenium

Years = 2014+
"""
import io
import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.data_source_scripts.USGS_MYB_Common import *

SPAN_YEARS = "2014-2018"


def usgs_platinum_url_helper(*, build_url, **_):
    """
    This helper function uses the "build_url" input from flowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    :param build_url: string, base url
    :return: list, urls to call, concat, parse, format into Flow-By-Activity
        format
    """
    url = build_url
    return [url]


def usgs_platinum_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                         sheet_name='T1')
    df_data_1 = pd.DataFrame(df_raw_data.loc[4:9]).reindex()
    df_data_1 = df_data_1.reset_index()
    del df_data_1["index"]

    df_data_2 = pd.DataFrame(df_raw_data.loc[18:30]).reindex()
    df_data_2 = df_data_2.reset_index()
    del df_data_2["index"]

    if len(df_data_1. columns) == 13:
        df_data_1.columns = ["Production", "space_6", "Units", "space_1",
                             "year_1", "space_2", "year_2", "space_3",
                             "year_3", "space_4", "year_4", "space_5",
                             "year_5"]
        df_data_2.columns = ["Production", "space_6", "Units", "space_1",
                             "year_1", "space_2", "year_2", "space_3",
                             "year_3", "space_4", "year_4", "space_5",
                             "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(SPAN_YEARS, year))
    for col in df_data_1.columns:
        if col not in col_to_use:
            del df_data_1[col]
            del df_data_2[col]

    frames = [df_data_1, df_data_2]
    df_data = pd.concat(frames)
    df_data = df_data.reset_index()
    del df_data["index"]
    return df_data


def usgs_platinum_parse(*, df_list, source, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param source: source
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Quantity", "Palladium, Pd content",
                  "Platinum, includes coins, Pt content",
                  "Platinum, Pt content",
                  "Iridium, Ir content", "Osmium, Os content",
                  "Rhodium, Rh content", "Ruthenium, Ru content",
                  "Iridium, osmium, and ruthenium, gross weight",
                  "Rhodium, Rh content"]
    dataframe = pd.DataFrame()

    for df in df_list:
        previous_name = ""
        for index, row in df.iterrows():

            if df.iloc[index]["Production"].strip() == "Exports, refined:":
                product = "exports"
            elif df.iloc[index]["Production"].strip() == \
                    "Imports for consumption, refined:":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == "Mine production:2":
                product = "production"

            name_array = df.iloc[index]["Production"].strip().split(",")

            if product == "production":
                name_array = previous_name.split(",")

            previous_name = df.iloc[index]["Production"].strip()
            name = name_array[0]

            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_varaibles()
                data["SourceName"] = source
                data["Year"] = str(year)
                data["Unit"] = "kilograms"
                data['FlowName'] = name + " " + product
                data["Description"] = name
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(SPAN_YEARS, year)
                if str(df.iloc[index][col_name]) == "--":
                    data["FlowAmount"] = str(0)
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = dataframe.append(data, ignore_index=True)
                dataframe = assign_fips_location_system(
                    dataframe, str(year))
    return dataframe
