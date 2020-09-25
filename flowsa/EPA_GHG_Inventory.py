# EPA_GHG_Inventory.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Inventory of US EPA GHG
https://www.epa.gov/ghgemissions/inventory-us-greenhouse-gas-emissions-and-sinks-1990-2018
"""


import zipfile
import io
import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system
import os

# Decided to add tables as a constant in the source code because
# the YML config isn't available in the ghg_call method.
# Only keeping years 2010-2018 for the following tables:
TABLES = {
    "Ch 2 - Trends": ["2-1"],
}

DROP_COLS = ["Unnamed: 0", "1990", "1991", "1992", "1993", "1994", "1995", "1996", "1997", "1998", "1999",
             "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009"]


def ghg_url_helper(build_url, config, args):
    """Only one URL is needed to retrieve the data for all tables for all years."""
    return [build_url]


def ghg_call(url, response, args):
    """
    Callback function for the US GHG Emissions download. Open the downloaded zip file and
    read the contained CSV(s) into pandas dataframe(s).
    """
    with zipfile.ZipFile(io.BytesIO(response.content), "r") as f:
        frames = []
        for chapter, tables in TABLES.items():
            for table in tables:
                # path = os.path.join("Chapter Text", chapter, f"Table {table}.csv")
                path = f"Chapter Text/{chapter}/Table {table}.csv"
                data = f.open(path)
                df = pd.read_csv(data, skiprows=2, encoding="ISO-8859-1")
                if len(df.columns) > 1:
                    # Assign SourceName now while we still have access to the table name:
                    source_name = f"EPA_GHG_Inventory_T_{table.replace('-', '_')}"
                    df["SourceName"] = source_name
                    frames.append(df)
        return pd.concat(frames)


def ghg_parse(dataframe_list, args):
    """ TODO. """
    cleaned_list = []
    for df in dataframe_list:
        df = df.drop(columns=DROP_COLS)
        data_type = df.columns[0]
        if 'gas' in data_type.lower():
            df["Class"] = "Chemicals"
        else:
            df["Class"] = "Other"

        # Rename the PK column from data_type to "ActivityConsumedBy" or "FlowName"?
        df = df.rename(columns={data_type: "FlowName"})

        df["ActivityProducedBy"] = "None"
        df["ActivityConsumedBy"] = "None"
        df["Compartment"] = "None"
        df["Location"] = "00000"
        # df["Year"] = args["year"]

        id_vars = ["Class", "SourceName", "ActivityConsumedBy", "ActivityProducedBy",
                   "Compartment", "Location", "FlowName"]
        # Set index on the df:
        df.set_index(id_vars)

        df = df.melt(id_vars=id_vars, var_name="Year", value_name="FlowAmount")

        df["Description"] = "None"
        df["Unit"] = "None"
        df = assign_fips_location_system(df, args["year"])

        # Add tmp DQ scores
        df["DataReliability"] = 5
        df["DataCollection"] = 5
        df["Compartment"] = "None"
        # Fill in the rest of the Flow by fields so they show "None" instead of nan.
        df["MeasureofSpread"] = "None"
        df["DistributionType"] = "None"
        df["FlowType"] = "None"

        print("Process df!")
        cleaned_list.append(df)

    df = pd.concat(cleaned_list)

    # df["WEIGHT"] = df["WEIGHT"].replace(-6, 0)
    # # df["WEIGHT"] = df["WEIGHT"].replace(-9, "withdrawn_keyword")
    #
    # # df = convert_values(df, args)
    # df = df.melt(id_vars=id_vars, var_name="FlowName", value_name="FlowAmount")
    #
    # try:
    #     df["ActivityConsumedBy"] = df["ActivityConsumedBy"].str.replace(r"[\']", "")
    #     print("Done replacing ActivityConsumedBy. Continue.")
    # except AttributeError as err:
    #     print(err)
    #     print("No need to parse this set's ActivityConsumedBy. Continue.")
    #
    # codes = {"LOT": {"Description": "Square footage of lot", "Unit": "square feet"},
    #          "LOTSIZE": {"Description": "Square footage of lot", "Unit": "square feet"},
    #          "WEIGHT": {"Description": "Final weight", "Unit": "None"},
    #          "METRO3": {"Description": "Central city / suburban status", "Unit": "None"},
    #          "CROPSL": {"Description": "Receive farm income", "Unit": "None"},
    #          "CONTROL": {"Description": "Control number", "Unit": "String"}}
    #
    #
    # for key, value in codes.items():
    #     if value:
    #         desc = value["Description"]
    #         unit = value["Unit"]
    #         df.loc[df["FlowName"] == key, "Description"] = desc
    #         df.loc[df["FlowName"] == key, "Unit"] = unit

    return df
