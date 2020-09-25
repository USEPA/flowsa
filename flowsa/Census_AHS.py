# Census_AHS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
US Census American Housing Survey (AHS)
2011 - 2017, National Level
https://www.census.gov/programs-surveys/ahs/data.html
"""


import zipfile
import io
import numpy as np
import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system


# 2011 and 2013 are LOT, 2015 and 2017 are LOTSIZE
COLS_TO_KEEP = ["LOT", "LOTSIZE", "WEIGHT", "METRO3", "CROPSL", "CONTROL"]


def ahs_url_helper(build_url, config, args):
    """Based on the configured year, get the version arg and replace it into the URL."""
    version = config["years"][args["year"]]
    url = build_url
    url = url.replace("__ver__", version)
    return [url]


def ahs_call(url, ahs_response, args):
    """
    Callback function for the Census AHS URL download. Open the downloaded zip file and
    read the contained CSV(s) into pandas dataframe(s).
    """
    # extract data from zip file (multiple csvs)
    with zipfile.ZipFile(io.BytesIO(ahs_response.content), "r") as f:
        # read in file names
        frames = []
        for name in f.namelist():
            if not name.endswith("Read Me.txt"):
                data = f.open(name)
                df = pd.read_csv(data, encoding="ISO-8859-1")
                # Before appending the dataframe, cut out columns we don"t need.
                # We are only interested in keeping: lotsize, weight, metro3, cropsl
                cols = [col for col in df.columns if col in COLS_TO_KEEP]
                df = df[cols]
                if len(df.columns) > 1:
                    # df = parse_frame(df, args)
                    frames.append(df)
        return pd.concat(frames)


# def convert_values(df, args):
#     """
#     Convert columns to usable values. Columns include:
#     LOT/LOTSIZE, WEIGHT, METRO3, CROPSL
#     """
#     if args['year'] in ['2011', '2013']:
#         print("All four columns are available.")
#
#     elif args['year'] == '2015':
#         print("LOT, WEIGHT, CROPSL")
#
#     elif args['year'] == '2017':
#         print("LOT, WEIGHT")
#
#     return df


# TODO: Figure out why all the None fields in parquet are nan instead.
def ahs_parse(dataframe_list, args):
    """ TODO. """
    df = pd.concat(dataframe_list)

    df["Class"] = "Land"
    df["SourceName"] = "Census_AHS"
    df["ActivityProducedBy"] = "None"
    df["Compartment"] = "None"
    df["Location"] = "00000"
    df["Year"] = args["year"]
    id_vars = ["Class", "SourceName", "ActivityConsumedBy", "ActivityProducedBy", "Compartment", "Location", "Year"]

    # Rename the PK column from "CONTROL" to "ActivityConsumedBy"
    df = df.rename(columns={"CONTROL": "ActivityConsumedBy"})

    # Set index on the df:
    df.set_index(id_vars)

    # Replace quotes ONLY on the rows with quotes: CROPSL and METRO3
    if args['year'] != '2017':
        df["CROPSL"] = df["CROPSL"].str.replace("B", "0")
        df["CROPSL"] = df["CROPSL"].str.replace("-6", "0")
        # df["CROPSL"] = df["CROPSL"].str.replace("-9", "withdrawn_keyword")
        df["CROPSL"] = df["CROPSL"].str.replace(r"[\']", "")

    if args['year'] not in ['2015', '2017']:
        df["METRO3"] = df["METRO3"].str.replace("B", "0")
        df["METRO3"] = df["METRO3"].str.replace("-6", "0")
        # df["METRO3"] = df["METRO3"].str.replace("-9", "withdrawn_keyword")
        df["METRO3"] = df["METRO3"].str.replace(r"[\']", "")
        df["LOT"] = df["LOT"].replace(-6, 0)
        # df["LOT"] = df["LOT"].replace(-9, "withdrawn_keyword")

    else:
        df["LOTSIZE"] = df["LOTSIZE"].str.replace("B", "0")
        df["LOTSIZE"] = df["LOTSIZE"].str.replace("-6", "0")
        # df["LOTSIZE"] = df["LOTSIZE"].str.replace("-9", "withdrawn_keyword")
        df["LOTSIZE"] = df["LOTSIZE"].str.replace(r"[\']", "")

    df["WEIGHT"] = df["WEIGHT"].replace(-6, 0)
    # df["WEIGHT"] = df["WEIGHT"].replace(-9, "withdrawn_keyword")

    # df = convert_values(df, args)
    df = df.melt(id_vars=id_vars, var_name="FlowName", value_name="FlowAmount")

    try:
        df["ActivityConsumedBy"] = df["ActivityConsumedBy"].str.replace(r"[\']", "")
        print("Done replacing ActivityConsumedBy. Continue.")
    except AttributeError as err:
        print(err)
        print("No need to parse this set's ActivityConsumedBy. Continue.")

    # All values in the "codes" dictionary were acquired from the "AHS Codebook 1997 and later.pdf"
    codes = {"LOT": {"Description": "Square footage of lot", "Unit": "square feet"},
             "LOTSIZE": {"Description": "Square footage of lot", "Unit": "square feet"},
             "WEIGHT": {"Description": "Final weight", "Unit": "None"},
             "METRO3": {"Description": "Central city / suburban status", "Unit": "None"},
             "CROPSL": {"Description": "Receive farm income", "Unit": "None"},
             "CONTROL": {"Description": "Control number", "Unit": "String"}}

    df["Description"] = "None"
    df["Unit"] = "None"

    for key, value in codes.items():
        if value:
            desc = value["Description"]
            unit = value["Unit"]
            df.loc[df["FlowName"] == key, "Description"] = desc
            df.loc[df["FlowName"] == key, "Unit"] = unit

    df = assign_fips_location_system(df, args["year"])

    # Add tmp DQ scores
    df["DataReliability"] = 5
    df["DataCollection"] = 5
    df["Compartment"] = "None"
    # Fill in the rest of the Flow by fields so they show "None" instead of nan.
    df["MeasureofSpread"] = "None"
    df["DistributionType"] = "None"
    df["FlowType"] = "None"
    return df
