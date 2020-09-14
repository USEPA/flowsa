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


COLS_TO_KEEP = ["LOTSIZE", "WEIGHT", "METRO3", "CROPSL", "CONTROL"]


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
    df = df.melt(id_vars=id_vars, var_name="FlowName", value_name="FlowAmount")

    print("Replacing single quotes from the values.")
    try:
        df["FlowAmount"] = df["FlowAmount"].str.replace(r"[\']", "")
        print("Done replacing FlowAmount.")
    except AttributeError as err:
        print(err)
        print("No need to parse this set's FlowAmount. Continue.")

    try:
        df["ActivityConsumedBy"] = df["ActivityConsumedBy"].str.replace(r"[\']", "")
        print("Done replacing ActivityConsumedBy. Continue.")
    except AttributeError as err:
        print(err)
        print("No need to parse this set's ActivityConsumedBy. Continue.")

    # All values in the "codes" dictionary were acquired from the "AHS Codebook 1997 and later.pdf"
    codes = {"LOTSIZE": {},
             "WEIGHT": {"Description": "Final weight", "Unit": "Range"},
             "METRO3": {"Description": "Central city / suburban status", "Unit": "Choice"},
             "CROPSL": {"Description": "Receive farm income", "Unit": "Choice"},
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
