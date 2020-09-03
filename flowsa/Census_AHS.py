# USDA_ERS_FIWS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
USDA Economic Research Service (ERS) Farm Income and Wealth Statistics (FIWS)
https://www.ers.usda.gov/data-products/farm-income-and-wealth-statistics/

Downloads the February 5, 2020 update
"""

import zipfile
import io
from functools import reduce
from pandas._libs.parsers import QUOTE_NONE
from flowsa.common import *


# def merge_frames(frames_list, id_col):
#     """Wrapper function to merge dataframes. Use this to avoid repeating lambda code."""
#     print("=====================================================================================================")
#     print("=====================================================================================================")
#     print("MERGING LIST OF FRAMES merge_frames =================================================================")
#     print("=====================================================================================================")
#     # frames = reduce(lambda x, y: pd.merge(x, y, on=id_col), frames_list)
#     # print(frames)
#     # return frames
#     # return reduce(lambda x, y: pd.merge(x, y, on=id_col), frames_list)
#     # frames = [df.set_index(id_col) for df in frames_list]
#     frames = pd.concat(frames_list, axis=1)
#     print(frames)
#     return frames


def ahs_url_helper(build_url, config, args):
    """Based on the configured year, get the version arg and replace it into the URL."""
    version = config['years'][args['year']]
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
            if not name.endswith('Read Me.txt'):
                data = f.open(name)
                # df = pd.read_csv(data, encoding="ISO-8859-1", low_memory=False, quoting=QUOTE_NONE)
                # df = pd.read_csv(data, encoding="ISO-8859-1", low_memory=False)
                df = pd.read_csv(data, encoding="ISO-8859-1")
                df = parse_frame(df, args)
                frames.append(df)
        # if len(frames) > 1:
        #     return merge_frames(frames, 'CONTROL')
        # return frames[0]
        return frames


def parse_frame(df, args):
    """ """
    df["Class"] = "Land"
    df["SourceName"] = "Census_AHS"
    df["ActivityProducedBy"] = "None"
    df["Compartment"] = "None"
    df["Location"] = "00000"
    df['Year'] = args["year"]
    id_vars = ["Class", "SourceName", "ActivityConsumedBy", "ActivityProducedBy", "Compartment", "Location", "Year"]

    print('3')
    # print(df)

    # Rename the PK column from "CONTROL" to "ActivityConsumedBy"
    df = df.rename(columns={'CONTROL': 'ActivityConsumedBy'})

    print('4')
    # print(df)

    # Set index on the df:
    df.set_index(id_vars)
    df = df.melt(id_vars=id_vars, var_name="FlowName", value_name="FlowAmount")

    print('5')
    print(df)
    print("Replacing single quotes from the values.")
    # data = df.replace("'", "", regex=True)
    try:
        df['FlowAmount'] = df['FlowAmount'].str.replace(r"[\']", "")
        print("Done replacing FlowAmount.")
    except AttributeError as err:
        print(err)
        print("No need to parse this set's FlowAmount. Continue.")

    try:
        df['ActivityConsumedBy'] = df['ActivityConsumedBy'].str.replace(r"[\']", "")
        print("Done replacing ActivityConsumedBy. Continue.")
    except AttributeError as err:
        print(err)
        print("No need to parse this set's ActivityConsumedBy. Continue.")

    print(df)

    df['SourceName'] = 'Census_AHS'
    # Add tmp DQ scores
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Compartment'] = None
    return df


# TODO: Figure out why all the None fields in parquet are nan instead.


def ahs_parse(dataframe_list, args):
    """ TODO. """

    return dataframe_list

    # print('1')
    # # print(dataframe_list)
    # if len(dataframe_list) > 1:
    #     # merge dataframes
    #     df = merge_frames(dataframe_list, 'CONTROL')
    # else:
    #     df = dataframe_list[0]
    #
    # print('2')
    # # print(df)

    # for df in dataframe_list:
    #     df["Class"] = "Land"
    #     df["SourceName"] = "Census_AHS"
    #     df["ActivityProducedBy"] = "None"
    #     df["Compartment"] = "None"
    #     df["Location"] = "00000"
    #     df['Year'] = args["year"]
    #     id_vars = ["Class", "SourceName", "ActivityConsumedBy", "ActivityProducedBy", "Compartment", "Location", "Year"]
    #
    #     print('3')
    #     # print(df)
    #
    #     # Rename the PK column from "CONTROL" to "ActivityConsumedBy"
    #     df = df.rename(columns={'CONTROL': 'ActivityConsumedBy'})
    #
    #     print('4')
    #     # print(df)
    #
    #     # Set index on the df:
    #     df.set_index(id_vars)
    #
    #     # use "melt" fxn to convert colummns into rows
    #     # This is resulting in an error: "Exception: Data must be 1-dimensional"
    #     # df = df.melt(id_vars=["Location", "ActivityConsumedBy", "Year", "Compartment"],
    #     df = df.melt(id_vars=id_vars, var_name="FlowName", value_name="FlowAmount")
    #
    #     print('5')
    #     print(df)
    #     print("Replacing single quotes from the values.")
    #     # data = df.replace("'", "", regex=True)
    #     df['FlowAmount'] = df['FlowAmount'].str.replace(r"[\']", "")
    #     print("Done replacing FlowAmount.")
    #     df['ActivityConsumedBy'] = df['ActivityConsumedBy'].str.replace(r"[\']", "")
    #     print("Done replacing ActivityConsumedBy. Continue.")
    #     print(df)
    #
    #     # drop columns
    #     # rename columns
    #     # assign flowname, based on comma placement
    #     # drop unnecessary rows
    #
    #     # add location system based on year of datatask
    #     # This crashes the script:
    #     # df = assign_fips_location_system(df, args['year'])
    #     # hard code data
    #     df['SourceName'] = 'Census_AHS'
    #     # Add tmp DQ scores
    #     df['DataReliability'] = 5
    #     df['DataCollection'] = 5
    #     df['Compartment'] = None
    #     # Before returning the dataframe, strip single quotes from the FlowAmount values.
    #     # The single quotes are breaking the float casting later on.
    #     # df['FlowAmount']
    #
    # return dataframe_list
