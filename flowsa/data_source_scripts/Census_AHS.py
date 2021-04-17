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
import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system

# 2011 and 2013 are LOT, 2015 and 2017 are LOTSIZE
COLS_TO_KEEP = ["LOT", "LOTSIZE", "WEIGHT", "METRO3", "CROPSL", "CONTROL"]


def ahs_url_helper(build_url, config, args):
    """
    This helper function uses the "build_url" input from flowbyactivity.py, which
    is a base url for blm pls data that requires parts of the url text string
    to be replaced with info specific to the data year.
    This function does not parse the data, only modifies the urls from which data is obtained.
    :param build_url: string, base url
    :param config: dictionary of method yaml
    :param args: dictionary, arguments specified when running
    flowbyactivity.py ('year' and 'source')
    :return: list of urls to call, concat, parse
    """
    version = config["years"][args["year"]]
    url = build_url
    url = url.replace("__ver__", version)
    return [url]


def ahs_call(url, ahs_response, args):
    """
    Convert response for calling url to pandas dataframe, transform to pandas df
    :param url: string, url
    :param response_load: df, response from url call
    :param args: dictionary, arguments specified when running
    flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
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


def ahs_parse(dataframe_list, args):
    """
    Functions to being parsing and formatting data into flowbyactivity format
    :param dataframe_list: list of dataframes to concat and format
    :param args: arguments as specified in flowbyactivity.py ('year' and 'source')
    :return: dataframe parsed and partially formatted to flowbyactivity specifications
    """

    df = pd.concat(dataframe_list)

    df["Class"] = "Land"
    df["SourceName"] = "Census_AHS"
    df["ActivityProducedBy"] = "None"
    df["Compartment"] = "None"
    df["Location"] = "00000"
    df["Year"] = args["year"]
    id_vars = ["Class", "SourceName", "ActivityConsumedBy",
               "ActivityProducedBy", "Compartment", "Location", "Year"]

    # Rename the PK column from "CONTROL" to "ActivityConsumedBy"
    df = df.rename(columns={"CONTROL": "ActivityConsumedBy"})

    # Set index on the df:
    df.set_index(id_vars)

    # Replace quotes ONLY on the rows with quotes: CROPSL and METRO3
    if args['year'] != '2017':
        df["CROPSL"] = df["CROPSL"].str.replace("B", "0")
        df["CROPSL"] = df["CROPSL"].str.replace("-6", "0")
        # df["CROPSL"] = df["CROPSL"].str.replace("-9", WITHDRAWN_KEYWORD)
        df["CROPSL"] = df["CROPSL"].str.replace(r"[\']", "")

    if args['year'] not in ['2015', '2017']:
        df["METRO3"] = df["METRO3"].str.replace("B", "0")
        df["METRO3"] = df["METRO3"].str.replace("-6", "0")
        # df["METRO3"] = df["METRO3"].str.replace("-9", WITHDRAWN_KEYWORD)
        df["METRO3"] = df["METRO3"].str.replace(r"[\']", "")
        df["LOT"] = df["LOT"].replace(-6, 0)
        # df["LOT"] = df["LOT"].replace(-9, WITHDRAWN_KEYWORD)

    else:
        df["LOTSIZE"] = df["LOTSIZE"].str.replace("B", "0")
        df["LOTSIZE"] = df["LOTSIZE"].str.replace("-6", "0")
        # df["LOTSIZE"] = df["LOTSIZE"].str.replace("-9", WITHDRAWN_KEYWORD)
        df["LOTSIZE"] = df["LOTSIZE"].str.replace(r"[\']", "")

    df["WEIGHT"] = df["WEIGHT"].replace(-6, 0)
    # df["WEIGHT"] = df["WEIGHT"].replace(-9, WITHDRAWN KEYWORD)

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
    df["Compartment"] = None
    # Fill in the rest of the Flow by fields so they show "None" instead of nan.
    df["MeasureofSpread"] = None
    df["DistributionType"] = None
    df["FlowType"] = None
    return df

# def calculate_urban_and_rural_residential_land_area(ahs_fba):
#     """
#     Read in an American Housing Survey FlowByActivity and
#     calculate the land area in the US occupied by
#     urban and rural housing
#     :param ahs_fba: American Housing Survey FlowByActivity
#     :return:
#     """
#
#     # assumption on lot size where missing (assumption duplicates USDA Major Land Use report)
#     fill_missing_lot_value = 200
#     convert_square_ft_to_acres = 43560
#
#     # pivot table
#     df = ahs_fba.pivot(index='ActivityConsumedBy', columns='FlowName', values = 'FlowAmount')
#
#     # replace any missing lot size values with assumption defined above
#     df2 = df.assign(LOT=df.apply(lambda x: fill_missing_lot_value if x['LOT']
#                                                                      == -9 else x['LOT'], axis=1))
#     # convert lot data from square feet to square acres
#     df2 = df.assign(LOT=df['LOT']/convert_square_ft_to_acres)
#
#     # label rows as urban or rural
# #     df2 = df2.assign(loc_label=df2.apply(lambda x: 'urban' if x['METRO3'] in
# #                                                               [1, 2, 4] else 'rural', axis=1))
#     df2 = df2.assign(loc_label=df2.apply(lambda x: 'urban' if x['METRO3'] in
#                                                               [1, 2, 4] else '', axis=1))
#     # df2 = df2.assign(loc_label=df2.apply(lambda x: 'rural' if (x['METRO3'] in
#     #                                                            [3, 5]) else x['loc_label'], axis=1))
#     df2 = df2.assign(loc_label=df2.apply(lambda x: 'rural' if (x['METRO3'] in [3, 5])
#                                                               & (x['CROPSL'] == 1) else x['loc_label'], axis=1))
#
#     # We then weighted the lot sizes (‘lot’) with the AHS survey weights, summed,
#     and converted to acres (dividing by 43560)
#
#     # weight_calc = weighted_average(df2, 'LOT', 'WEIGHT', 'loc_label')
#     df3 = df2.assign(FlowAmount=df2['LOT'] * df2['WEIGHT'])
#
#     calculated_area = df3.groupby(df3['loc_label']).agg({'FlowAmount': ['sum']})
#
#     return calculated_area
