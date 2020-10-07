# EPA_GHG_Inventory.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Inventory of US EPA GHG
https://www.epa.gov/ghgemissions/inventory-us-greenhouse-gas-emissions-and-sinks-1990-2018
"""


import zipfile
import io
import numpy as np
import pandas as pd
from flowsa.flowbyfunctions import assign_fips_location_system, add_missing_flow_by_fields
from flowsa.common import flow_by_activity_fields

# Decided to add tables as a constant in the source code because the YML config isn't available in the ghg_call method.
# Only keeping years 2010-2018 for the following tables:
# TABLES = {
#     "Ch 2 - Trends": ["2-1"],
#     "Ch 3 - Energy": ["3-10", "3-11", "3-14", "3-15", "3-21", "3-22", "3-37", "3-38", "3-39", "3-57", "3-59"]
#     "Ch 4 - Industrial Processes": ["4-43"]
# }

# NOTE: 3-22 is completely different format...
TABLES = {
    "Ch 4 - Industrial Processes": ["4-43", "4-48", "4-80", "4-94", "4-99", "4-101"],
    "Ch 5 - Agriculture": ["5-3", "5-7", "5-18", "5-19", "5-30"],
    # "Appendices": [""]
    "Executive Summary": ["ES-5"]
}   # "3-22",

# Table 3-22 has TOTAL data, and not YEARLY data, so the format varies drastically.
# Consider splitting the calls by YEARLY and TOTAL to facilitate data gathering.
SPECIAL_FORMAT = ["3-22"]

DROP_COLS = ["Unnamed: 0", "1990", "1991", "1992", "1993", "1994", "1995", "1996", "1997", "1998",
             "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009"]

TBL_META = {
    "EPA_GHG_Inventory_T_2_1": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 2-1:  Recent Trends in U.S. Greenhouse Gas Emissions and Sinks (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_3_10": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 3-10:  CH4 Emissions from Stationary Combustion (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_3_11": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 3-11:  N2O Emissions from Stationary Combustion (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_3_14": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 3-14:  CH4 Emissions from Mobile Combustion (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_3_15": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 3-14:  CH4 Emissions from Mobile Combustion (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_3_21": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "TBtu",
        "desc": "Table 3-21:  Adjusted Consumption of Fossil Fuels for Non-Energy Uses (TBtu)"
    },
    "EPA_GHG_Inventory_T_3_22": {  # TODO: This is a different format!
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 3-22:  2018 Adjusted Non-Energy Use Fossil Fuel Consumption, Storage, and Emissions"
    },
    "EPA_GHG_Inventory_T_3_37": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 3-37:  CH4 Emissions from Petroleum Systems (MMT CO2 Eq.)"
    },
    "EPA_GHG_Inventory_T_3_38": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CH4 Eq",
        "desc": "Table 3-38:  CH4 Emissions from Petroleum Systems (kt CH4)"
    },
    "EPA_GHG_Inventory_T_3_39": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 3-39:  CO2 Emissions from Petroleum Systems (MMT CO2)"
    },
    "EPA_GHG_Inventory_T_3_57": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 3-57:  CH4 Emissions from Natural Gas Systems (MMT CO2 Eq.)a"
    },
    "EPA_GHG_Inventory_T_3_59": {
        "class": "Chemicals", "unit": "kg", "compartment": "air", "flow_name": "CO2 Eq",
        "desc": "Table 3-59:  Non-combustion CO2 Emissions from Natural Gas Systems (MMT)"
    },
}


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
                if table not in SPECIAL_FORMAT:
                    df = pd.read_csv(data, skiprows=2, encoding="ISO-8859-1")
                else:
                    # Skipping this weird tables for now. Will require more thought for processing...
                    pass
                    # Skip first two rows, as usual, but make headers the next 3 rows:
                    df = pd.read_csv(data, skiprows=2, encoding="ISO-8859-1", header=[0, 1, 2])
                    # The next two rows are headers and the third is units:
                    new_headers = []
                    for col in df.columns:
                        # unit = col[2]
                        new_header = 'Unnamed: 0'
                        if 'Unnamed' not in col[0]:
                            if 'Unnamed' not in col[1]:
                                new_header = f'{col[0]} {col[1]}'
                            else:
                                new_header = col[0]
                            if 'Unnamed' not in col[2]:
                                new_header += f' {col[2]}'
                            # unit = col[2]
                        elif 'Unnamed' in col[0] and 'Unnamed' not in col[2]:
                            new_header = col[2]
                        new_headers.append(new_header)
                    df.columns = new_headers
                    print('break')

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
        # Specify to ignore errors in case one of the drop_cols is missing.
        df = df.drop(columns=DROP_COLS, errors='ignore')

        # data_type = df.columns[0]
        # if 'gas' in data_type.lower():
        #     df["Class"] = "Chemicals"
        # else:
        #     df["Class"] = "Other"

        # Rename the PK column from data_type to "ActivityProducedBy":
        df = df.rename(columns={df.columns[0]: "ActivityProducedBy"})

        # df["ActivityProducedBy"] = "None"
        df["ActivityConsumedBy"] = "None"
        df["Compartment"] = "None"
        # df["FlowType"] = "ELEMENTARY_FLOW"
        df["Location"] = "00000"
        # if 'Year' not in df.columns:
        #     df["Year"] = args["year"]

        id_vars = ["SourceName", "ActivityConsumedBy", "ActivityProducedBy", "Compartment", "Location"]
        # Set index on the df:
        df.set_index(id_vars)

        # If Table 3-22:
        # df = df.melt(id_vars=id_vars, var_name="FlowName", value_name="FlowAmount")
        df = df.melt(id_vars=id_vars, var_name="Year", value_name="FlowAmount")

        # Dropping all rows with value "+"
        try:
            df = df[~df["FlowAmount"].str.contains("\\+", na=False)]
        except AttributeError as ex:
            print(ex)

        # Convert all empty cells to nan cells
        df["FlowAmount"].replace("", np.nan, inplace=True)
        # Table 3-10 has some NO values, dropping these.
        df["FlowAmount"].replace("NO", np.nan, inplace=True)
        # Drop any nan rows
        df.dropna(subset=['FlowAmount'], inplace=True)
        # Remove commas from numbers:
        df["FlowAmount"].replace(',', '', regex=True, inplace=True)

        df["Description"] = "None"
        df["Unit"] = "Other"
        # Update classes:
        for tbl, meta in TBL_META.items():
            df.loc[df["SourceName"] == tbl, "Class"] = meta["class"]
            df.loc[df["SourceName"] == tbl, "Unit"] = meta["unit"]
            df.loc[df["SourceName"] == tbl, "Description"] = meta["desc"]
            df.loc[df["SourceName"] == tbl, "Compartment"] = meta["compartment"]
            df.loc[df["SourceName"] == tbl, "FlowName"] = meta["flow_name"]

        # Add tmp DQ scores
        df["DataReliability"] = 5
        df["DataCollection"] = 5
        df["Compartment"] = "None"
        # Fill in the rest of the Flow by fields so they show "None" instead of nan.
        df["MeasureofSpread"] = "None"
        df["DistributionType"] = "None"
        df["FlowType"] = "None"

        df = assign_fips_location_system(df, args["year"])
        # add missing flow by sector fields
        df = add_missing_flow_by_fields(df, flow_by_activity_fields)

        cleaned_list.append(df)

    df = pd.concat(cleaned_list)

    return df
