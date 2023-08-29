# EPAN_NI.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Projects
/
FLOWSA
/


Years = 2002, 2007, 2012
"""

import io
import pandas as pd


def name_and_unit_split(df_legend):
    for i in range(len(df_legend)):
        apb = df_legend.loc[i, "name"]
        apb_str = str(apb)
        estimate = ""
        if 'low estimate' in apb_str.lower():
            estimate = ' low estimate'
            apb_str_split = apb_str.lower().split('low estimate')
            apb_str = apb_str_split[0] + apb_str_split[1]
        elif 'high estimate' in apb_str.lower():
            estimate = ' high estimate'
            apb_str_split = apb_str.lower().split('high estimate')
            apb_str = apb_str_split[0] + apb_str_split[1]

        if 'area' in apb_str.lower():
            df_legend.loc[i, "Class"] = "Land "
            df_legend.loc[i, "FlowName"] = 'Area' + estimate

        else:
            df_legend.loc[i, "Class"] = "Chemicals "
            df_legend.loc[i, "FlowName"] = 'Phosphorus' + estimate

        if '(' in apb_str:
            apb_split = apb_str.split('(')
            activity = apb_split[0].strip()
            unit_str = apb_split[1]
            unit_list = unit_str.split(')')
            unit = unit_list[0]
            if ' p ' in unit.lower():
                unit_split = unit.lower().split(' p ')
                new_unit = ""
                new_unit = unit_split[0] + unit_split[1]
            else:
                new_unit = unit
            if 'kg' in new_unit.lower():
                unit_split = new_unit.lower().split('kg')
                new_unit = ""
                new_unit = unit_split[0] + "/kg " + unit_split[1]
            df_legend.loc[i, "ActivityProducedBy"] = activity
            df_legend.loc[i, "ActivityConsumedBy"] = None
            df_legend.loc[i, "Unit"] = new_unit
        else:

            df_legend.loc[i, "Unit"] = None
            df_legend.loc[i, "ActivityProducedBy"] = apb_str
            df_legend.loc[i, "ActivityConsumedBy"] = None

        if apb_str == 'Livestock Waste recovered and applied to fields':
            df_legend.loc[i, "ActivityProducedBy"] = None
            df_legend.loc[i, "ActivityConsumedBy"] = activity
        if 'emissions' in apb_str:
            df_legend.loc[i, "Compartment "] = "air"
        else:
            df_legend.loc[i, "Compartment "] = "ground"
    return df_legend


def pi_url_helper(*, build_url, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
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


def pi_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param resp: df, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df_legend = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                       sheet_name='Legend')
    df_legend = pd.DataFrame(df_legend.loc[0:18]).reindex()
    df_legend.columns = ["HUC_8", "HUC8 CODE"]
    if year == '2002':
        df_raw = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                        sheet_name='2002')
        df_raw = df_raw.rename(
            columns={'P_deposition': '2P_deposition',
                     'livestock_Waste_2007': 'livestock_Waste',
                     'livestock_demand_2007': 'livestock_demand',
                     'livestock_production_2007': 'livestock_production',
                     '02P_Hi_P': 'P_Hi_P', 'Surplus_2002': 'surplus'})
    elif year == '2007':
        df_raw = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                        sheet_name='2007')
        df_raw = df_raw.rename(
            columns={'P_deposition': '2P_deposition',
                     'Crop_removal_2007': 'Crop_removal',
                     'livestock_Waste_2007': 'livestock_Waste',
                     'livestock_demand_2007': 'livestock_demand',
                     'livestock_production_2007': 'livestock_production',
                     '02P_Hi_P': 'P_Hi_P', 'Surplus_2007': 'surplus'})
    else:
        df_raw = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                        sheet_name='2012')
        df_raw = df_raw.rename(
            columns={'P_deposition': '2P_deposition',
                     'Crop_removal_2012': 'Crop_removal',
                     'livestock_Waste_2012': 'livestock_Waste',
                     'livestock_demand_2012': 'livestock_demand',
                     'livestock_production_2012': 'livestock_production',
                     '02P_Hi_P': 'P_Hi_P', 'Surplus_2012': 'surplus'})

    for col_name in df_raw.columns:
        for i in range(len(df_legend)):
            if '_20' in df_legend.loc[i, "HUC_8"]:
                legend_str = str(df_legend.loc[i, "HUC_8"])
                list = legend_str.split('_20')
                df_legend.loc[i, "HUC_8"] = list[0]

            if col_name == df_legend.loc[i, "HUC_8"]:
                df_raw = df_raw.rename(
                    columns={col_name: df_legend.loc[i, "HUC8 CODE"]})
    df_des = df_raw.filter(['HUC8 CODE', 'State Name'])
    df_raw = df_raw.drop(columns=['State Name', 'State FP Code'])

    # use "melt" fxn to convert colummns into rows
    df = df_raw.melt(id_vars=["HUC8 CODE"],
                     var_name="name",
                     value_name="FlowAmount")

    df_legend = df_legend.rename(columns={"HUC8 CODE": "name"})
    df_legend = name_and_unit_split(df_legend)

    df = pd.merge(df, df_legend, on="name")
    df = df.drop(columns=["HUC_8", "name"])
    df = df.merge(df_des, left_on='HUC8 CODE', right_on='HUC8 CODE')
    df = df.rename(columns={"HUC8 CODE": "Location",
                            "State Name": "Description"})
    return df


def pi_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    data = {}
    row_to_use = ["Production2", "Production", "Imports for consumption"]
    df = pd.DataFrame()
    for df in df_list:

        df["SourceName"] = "EPA_NI"
        df["LocationSystem"] = 'HUC'
        df["Year"] = str(year)
        df["FlowType"] = "ELEMENTARY_FLOW"
        df["Compartment"] = "ground"
    return df
