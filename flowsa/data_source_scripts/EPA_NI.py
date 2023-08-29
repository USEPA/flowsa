# EPAN_NI.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Projects
/
FLOWSA
/
SourceName: Nitrogen FBA import - Sabo
https://agupubs.onlinelibrary.wiley.com/doi/10.1029/2019JG005110

Years = 2002, 2007, 2012
"""
import io
import pandas as pd


def name_and_unit_split(df_legend):
    for i in range(len(df_legend)):
        apb = df_legend.loc[i, "name"]
        apb_str = str(apb)
        if '(' in apb_str:
            apb_split = apb_str.split('(')
            activity = apb_split[0].strip()
            unit_str = apb_split[1]
            unit_list = unit_str.split(')')
            unit = unit_list[0]
            df_legend.loc[i, "ActivityProducedBy"] = activity
            df_legend.loc[i, "ActivityConsumedBy"] = None
            df_legend.loc[i, "Unit"] = unit
        else:
            df_legend.loc[i, "Unit"] = None
            df_legend.loc[i, "ActivityProducedBy"] = apb_str
            df_legend.loc[i, "ActivityConsumedBy"] = None

        if 'agricultural' in apb_str.lower():
            abs_split = activity.lower().split('agricultural')
            df_legend.loc[i, "FlowName"] = abs_split[0].strip() + abs_split[1]
            df_legend.loc[i, "ActivityProducedBy"] = 'Agricultural'
        elif 'area' in apb_str.lower():
            df_legend.loc[i, "FlowName"] = "Area"
            df_legend.loc[i, "ActivityProducedBy"] = activity
        elif 'emissions' in apb_str.lower():
            abs_split = activity.lower().split('emissions')
            activity = abs_split[0].strip() + abs_split[1]
            if ':' in activity:
                activity_split = activity.lower().split(':')
                df_legend.loc[i, "FlowName"] = activity_split[1]
                df_legend.loc[i, "ActivityProducedBy"] = activity_split[0]
            else:
                df_legend.loc[i, "FlowName"] = "Nitrogen"
                df_legend.loc[i, "ActivityProducedBy"] = activity
        elif 'n content' in apb_str.lower():
            abs_split = activity.lower().split('n content')
            activity = abs_split[0].strip() + abs_split[1]
            df_legend.loc[i, "FlowName"] = "Nitrogen Content"
            df_legend.loc[i, "ActivityProducedBy"] = activity
        elif 'n recovered' in apb_str.lower():
            abs_split = activity.lower().split('n recovered')
            activity = abs_split[0].strip() + abs_split[1]
            df_legend.loc[i, "FlowName"] = "Nitrogen Recovered"
            df_legend.loc[i, "ActivityProducedBy"] = activity
        elif 'n fixation' in apb_str.lower():
            abs_split = activity.lower().split('n fixation')
            activity = abs_split[0].strip() + abs_split[1]
            df_legend.loc[i, "FlowName"] = "Nitrogen Fixation"
            df_legend.loc[i, "ActivityProducedBy"] = activity
        elif 'n-fixing' in apb_str.lower():
            abs_split = activity.lower().split('n-fixing')
            activity = abs_split[0].strip() + abs_split[1]
            df_legend.loc[i, "FlowName"] = "N Fixing"
            df_legend.loc[i, "ActivityProducedBy"] = activity
        else:
            df_legend.loc[i, "ActivityProducedBy"] = activity
            if 'NOX-N' in apb_str:
                df_legend.loc[i, "FlowName"] = 'NOX-N'
            elif 'N2O' in apb_str:
                df_legend.loc[i, "FlowName"] = 'N2O'
            elif 'NOx' in apb_str:
                df_legend.loc[i, "FlowName"] = 'NOx'
            elif 'N2' in apb_str:
                df_legend.loc[i, "FlowName"] = 'N2'
            elif 'land area' in apb_str.lower():
                df_legend.loc[i, "FlowName"] = 'Area'
            else:
                df_legend.loc[i, "FlowName"] = 'Nitrogen'

        if 'emissions' in apb_str.lower():
            df_legend.loc[i, "Compartment "] = "air"
        else:
            df_legend.loc[i, "Compartment "] = "ground"
    return df_legend


def ni_url_helper(*, build_url, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :param args: dictionary, arguments specified when running generateflowbyactivity.py
        generateflowbyactivity.py ('year' and 'source')
    :return: list, urls to call, concat, parse, format into Flow-By-Activity
        format
    """
    url = build_url
    return [url]


def ni_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df_legend = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                       sheet_name='Legend')
    if year == '2002':
        df_raw = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                        sheet_name='2002')
    elif year == '2007':
        df_raw = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                        sheet_name='2007')
    else:
        df_raw = pd.io.excel.read_excel(io.BytesIO(resp.content),
                                        sheet_name='2012')

    for col_name in df_raw.columns:
        for i in range(len(df_legend)):
            if '_20' in df_legend.loc[i, "HUC8_1"]:
                legend_str = str(df_legend.loc[i, "HUC_8"])
                list = legend_str.split('_20')
                df_legend.loc[i, "HUC8_1"] = list[0]

            if col_name == df_legend.loc[i, "HUC8_1"]:
                df_raw = df_raw.rename(
                    columns={col_name: df_legend.loc[i, "HUC8 CODE"]})

    # use "melt" fxn to convert colummns into rows
    df = df_raw.melt(id_vars=["HUC8_1"],
                     var_name="name",
                     value_name="FlowAmount")
    df = df.rename(columns={"HUC8_1": "Location"})
    df_legend = df_legend.rename(columns={"HUC8 CODE": "name"})
    df_legend = name_and_unit_split(df_legend)

    df = pd.merge(df, df_legend, on="name")
    df = df.drop(columns=["HUC8_1", "name"])
    return df


def ni_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # load arguments necessary for function

    df = pd.DataFrame()
    for df in df_list:
        df["Class"] = "Chemicals"
        df["SourceName"] = "EPA_NI"
        df["LocationSystem"] = 'HUC'
        df["Year"] = str(year)
        df["FlowType"] = "ELEMENTARY_FLOW"
    return df
