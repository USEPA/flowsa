# EPAN_NI.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
import io
import pandas as pd
from flowsa.common import externaldatapath, US_FIPS

"""
Projects
/
FLOWSA
/
SourceName: Nitrogen FBA import - Sabo
https://agupubs.onlinelibrary.wiley.com/doi/10.1029/2019JG005110

Years = 2002, 2007, 2012
"""


def ni_parse(**kwargs):
    """
    Combine, parse, and format the provided dataframes
    :param kwargs: potential arguments include:
                   dataframe_list: list of dataframes to concat and format
                   args: dictionary, used to run flowbyactivity.py ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """
    # load arguments necessary for function
    args = kwargs['args']

    # Read directly into a pandas df
    df_raw_2002 = pd.io.excel.read_excel(externaldatapath + "jgrg21492-sup-0002-2019jg005110-ds01.xlsx", sheet_name='2002')
    df_raw_2007 = pd.io.excel.read_excel(externaldatapath + "jgrg21492-sup-0002-2019jg005110-ds01.xlsx",
                                         sheet_name='2007')
    df_raw_2012 = pd.io.excel.read_excel(externaldatapath + "jgrg21492-sup-0002-2019jg005110-ds01.xlsx",
                                         sheet_name='2012')
    df_raw_legend = pd.io.excel.read_excel(externaldatapath + "jgrg21492-sup-0002-2019jg005110-ds01.xlsx", sheet_name='Legend')
    if args['year'] == '2002':
        for col_name in df_raw_2002.columns:
            for i in range(len(df_raw_legend)):
                if col_name == df_raw_legend.loc[i, "HUC8_1"]:
                    df_raw_2002 = df_raw_2002.rename(columns={col_name: df_raw_legend.loc[i, "HUC8_1"]})
        # use "melt" fxn to convert colummns into rows
        df = df_raw_2002.melt(id_vars=["HUC8_1"],
                     var_name="ActivityProducedBy",
                     value_name="FlowAmount")
    if args['year'] == '2007':
        for col_name in df_raw_2007.columns:
            for i in range(len(df_raw_legend)):
                if col_name == df_raw_legend.loc[i, "HUC8_1"]:
                    df_raw_2007 = df_raw_2007.rename(columns={col_name: df_raw_legend.loc[i, "HUC8_1"]})
        # use "melt" fxn to convert colummns into rows
        df = df_raw_2007.melt(id_vars=["HUC8_1"],
                     var_name="ActivityProducedBy",
                     value_name="FlowAmount")
    if args['year'] == '2012':
        for col_name in df_raw_2012.columns:
            for i in range(len(df_raw_legend)):
                if col_name == df_raw_legend.loc[i, "HUC8_1"]:
                    df_raw_2012 = df_raw_2012.rename(columns={col_name: df_raw_legend.loc[i, "HUC8_1"]})
        # use "melt" fxn to convert colummns into rows
        df = df_raw_2012.melt(id_vars=["HUC8_1"],
                              var_name="ActivityProducedBy",
                              value_name="FlowAmount")

    df = df.rename(columns={"HUC8_1": "Location"})


    for i in range(len(df)):

        apb = df.loc[i, "ActivityProducedBy"]
        apb_str = str(apb)
        if '(' in apb_str:
            apb_split = apb_str.split('(')
            activity = apb_split[0]
            unit_str = apb_split[1]
            unit_list = unit_str.split(')')
            unit = unit_list[0]
            df.loc[i, "ActivityProducedBy"] = activity
            df.loc[i, "Units"] = unit
        else:
            df.loc[i, "Units"] = None

        if 'N2' in apb_str:
            df.loc[i, "FlowName"] = 'N2'
        elif 'NOx' in apb_str:
            df.loc[i, "FlowName"] = 'NOx'
        elif 'N2O' in apb_str:
            df.loc[i, "FlowName"] = 'N2O'
        else:
            df.loc[i, "FlowName"] = 'Nitrogen'

        if 'emissions' in apb_str:
            df.loc[i, "Compartment "] = "air"
        else:
            df.loc[i, "Compartment "] = "ground"
    df["Class"] = "Chemicals"
    df["SourceName"] = "EPA_NI"
    df["LocationSystem"] = 'HUC'
    df["Year"] = str(args["year"])
    return df





