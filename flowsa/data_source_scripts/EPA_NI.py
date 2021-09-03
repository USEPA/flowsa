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
SPAN_YEARS = "2002"


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
  #  df_raw_2007 = pd.io.excel.read_excel(externaldatapath + "jgrg21492-sup-0002-2019jg005110-ds01.xlsx",
   #                                      sheet_name='2007')
  #  df_raw_2012 = pd.io.excel.read_excel(externaldatapath + "jgrg21492-sup-0002-2019jg005110-ds01.xlsx",
 #                                        sheet_name='2012')
    df_raw_legend = pd.io.excel.read_excel(externaldatapath + "jgrg21492-sup-0002-2019jg005110-ds01.xlsx", sheet_name='Legend')
    if args['year']
    for col_name in df_raw_2002.columns:
        for i in range(len(df_raw_legend)):
            if col_name == df_raw_legend.loc[i, "HUC8_1"]:
                df_raw_2002 = df_raw_2002.rename(columns={col_name: df_raw_legend.loc[i, "HUC8 CODE"]})
    # use "melt" fxn to convert colummns into rows
    df = df_raw_2002.melt(id_vars=["HUC8_1"],
                 var_name="ActivityProducedBy",
                 value_name="FlowAmount")


















    df = df[df['Year'] == args['year']]
    # hardcode data
    df["Class"] = "Money"
    df["FlowType"] = "TECHNOSPHERE_FLOW"
    df['Description'] = 'BEA_2012_Detail_Code'
    df['FlowName'] = 'Gross Output'
    df["SourceName"] = "BEA_GDP_GrossOutput"
    df["Location"] = US_FIPS
    df['LocationSystem'] = "FIPS_2015"  # state FIPS codes have not changed over last decade
    df["Unit"] = "USD"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5 # tmp

    return df





