# BEA.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Supporting functions for BEA data.

BEA data is imported as csv files from useeior and formatted into FBAs in the scripts folder
https://github.com/USEPA/flowsa/tree/master/scripts/FlowByActivity_Datasets
"""

# write_FBA_BEA_Detail_GrossOutput.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Generation of BEA Gross Output data as FBA, csv files for BEA data originate from USEEIOR and were originally
generated on 2020-07-14.
"""

from flowsa.common import *
import pandas as pd


def bea_gdp_parse(dataframe_list, args):
    # Read directly into a pandas df
    df_raw = pd.read_csv(externaldatapath + "BEA_GDP_GrossOutput_IO.csv")

    df = df_raw.rename(columns={'Unnamed: 0': 'ActivityProducedBy'})

    # use "melt" fxn to convert colummns into rows
    df = df.melt(id_vars=["ActivityProducedBy"],
                 var_name="Year",
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

    return df


def subset_BEA_Use(df, attr):
    """
    Function to modify loaded BEA table based on data in the FBA method yaml
    :param df: flowbyactivity dataframe
    :param attr: attribute parameters from method yaml
    :return: modified BEA dataframe
    """
    commodity = attr['clean_parameter']
    df = df.loc[df['ActivityProducedBy'] == commodity]

    # set column to None to enable generalizing activity column later
    df.loc[:, 'ActivityProducedBy'] = None

    return df
