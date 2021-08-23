# BEA.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Supporting functions for BEA data.

Generation of BEA Gross Output data as FBA, csv files for BEA data
originate from USEEIOR and were originally generated on 2020-07-14.
"""
import pandas as pd
from flowsa.common import externaldatapath, US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system


def bea_gdp_parse(**kwargs):
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
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5 # tmp

    return df


def bea_use_detail_br_parse(**kwargs):
    """
    Combine, parse, and format the provided dataframes
    :param kwargs: potential arguments include:
                   dataframe_list: list of dataframes to concat and format
                   args: dictionary, used to run flowbyactivity.py ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """
    # load arguments necessary for function
    args = kwargs['args']

    csv_load = externaldatapath + "BEA_" + str(args['year']) + "_Detail_Use_PRO_BeforeRedef.csv"
    df_raw = pd.read_csv(csv_load)

    # first column is the commodity being consumed
    df = df_raw.rename(columns={'Unnamed: 0': 'ActivityProducedBy'})

    # use "melt" fxn to convert colummns into rows
    df = df.melt(id_vars=["ActivityProducedBy"],
                 var_name="ActivityConsumedBy",
                 value_name="FlowAmount")

    df['Year'] = str(args['year'])
    # hardcode data
    df['FlowName'] = "USD" + str(args['year'])
    df["Class"] = "Money"
    df["FlowType"] = "TECHNOSPHERE_FLOW"
    df['Description'] = 'BEA_2012_Detail_Code'
    df["SourceName"] = "BEA_Use_BR"
    df["Location"] = US_FIPS
    df['LocationSystem'] = "FIPS_2015"
    df['FlowAmount'] = df['FlowAmount'] * 1000000  # original unit in million USD
    df["Unit"] = "USD"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5 #tmp

    return df


def bea_make_detail_br_parse(**kwargs):
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
    df_raw = pd.read_csv(externaldatapath + "BEA_" + str(args['year']) +
                         "_Detail_Make_BeforeRedef.csv")

    # first column is the industry
    df = df_raw.rename(columns={'Unnamed: 0': 'ActivityProducedBy'})

    # use "melt" fxn to convert colummns into rows
    df = df.melt(id_vars=["ActivityProducedBy"],
                 var_name="ActivityConsumedBy",
                 value_name="FlowAmount")

    df['Year'] = str(args['year'])
    # hardcode data
    df['FlowName'] = "USD" + str(args['year'])
    df["Class"] = "Money"
    df["FlowType"] = "TECHNOSPHERE_FLOW"
    df['Description'] = 'BEA_2012_Detail_Code'
    df["SourceName"] = "BEA_Make_BR"
    df["Location"] = US_FIPS
    df['LocationSystem'] = "FIPS_2015"
    df['FlowAmount'] = df['FlowAmount'] * 1000000  # original unit in million USD
    df["Unit"] = "USD"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5 #tmp

    return df


def bea_make_ar_parse(**kwargs):
    """
    Combine, parse, and format the provided dataframes
    :param kwargs: potential arguments include:
                   dataframe_list: list of dataframes to concat and format
                   args: dictionary, used to run flowbyactivity.py ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """
    # load arguments necessary for function
    args = kwargs['args']

    # df = pd.concat(dataframe_list, sort=False)
    df_load = pd.read_csv(externaldatapath + "BEA_" + args['year'] +
                          "_Make_AfterRedef.csv", dtype="str")
    # strip whitespace
    df = df_load.apply(lambda x: x.str.strip())
    # drop rows of data
    df = df[df['Industry'] == df['Commodity']].reset_index(drop=True)
    # drop columns
    df = df.drop(columns=['Commodity', 'CommodityDescription'])
    # rename columns
    df = df.rename(columns={'Industry': 'ActivityProducedBy',
                            'IndustryDescription': 'Description',
                            'ProVal': 'FlowAmount',
                            'IOYear': 'Year'})
    df.loc[:, 'FlowAmount'] = df['FlowAmount'].astype(float) * 1000000
    # hard code data
    df['Class'] = 'Money'
    df['SourceName'] = 'BEA_Make_AR'
    df['Unit'] = 'USD'
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, args['year'])
    df['FlowName'] = 'Gross Output Producer Value After Redef'
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df


def subset_BEA_Use(df, attr):
    """
    Function to modify loaded BEA table based on data in the FBA method yaml
    :param df: df, flowbyactivity format
    :param attr: dictionary, attribute data from method yaml for activity set
    :return: modified BEA dataframe
    """
    commodity = attr['clean_parameter']
    df = df.loc[df['ActivityProducedBy'] == commodity]

    # set column to None to enable generalizing activity column later
    df.loc[:, 'ActivityProducedBy'] = None

    return df
