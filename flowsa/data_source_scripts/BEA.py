# BEA.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Supporting functions for BEA data.

Generation of BEA Gross Output data as FBA, csv files for BEA data
originate from USEEIOR and were originally generated on 2020-07-14.
"""
import pandas as pd
from flowsa.common import US_FIPS
from flowsa.settings import externaldatapath
from flowsa.flowbyfunctions import assign_fips_location_system


def bea_gdp_parse(*, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # Read directly into a pandas df
    df_raw = pd.read_csv(externaldatapath + "BEA_GDP_GrossOutput_IO.csv")

    df = df_raw.rename(columns={'Unnamed: 0': 'ActivityProducedBy'})

    # use "melt" fxn to convert colummns into rows
    df = df.melt(id_vars=["ActivityProducedBy"],
                 var_name="Year",
                 value_name="FlowAmount")

    df = df[df['Year'] == year]
    # hardcode data
    df["Class"] = "Money"
    df["FlowType"] = "TECHNOSPHERE_FLOW"
    df['Description'] = 'BEA_2012_Detail_Code'
    df['FlowName'] = 'Gross Output'
    df["SourceName"] = "BEA_GDP_GrossOutput"
    df["Location"] = US_FIPS
    # state FIPS codes have not changed over last decade
    df['LocationSystem'] = "FIPS_2015"
    df["Unit"] = "USD"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df


def bea_use_detail_br_parse(*, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param year: year)
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    csv_load = f'{externaldatapath}BEA_{str(year)}' \
               f'_Detail_Use_PRO_BeforeRedef.csv'
    df_raw = pd.read_csv(csv_load)

    # first column is the commodity being consumed
    df = df_raw.rename(columns={'Unnamed: 0': 'ActivityProducedBy'})

    # use "melt" fxn to convert colummns into rows
    df = df.melt(id_vars=["ActivityProducedBy"],
                 var_name="ActivityConsumedBy",
                 value_name="FlowAmount")

    df['Year'] = str(year)
    # hardcode data
    df['FlowName'] = "USD" + str(year)
    df["Class"] = "Money"
    df["FlowType"] = "TECHNOSPHERE_FLOW"
    df['Description'] = 'BEA_2012_Detail_Code'
    df["SourceName"] = "BEA_Use_Detail_PRO_BeforeRedef"
    df["Location"] = US_FIPS
    df['LocationSystem'] = "FIPS_2015"
    # original unit in million USD
    df['FlowAmount'] = df['FlowAmount'] * 1000000
    df["Unit"] = "USD"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df


def bea_make_detail_br_parse(*, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # Read directly into a pandas df
    df_raw = pd.read_csv(externaldatapath + "BEA_" + str(year) +
                         "_Detail_Make_BeforeRedef.csv")

    # first column is the industry
    df = df_raw.rename(columns={'Unnamed: 0': 'ActivityProducedBy'})

    # use "melt" fxn to convert colummns into rows
    df = df.melt(id_vars=["ActivityProducedBy"],
                 var_name="ActivityConsumedBy",
                 value_name="FlowAmount")

    df['Year'] = str(year)
    # hardcode data
    df['FlowName'] = "USD" + str(year)
    df["Class"] = "Money"
    df["FlowType"] = "TECHNOSPHERE_FLOW"
    df['Description'] = 'BEA_2012_Detail_Code'
    df["SourceName"] = "BEA_Make_Detail_BeforeRedef"
    df["Location"] = US_FIPS
    df['LocationSystem'] = "FIPS_2015"
    # original unit in million USD
    df['FlowAmount'] = df['FlowAmount'] * 1000000
    df["Unit"] = "USD"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df


def bea_make_ar_parse(*, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param dataframe_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # df = pd.concat(dataframe_list, sort=False)
    df_load = pd.read_csv(externaldatapath + "BEA_" + year +
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
    df = assign_fips_location_system(df, year)
    df['FlowName'] = 'Gross Output Producer Value After Redef'
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df


def subset_BEA_table(df, attr, **_):
    """
    Modify loaded BEA table (make or use) based on data in the FBA method yaml
    :param df: df, flowbyactivity format
    :param attr: dictionary, attribute data from method yaml for activity set
    :return: modified BEA dataframe
    """
    # extract commodity to filter and which Activity column used to filter
    (commodity, ActivityCol), *rest = attr['clean_parameter'].items()
    df = df.loc[df[ActivityCol] == commodity].reset_index(drop=True)

    # set column to None to enable generalizing activity column later
    df.loc[:, ActivityCol] = None

    return df
