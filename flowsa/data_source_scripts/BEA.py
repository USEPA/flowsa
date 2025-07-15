# BEA.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Supporting functions for BEA data.

Generation of BEA Gross Output data and industry transaction data as FBA,
Source csv files for BEA data are documented
in scripts/write_BEA_data_from_useeior.py
"""

import numpy as np
import pandas as pd
from flowsa.location import US_FIPS
from flowsa.settings import externaldatapath
from flowsa.flowbyfunctions import assign_fips_location_system, aggregator

def bea_parse(*, source, year, **_):
    """
    Parse BEA data for GrossOutput, Make, and Use tables
    :param source:
    :param year:
    :return:
    """

    if 'Make' in source:
        filename = source.replace('_Make_', f'_Make_{year}_')
    elif 'Use_SUT' in source:
        filename = f'{source}_{year}_17sch'
    elif 'Use' in source:
        filename = source.replace('_Use_', f'_Use_{year}_')
    elif 'Supply' in source:
        filename = f'{source}_{year}_17sch'
    else: # GrossOutput_IO
        filename = f'{source}_17sch'

    df = pd.read_csv(externaldatapath / f"{filename}.csv")

    if any(substring in source for substring in ['BeforeRedef', 'SUT']):
        df = df.rename(columns={'Unnamed: 0': 'ActivityProducedBy'})
        # use "melt" fxn to convert colummns into rows
        df = df.melt(id_vars=["ActivityProducedBy"],
                     var_name="ActivityConsumedBy",
                     value_name="FlowAmount")
    elif '_Make_AfterRedef' in source:
        # strip whitespace
        for c in list(df.select_dtypes(include=['object']).columns):
            df[c] = df[c].apply(lambda x: x.strip())
        # drop rows of data
        df = df[df['Industry'] == df['Commodity']].reset_index(drop=True)
        # drop columns
        df = df.drop(columns=['Commodity', 'CommodityDescription'])
        # rename columns
        df = df.rename(columns={'Industry': 'ActivityProducedBy',
                                'IndustryDescription': 'Description',
                                'ProVal': 'FlowAmount',
                                'IOYear': 'Year'})
    elif 'GrossOutput' in source:
        df = df.rename(columns={'Unnamed: 0': 'ActivityProducedBy'})
        df = df.melt(id_vars=["ActivityProducedBy"],
                     var_name="Year",
                     value_name="FlowAmount")
        df = df[df['Year'] == year]
    elif 'Supply' in source:
        df = np.transpose(df)
        df.columns = df.iloc[0]
        df = df.reset_index().rename(columns={'index' :'ActivityProducedBy'})
        df = df.drop(df.index[0])
        # use "melt" fxn to convert colummns into rows
        df = df.melt(id_vars=["ActivityProducedBy"],
                     var_name="ActivityConsumedBy",
                     value_name="FlowAmount")
    df = df.reset_index(drop=True)

    # columns relevant to all BEA data
    df["SourceName"] = source
    df['Year'] = str(year)
    df['FlowName'] = f"USD{str(year)}"
    df["Class"] = "Money"
    df["FlowType"] = "TECHNOSPHERE_FLOW"
    df["Location"] = US_FIPS
    df = assign_fips_location_system(df, year)
    df['FlowAmount'] = df['FlowAmount']
    df["Unit"] = "Million USD"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp
    df['Description'] = filename

    return df

if __name__ == "__main__":
    import flowsa
    for y in range(2012, 2024):
        flowsa.generateflowbyactivity.main(year=y, source='BEA_Summary_Supply')
        flowsa.generateflowbyactivity.main(year=y, source='BEA_Summary_Use_SUT')
        fba = flowsa.getFlowByActivity('BEA_Summary_Supply', y)
        fba2 = flowsa.getFlowByActivity('BEA_Summary_Use_SUT', y)
