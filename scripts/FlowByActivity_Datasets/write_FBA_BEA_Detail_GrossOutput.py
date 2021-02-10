# write_FBA_BEA_Detail_GrossOutput.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Generation of BEA Gross Output data as FBA
"""

from flowsa.common import *
import pandas as pd
from flowsa.flowbyactivity import store_flowbyactivity
from flowsa.flowbyfunctions import add_missing_flow_by_fields

year = '2017'
csv_load = datapath + "BEA_GDP_GrossOutput_IO.csv"


if __name__ == '__main__':
    # Read directly into a pandas df
    df_raw = pd.read_csv(csv_load)

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
    df["SourceName"] = "BEA_GDP"
    df["Location"] = US_FIPS
    df['LocationSystem'] = "FIPS_2015"  # state FIPS codes have not changed over last decade
    df["Unit"] = "USD"

    # add missing dataframe fields (also converts columns to desired datatype)
    flow_df = add_missing_flow_by_fields(df, flow_by_activity_fields)
    parquet_name = 'BEA_GDP_GrossOutput_'+year
    store_flowbyactivity(flow_df, parquet_name)
