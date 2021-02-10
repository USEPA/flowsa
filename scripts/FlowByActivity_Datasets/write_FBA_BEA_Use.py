# write_FBA_BEA_Use.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Generation of BEA Use data as FBA
"""

from flowsa.common import *
import pandas as pd
from flowsa.flowbyactivity import store_flowbyactivity
from flowsa.flowbyfunctions import add_missing_flow_by_fields

year = 2012
level = 'Detail'
csv_load = datapath + "BEA_"+str(year)+"_"+level+"_Use_PRO_BeforeRedef.csv"

if __name__ == '__main__':
    # Read directly into a pandas df
    df_raw = pd.read_csv(csv_load)

    # first column is the commodity being consumed
    df = df_raw.rename(columns={'Unnamed: 0': 'ActivityProducedBy'})

    # use "melt" fxn to convert colummns into rows
    df = df.melt(id_vars=["ActivityProducedBy"],
                 var_name="ActivityConsumedBy",
                 value_name="FlowAmount")

    df['Year'] = str(year)
    # hardcode data
    df['FlowName'] = "USD"+str(year)
    df["Class"] = "Money"
    df["FlowType"] = "TECHNOSPHERE_FLOW"
    df['Description'] = 'BEA_2012_Detail_Code'
    df["SourceName"] = "BEA_Use"
    df["Location"] = US_FIPS
    df['LocationSystem'] = "FIPS_2015"
    df['FlowAmount']=df['FlowAmount']*1000000 # original unit in million USD
    df["Unit"] = "USD"

    # add missing dataframe fields (also converts columns to desired datatype)
    flow_df = add_missing_flow_by_fields(df, flow_by_activity_fields)
    parquet_name = 'BEA_Use_'+level+'_PRO_BeforeRedef_'+str(year)
    store_flowbyactivity(flow_df, parquet_name)
