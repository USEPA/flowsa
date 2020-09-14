# write_FBA_USGS_WU_Coef.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Animal Water Use coefficients data obtained from: USGS Publication (Lovelace, 2005)

Data output saved as csv, retaining assigned file name "USGS_WU_Coef_Raw.csv"
"""

from flowsa.common import *
import pandas as pd
from flowsa.flowbyactivity import store_flowbyactivity
from flowsa.flowbyfunctions import add_missing_flow_by_fields


# 2012--2018 fisheries data at state level
csv_load = datapath + "BEA_GDP_GrossOutput_IO.csv"


if __name__ == '__main__':
    # Read directly into a pandas df
    df_raw = pd.read_csv(csv_load)

    df = df_raw.rename(columns={'Unnamed: 0': 'ActivityProducedBy'})

    # use "melt" fxn to convert colummns into rows
    df = df.melt(id_vars=["ActivityProducedBy"],
                 var_name="Year",
                 value_name="FlowAmount")

    df = df[df['Year'] == '2011']
    # hardcode data
    df["Class"] = "Money"
    df['Description'] = 'BEA_2012_Detail_Code'
    df['FlowName'] = 'Gross Output'
    df["SourceName"] = "BEA_GDP"
    df["Location"] = US_FIPS
    df['LocationSystem'] = "FIPS_2015"  # state FIPS codes have not changed over last decade
    df["Unit"] = "USD"

    # add missing dataframe fields (also converts columns to desired datatype)
    flow_df = add_missing_flow_by_fields(df, flow_by_activity_fields)
    parquet_name = 'BEA_GDP_GrossOutput_IO_2011'
    store_flowbyactivity(flow_df, parquet_name)
