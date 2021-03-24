# write_FBA_USGS_WU_Coef.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Animal Water Use coefficients data obtained from: USGS Publication (Lovelace, 2005)

Data output saved as csv, retaining assigned file name "USGS_WU_Coef_Raw.csv"
"""


from flowsa.common import *
import pandas as pd
from flowsa.flowbyactivity import process_data_frame
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.common import fba_fill_na_dict
from flowsa.dataclean import clean_df

# add info to be read when creating a bibliography
author = 'US Geological Survey'
source_name = 'Method for Estimating Water Withdrawals for Livestock in the United States, 2005'
citable_url = 'https://pubs.er.usgs.gov/publication/sir20095041'

# 2012--2018 fisheries data at state level
csv_load = externaldatapath + "USGS_WU_Coef_Raw.csv"


if __name__ == '__main__':
    # Read directly into a pandas df
    df_raw = pd.read_csv(csv_load)

    # rename columns to match flowbyactivity format
    df = df_raw.copy()
    df = df.rename(columns={"Animal Type": "ActivityConsumedBy",
                            "WUC_Median": "FlowAmount",
                            "WUC_Minimum": "Min",
                            "WUC_Maximum": "Max"
                            })

    # drop columns
    df = df.drop(columns=["WUC_25th_Percentile", "WUC_75th_Percentile"])

    # hardcode data
    df["Class"] = "Water"
    df["SourceName"] = "USGS_WU_Coef"
    df["Location"] = US_FIPS
    df['Year'] = 2005
    df = assign_fips_location_system(df, '2005')
    df["Unit"] = "gallons/animal/day"

    # add missing dataframe fields (also converts columns to desired datatype)
    flow_df = clean_df(df, flow_by_activity_fields, fba_fill_na_dict, drop_description=False)
    parquet_name = 'USGS_WU_Coef'
    process_data_frame(flow_df, parquet_name, '2005')
