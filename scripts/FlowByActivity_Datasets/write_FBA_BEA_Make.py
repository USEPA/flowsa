# write_FBA_BEA_Make.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Generation of BEA Make data as FBA
"""

from flowsa.common import *
import pandas as pd
from flowsa.flowbyactivity import process_data_frame
from flowsa.dataclean import add_missing_flow_by_fields
from flowsa.flowbyfunctions import assign_fips_location_system

year = 2012
level = 'Detail'
csv_load = datapath + "BEA_"+str(year)+"_"+level+"_Make_BeforeRedef.csv"


def bea_make_parse_afterredef(year, level):
    # concat dataframes - tmp load from uploaded csv
    # df = pd.concat(dataframe_list, sort=False)
    df_load = pd.read_csv(datapath + "BEA_2002_Detail_Make_AfterRedef.csv", dtype="str")
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
    df['SourceName'] = 'BEA_Make_Table'
    df['Unit'] = 'USD'
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, args['year'])
    df['FlowName'] = 'Gross Output Producer Value After Redef'
    return df

def bea_make_parse_beforeredef(year, level):
    # Read directly into a pandas df
    df_raw = pd.read_csv(csv_load)

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
    df["SourceName"] = "BEA_Make"
    df["Location"] = US_FIPS
    df['LocationSystem'] = "FIPS_2015"
    df['FlowAmount'] = df['FlowAmount'] * 1000000  # original unit in million USD
    df["Unit"] = "USD"
    return df

if __name__ == '__main__':


    # add missing dataframe fields (also converts columns to desired datatype)
    flow_df = add_missing_flow_by_fields(df, flow_by_activity_fields)
    parquet_name = 'BEA_Make_'+level+'_BeforeRedef_'+str(year)
    process_data_frame(flow_df, parquet_name, str(year))
