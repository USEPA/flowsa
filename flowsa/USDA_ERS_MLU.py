# USDA_CoA_Cropland.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import pandas as pd
import numpy as np
import io
from flowsa.common import *

"""
USDA Economic Research Service (ERS) Major Land Uses (MLU)
https://www.ers.usda.gov/data-products/major-land-uses/
Last updated: Thursday, April 16, 2020
"""



def mlu_call(url, mlu_response, args):
    with io.StringIO(mlu_response.text) as fp:
       # for line in fp:
        #    if line[0] != '#':
         #       if "16s" not in line:
        df = pd.read_csv(fp, encoding="ISO-8859-1")
    return df

def mlu_parse(dataframe_list, args):
    """Class: Land
    SourceName: USDA_ERS_MLU
    FlowName: None
    ActivityProducedBy: None
    ActivityConsumedBy: column headers
    Compartment: None"""
    # concat dataframes
    df = pd.concat(dataframe_list, sort=False)
    data = pd.DataFrame(columns=flow_by_activity_fields)
    # select data for chosen year, cast year as string to match argument
    df['Year'] = df['Year'].astype(str)
    if(df[df['Year'] == args['year']]):
        data["Class"]="Land"

    return data


