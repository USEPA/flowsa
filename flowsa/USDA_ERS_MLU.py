# USDA_CoA_Cropland.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import pandas as pd
import numpy as np
import io
from flowsa.common import *
from flowsa.flowbyfunctions import assign_fips_location_system

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
    output = pd.DataFrame()
    # concat dataframes
    df = pd.concat(dataframe_list, sort=False)
    data = {}
    df_columns = df.columns.tolist()
    location = ""

    fips = get_all_state_FIPS_2()
    for index, row in df.iterrows():
        if int(row["Year"]) == int(args['year']):
            if (row["Region or State"] != "Northeast") & (row["Region or State"] != "Lake States") & \
                    (row["Region or State"] != "Corn Belt") & (row["Region or State"] != "Northern Plains") & \
                    (row["Region or State"] != "Appalachian") & (row["Region or State"] != "Southeast") & \
                    (row["Region or State"] != "Delta States") & (row["Region or State"] != "Southern Plains") & \
                    (row["Region or State"] != "Mountain") & (row["Region or State"] != "Pacific") & \
                    (row["Region or State"] != "48 States"):
                if row['Region or State'] == "U.S. total":
                    location = "00000"
                else:
                    for i, fips_row in fips.iterrows():
                        if fips_row["State"] == row['Region or State']:
                            location = fips_row["FIPS_2"] + "000"

                for col in df_columns:
                    if (col != "SortOrder") & (col != "Region") & (col != "Region or State") & (col != "Year"):
                        data["Class"] = "Land"
                        data["SourceName"] = "USDA_ERS_MLU"
                        # flownames are the same as ActivityConsumedBy for purposes of mapping elementary flows
                        data['FlowName'] = col
                        data["FlowAmount"] = int(row[col])
                        data["ActivityProducedBy"] = None
                        data["ActivityConsumedBy"] = col
                        data['FlowType'] = 'ELEMENTARY_FLOW'
                        data["Compartment"] = 'ground'
                        data["Location"] = location
                        data["Year"] = int(args['year'])
                        data["Unit"] = "Thousand Acres"
                        output = output.append(data, ignore_index=True)
    output = assign_fips_location_system(output, args['year'])

    return output
