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
        if(int(row["Year"]) == int(args['year'])):
            if(row["Region or State"] != "Northeast" and row["Region or State"] != "Lake States" and
                    row["Region or State"] != "Corn Belt" and row["Region or State"] != "Northern Plains" and
                    row["Region or State"] != "Appalachian" and row["Region or State"] != "Southeast" and
                    row["Region or State"] != "Delta States" and row["Region or State"] != "Southern Plains" and
                    row["Region or State"] != "Mountain" and row["Region or State"] != "Pacific" and
                    row["Region or State"] != "48 States"):
                if(row['Region or State'] == "U.S. total"):
                    location = "00000"
                else:
                    for i, fips_row in fips.iterrows():
                        if(fips_row["State"] == row['Region or State']):
                            location = fips_row["FIPS_2"] + "000"

                for col in df_columns:
                    if(col != "SortOrder" and col != "Region" and col != "Region or State" and col != "Year"):
                        data["Class"] = "Land"
                        data["SourceName"] = "USDA_ERS_MLU"
                        data["FlowAmount"] = int(row[col])
                        data["ActivityProducedBy"] = None
                        data["ActivityConsumedBy"] = col
                        data["Compartment"] = 'ground'
                        data["Location"] = location
                        data["Year"] = int(args['year'])
                        data["Unit"] = "Thousand Acres"
                        output = output.append(data, ignore_index=True)
    output = assign_fips_location_system(output, args['year'])

    # hardcode flownames based on file names of excel workbooks
    output.loc[output['ActivityConsumedBy'] == 'Total land', 'FlowName'] = 'Total land'
    output.loc[output['ActivityConsumedBy'] == 'Total cropland', 'FlowName'] = 'Cropland'
    output.loc[output['ActivityConsumedBy'].isin(['Cropland used for crops',
                                                  'Cropland used for pasture',
                                                  'Cropland idled'
                                                  ]), 'FlowName'] = 'Cropland components'
    output.loc[output['ActivityConsumedBy'] == 'Grassland pasture and range', 'FlowName'] = 'Grassland pasture and range'
    output.loc[output['ActivityConsumedBy'] == 'Forest-use land (all)', 'FlowName'] = 'Forest-use land'
    output.loc[output['ActivityConsumedBy'].isin(['Forest-use land grazed',
                                                  'Forest-use land not grazed'
                                                  ]), 'FlowName'] = 'Forest-use land components'
    output.loc[output['ActivityConsumedBy'] == 'All special uses of land', 'FlowName'] = 'Special uses'
    output.loc[output['ActivityConsumedBy'].isin(['Land in rural transportation facilities',
                                                  'Land in rural parks and wildlife areas',
                                                  'Land in defense and industrial areas',
                                                  'Farmsteads, roads, and miscellaneous farmland'
                                                  ]), 'FlowName'] = 'Special use components'
    output.loc[output['ActivityConsumedBy'] == 'Land in urban areas', 'FlowName'] = 'Urban area'
    output.loc[output['ActivityConsumedBy'] == 'Other land', 'FlowName'] = 'Other or miscellaneous land uses'

    return output
