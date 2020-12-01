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


def allocate_usda_ers_mlu_land_in_urban_areas(df_load, attr, fbs_list):
    """
    This function is used to allocate the USDA_ERS_MLU activity 'land in urban areas' to NAICS 2012 sectors. Allocation
    is dependent on assumptions defined in 'values_from_literature.py' as well as results from allocating
    'EIA_CBECS_Land' and 'EIA_MECS_Land' to land based sectors.

    Methodology is based on the manuscript:
    Lin Zeng and Anu Ramaswami
    Impact of Locational Choices and Consumer Behaviors on Personal Land Footprints:
    An Exploration Across the Urban–Rural Continuum in the United States
    Environmental Science & Technology 2020 54 (6), 3091-3102
    DOI: 10.1021/acs.est.9b06024

    :param df_load:
    :return:
    """

    # todo: need to group everything by location and total value because likely need to have state version

    from flowsa.values_from_literature import get_urban_land_use_for_airports, get_urban_land_use_for_railroads

    # tmp to test if method works
    # allocated_urban_areas_df = df_load.copy()

    # test
    df_load = flow_subset_mapped.copy()

    # define sector column to base calculations
    sector_col = 'SectorConsumedBy'
    # create allocations at the 6 digit NAICS (aggregate later if necessary)
    df = df_load[df_load[sector_col].apply(lambda x: len(x) == 6)].reset_index(drop=True)

    # read in the cbecs and mecs df from df_list
    for df_i in fbs_list:
        if df_i['Context'].all() == 'resource/ground/human-dominated/Commercial':
            cbecs = df_i
        elif df_i['Context'].all() == 'resource/ground/human-dominated/industrial':
            mecs = df_i

    # calculate total residential area from the American Housing Survey
    # todo: base calculation off AHS df, not tmp assumption
    df_residential = df[df[sector_col] == 'F01000']
    # temp residential multiplier
    residential_multiplier = 0.6
    df_residential = df_residential.assign(FlowAmount=df_residential['FlowAmount'] * residential_multiplier)

    # make an assumption about the percent of urban area that is open space

    # determine commercial land area from prior calculation

    # calculate total urban transportation by subtrating calculated areas from total urban land (including land for
    # commercial and manufactured buildings
    # df_tut

    # make an assumption about the percent of urban area used by airports
    airport_multiplier = get_urban_land_use_for_airports()
    df_airport = df[df[sector_col] == '488119']
    df_airport.loc[:, 'FlowAmount'] = df_airport['FlowAmount'] * airport_multiplier

    # make an assumption about the percent of urban area used by railroads
    railroad_multiplier = get_urban_land_use_for_railroads()
    df_railroad = df[df[sector_col] == '482112']
    df_railroad.loc[:, 'FlowAmount'] = df_railroad['FlowAmount'] * railroad_multiplier

    # further allocate the remaining urban transportation area using Federal Highway Administration fees

    # concat all df subsets
    allocated_urban_areas_df = pd.concat([df_airport, df_railroad], sort=False).reset_index(drop=True)

    return allocated_urban_areas_df


def allocate_usda_ers_mlu_land_in_rural_transportation_areas(df_load, attr):
    """
    This function is used to allocate the USDA_ERS_MLU activity 'land in urban areas' to NAICS 2012 sectors. Allocation
    is dependent on assumptions defined in 'values_from_literature.py' as well as results from allocating
    'EIA_CBECS_Land' and 'EIA_MECS_Land' to land based sectors.

    Methodology is based on the manuscript:
    Lin Zeng and Anu Ramaswami
    Impact of Locational Choices and Consumer Behaviors on Personal Land Footprints:
    An Exploration Across the Urban–Rural Continuum in the United States
    Environmental Science & Technology 2020 54 (6), 3091-3102
    DOI: 10.1021/acs.est.9b06024

    :param df_load:
    :return:
    """

    # tmp to test if method works
    allocated_urban_areas_df = df_load.copy()

    return allocated_urban_areas_df
