# EIA_CBECS_Land.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import pandas as pd
import numpy as np
import tabula
import io
from flowsa.common import *
from flowsa.flowbyfunctions import assign_fips_location_system


"""
2012 Commercial Buildings Energy Consumption Survey (CBECS)
https://www.eia.gov/consumption/commercial/reports/2012/energyusage/index.php 
Last updated: Monday, August 17, 2020
"""
def blm_pls_URL_helper(build_url, config, args):
    """This helper function uses the "build_url" input from flowbyactivity.py, which is a base url for coa cropland data
    that requires parts of the url text string to be replaced with info specific to the usda nass quickstats API.
    This function does not parse the data, only modifies the urls from which data is obtained. """
    # initiate url list for coa cropland data
    urls = []
    year = args["year"]
    if year == '2015':
        url_base = config['url']
        url = url_base["base_url_2015"]
    else:
        url = build_url

    file_name = config['file_name']
    url = url + file_name[year]
    urls.append(url)
    return urls

def blm_pls_call(url, response_load, args):
    dataframe = pd.DataFrame()


    # define pages to extract data from
    pages = range(99, 114)

    # Read pdf into list of DataFrame
    df_list = []
    for x in pages:
        df = tabula.read_pdf(io.BytesIO(response_load.content), pages=x, stream=True)[0]
    oil_and_gas_pre_reform_act_leases = False
    pre_reform_act_leases_public_domain = False
    pre_reform_act_leases_acquired_lands = False
    pre_reform_act_future_interest_leases = False

    df.columns = ["one", "two", "three", "four"]
    df = df.drop(columns=["two", "three"])
    df.dropna(subset=["one"], inplace=True)
    LocationStr = []

    FlowName = []
    FlowAmount = []
    FlowAmount_No_Comma = []

    for index, row in df.iterrows():
        if row["one"] == "Public Domain":
            pre_reform_act_leases_public_domain = True
        if row["one"] == "Acquired Lands":
            pre_reform_act_leases_acquired_lands = True
        if row["one"] == "Pre-Reform Act Future Interest Leases":
            pre_reform_act_leases_acquired_lands = True




        if row["one"] != "Public Domain" and row["one"] != "Acquired Lands":
            if pre_reform_act_leases_public_domain:
                split_str = row["one"].split(" ")
                if split_str[0] == "North" or split_str[0] == "South" or split_str[0] == "West" or split_str[0] == "New":
                    LocationStr.append(split_str[0] + " " + split_str[1].lower())
                    FlowName.append("Oil and gas pre-Reform Act leases, public domain")
                    FlowAmount.append(split_str[3])
                else:
                    LocationStr.append(split_str[0])
                    FlowName.append("Oil and gas pre-Reform Act leases, public domain")
                    FlowAmount.append(split_str[2])
                    if split_str[0] == "Total":
                        pre_reform_act_leases_public_domain = False
            if pre_reform_act_leases_acquired_lands:
                split_str = row["one"].split(" ")
                if split_str[0] == "North" or split_str[0] == "South" or split_str[0] == "West" or split_str[0] == "New":
                    LocationStr.append(split_str[0] + " " + split_str[1].lower())
                    FlowName.append("Oil and gas pre-Reform Act leases, acquired land")
                    FlowAmount.append(split_str[3])
                else:
                    LocationStr.append(split_str[0])
                    FlowName.append("Oil and gas pre-Reform Act leases, acquired land")
                    FlowAmount.append(split_str[2])
                    if split_str[0] == "Total":
                        pre_reform_act_leases_acquired_lands = False

    for i in range(len(FlowAmount)):
        if "," in FlowAmount[i]:
            FlowAmount_No_Comma.append("".join(FlowAmount[i].split(",")))
        else:
            FlowAmount_No_Comma.append(FlowAmount[i])
    dataframe["LocationStr"] = LocationStr
    dataframe["FlowName"] = FlowName
    dataframe["FlowAmount"] = FlowAmount_No_Comma
    return dataframe


def blm_pls_parse(dataframe_list, args):
    Location = []
    fips = get_all_state_FIPS_2()
    for df in dataframe_list:
        for index, row in df.iterrows():
            if (row['LocationStr'] == "Total"):
                Location.append("00000")
            else:
                for i, fips_row in fips.iterrows():
                    if (fips_row["State"] == row['LocationStr']):
                        Location.append(fips_row["FIPS_2"] + "000")
        df = df.drop(columns=["LocationStr"])

        # replace withdrawn code
        df.loc[df['FlowAmount'] == "Q", 'FlowAmount'] = withdrawn_keyword
        df.loc[df['FlowAmount'] == "N", 'FlowAmount'] = withdrawn_keyword
        df['Location'] = Location
        df["Class"] = 'Land'
        df["SourceName"] = 'BLM_Public_Land_Statistics'
        df['Year'] = args["year"]
        df['Unit'] = "Acres"
    return df

