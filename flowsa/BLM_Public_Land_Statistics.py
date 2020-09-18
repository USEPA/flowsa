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


def split(row, header, sub_header):
    LocationStr = ""
    FlowName = ""
    FlowAmount = ""
    FlowAmount_No_Comma = ""
    df = pd.DataFrame()
    split_str_one = row["one"].split(" ")

    FlowName = header + ", " + sub_header

    if split_str_one[0] == "North" or split_str_one[0] == "South" or split_str_one[0] == "West" or \
            split_str_one[0] == "New":
        LocationStr = split_str_one[0] + " " + split_str_one[1].lower()
    else:
        LocationStr = split_str_one[0]

    if isinstance(row["two"], str):
        split_str_two = row["two"].split(" ")
        FlowAmount = split_str_two[0]
    else:
        FlowAmount_No_Comma = row["two"]
    if "," in FlowAmount:
        FlowAmount_No_Comma = "".join(FlowAmount.split(","))
    else:
        FlowAmount_No_Comma = float(FlowAmount)

    return LocationStr, FlowName, FlowAmount_No_Comma


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
    df = pd.DataFrame()
    df_list = []
    LocationStr = []
    FlowName = []
    FlowAmount = []
    FlowAmount_No_Comma = []
    pdf_pages = []

#, "Acquired Lands"
    sub_headers = {
                "Pre-Reform Act Future Interest Leases": ["Public Domain & Acquired Lands"]
}





    if args["year"] == "2007":
        #pages = [99, 100, 101, 102, 103, 104, 106, 107, 108, 109, 110, 111, 112, 114, 115, 122, 123, 124, 126, 127, 128,
                # 129, 130]
        #pages = [99, 100, 101, 102]
        pages = [100]
        copy = False
        skip = False
        header = ""
        sub_header = ""
        sub_head = False
        data_frame_list = []
        location_str = []
        flow_value = []
        flow_name = []
        for x in pages:
            pdf_page = tabula.read_pdf(io.BytesIO(response_load.content), pages=x, stream=True, guess=False,)[0]
            pdf_page.columns = ["one", "two"]
            pdf_page.dropna(subset=["one"], inplace=True)
            pdf_pages.append(pdf_page)

        for page in pdf_pages:
            for index, row in page.iterrows():
               for item in sub_headers:
                    if row["one"] == item:
                        header = row["one"]
                    if row["one"] in sub_headers[item] and header == item:
                        sub_header = row["one"]
                        copy = True

                    if copy == True :
                        if header == item:
                            for x in sub_headers[item]:
                                if sub_header == x:
                                    if "FISCAL" in row["one"]:
                                        skip = True
                                    if sub_header is not row["one"] and skip is not True:
                                        lists = split(row, header, sub_header)
                                        location_str.append(lists[0])
                                        flow_name.append(lists[1])
                                        flow_value.append(lists[2])
                                      #  data_frame_list.append(df)
                                        if "Total" in row["one"]:
                                            copy = False
                                    if sub_header + "—continued" in row["one"]:
                                        skip = False

    df["LocationStr"] = location_str
    df["FlowName"] = flow_name
    df["FlowAmount"] = flow_value
    return df


def blm_pls_parse(dataframe_list, args):
    Location = []
    fips = get_all_state_FIPS_2()
    for df in dataframe_list:
        df = df.drop(df[df.FlowAmount == ""].index)
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

