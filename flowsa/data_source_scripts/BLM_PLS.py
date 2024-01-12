# BLM_PLS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Supporting functions for importing and transforming
Bureau of Land Management Public Land Statistics data
"""

import re
import io
from tabula.io import read_pdf
import pandas as pd
from flowsa.location import get_all_state_FIPS_2
from flowsa.common import WITHDRAWN_KEYWORD
from flowsa.flowsa_log import log


def split(row, header, sub_header, next_line):
    """
    Helper function for parsing df
    :param row: df row
    :param header: df column header
    :param sub_header: df column subheader
    :param next_line: next row
    :return: data appropriately split
    """
    # ensure space after "Leases"
    row["one"] = (row["one"]
                  .replace("Leases", " Leases ")
                  .replace("  ", " ")
                  .strip()
                  )

    flow_amount = ""
    num_in_state = False
    split_str_one = row["one"].split(" ")
    split_str_two = ""
    if len(row) >= 3:
        if isinstance(row["two"], float):
            split_str_two = row["two"]
        else:
            split_str_two = row["two"].split(" ")
    column = 0
    if "—continued" in header:
        split_header = header.split("—continued")
        header = split_header[0]

    if sub_header == "":
        flow_name = header
    else:
        flow_name = header + ", " + sub_header

    if split_str_one[0] == "North" or split_str_one[0] == "South" or \
            split_str_one[0] == "West" or split_str_one[0] == "New":
        if any(i.isdigit() for i in split_str_one[1]):
            remove_number = re.sub(r'\d+', '', split_str_one[1])
            location_str = split_str_one[0] + " " + remove_number.lower()
            num_in_state = True
        else:
            location_str = split_str_one[0] + " " + split_str_one[1].lower()

        if num_in_state:
            if len(split_str_one) == 2:
                column = 1
            elif len(split_str_one) == 3:
                column = 4
            elif len(split_str_one) >= 4:
                column = 4
        else:
            if len(split_str_one) == 2:
                column = 2
            elif len(split_str_one) == 3:
                column = 1
            elif len(split_str_one) >= 4:
                if "Leases" in split_str_one[2]:
                    column = 1
                else:
                    if any(i.isdigit() for i in split_str_one[2]):
                        column = 5
                    elif any(i.isdigit() for i in split_str_one[3]):
                        column = 6
                    elif any(i.isdigit() for i in split_str_one[4]):
                        column = 7
                    else:
                        column = 5

    else:
        if any(i.isdigit() for i in split_str_one[0]):
            remove_number = re.sub(r'\d+', '', split_str_one[0])
            location_str = remove_number
            num_in_state = True
        else:
            location_str = split_str_one[0]
        if num_in_state:
            if len(split_str_one) == 1:
                column = 1
            elif len(split_str_one) == 2:
                column = 3
            elif len(split_str_one) >= 3:
                if "Leases" in split_str_one[1]:
                    column = 5
                else:
                    column = 3
        else:
            if len(split_str_one) == 1:
                column = 2
            elif len(split_str_one) == 2:
                column = 1
            elif len(split_str_one) >= 3:
                if "FFMC" in split_str_one:
                    column = 7
                elif "Leases" in split_str_one[2] or "III" in \
                        split_str_one[2] or "Act" in split_str_one:
                    if next_line:
                        column = 6
                    else:
                        if isinstance(split_str_two, float):
                            column = 2
                        else:
                            if (len(split_str_two) == 0) & \
                                    (split_str_one[2] == "Leases"):
                                column = 6
                            elif len(split_str_two) == 0:
                                column = 2
                            elif split_str_two[0] == "None":
                                column = 8
                            else:
                                column = 2
                elif "Relinquishment" in split_str_one:
                    column = 6
                elif next_line:
                    column = 5
                elif split_str_one[1] == "":
                    if len(split_str_one) == 3:
                        if "/" in split_str_one[2]:
                            column = 2
                        else:
                            column = 1
                    elif len(split_str_one) == 4:
                        column = 5
                    elif len(split_str_one) == 5:
                        column = 6
                    elif len(split_str_one) == 6:
                        if any(i.isdigit() for i in split_str_one[2]):
                            column = 5
                        else:
                            column = 6
                    elif len(split_str_one) == 7:
                        if "/" in split_str_one[2]:
                            column = 6
                        elif split_str_one[3]:
                            column = 7

                else:
                    if any(i.isdigit() for i in split_str_one[1]):
                        column = 4
                    elif any(i.isdigit() for i in split_str_one[2]):
                        column = 5
                    elif any(i.isdigit() for i in split_str_one[3]):
                        column = 6
                    elif any(i.isdigit() for i in split_str_one[4]):
                        column = 7
                    else:
                        column = 4
    if column == 1:
        if split_str_two == "":
            flow_amount = ""
        else:
            flow_amount = split_str_two[0]
    elif column == 2:
        if isinstance(split_str_two, list):
            flow_amount = split_str_two[1]
        elif isinstance(split_str_two, float):
            flow_amount = split_str_two
        else:
            flow_amount = ""
    elif column == 3:
        flow_amount = split_str_one[1]
    elif column == 4:
        flow_amount = split_str_one[2]
    elif column == 5:
        flow_amount = split_str_one[3]
    elif column == 6:
        flow_amount = split_str_one[4]
    elif column == 7:
        flow_amount = split_str_one[5]
    elif column == 8:
        flow_amount = split_str_one[6]
    elif column == 0:
        log.warning('FlowAmount is 0, check split() allocations.')

    if next_line:
        location_str = "Total"

    if flow_amount == "":
        flow_amount_no_comma = flow_amount
    elif pd.isna(flow_amount):
        flow_amount_no_comma = ""
    elif "," in flow_amount:
        flow_amount_no_comma = "".join(flow_amount.split(","))
    else:
        flow_amount_no_comma = float(flow_amount)
    return location_str, flow_name, flow_amount_no_comma


def blm_pls_URL_helper(*, build_url, year, config, **_):
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data
    is obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :param args: dictionary, arguments specified when running generateflowbyactivity.py
        generateflowbyactivity.py ('year' and 'source')
    :return: list, urls to call, concat, parse, format into
        Flow-By-Activity format
    """
    # initiate url list for blm pls
    urls = []
    if year == '2015':
        url_base = config['url']
        url = url_base["base_url_2015"]
    else:
        url = build_url

    file_name = config['file_name']
    url = url + file_name[year]
    urls.append(url)
    return urls


def blm_pls_call(*, resp, year, **_):
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param resp: Response, response from url call
    :param year: year
    :return: pandas dataframe of original source data
    """
    df = pd.DataFrame()
    sub_headers = {}

    skip = False
    last_row_header = ""
    next_line = False
    copy = False
    location_str = []
    flow_value = []
    flow_name = []
    number_of_sub_headers = 0

    duplicate_headers = [
        "Pre-Reform Act Future Interest Leases",
        "Reform Act Leases",
        "Reform Act Future Interest Leases"]

    if year == "2007":
        sub_headers = {
            "Oil and Gas Pre-Reform Act Leases":
                {"Public Domain": [99], "Acquired Lands": [99]},
            "Pre-Reform Act Future Interest Leases":
                {"Public Domain & Acquired Lands": [100, 109, 110]},
            "Reform Act Leases":
                {"Public Domain": [101, 110], "Acquired Lands": [101, 102]},
            "Reform Act Leases—continued": {"Acquired Lands": [111]},
            "Reform Act Future Interest Leases": {
                "Public Domain & Acquired Lands": [103],
                "Acquired Lands": [112]},
            "Competitive General Services Administration (GSA) "
            "Oil & Gas Leases": {"Public Domain": [103]},
            "Competitive Protective Leases":
                {"Public Domain & Acquired Lands": [103]},
            "Competitive National Petroleum Reserve—Alaska Leases":
                {"Public Domain": [104]},
            "Competitive Naval Oil Shale Reserve Leases":
                {"Public Domain": [104]},
            "Pre-EPAct Competitive Geothermal Leases":
                {"Public Domain & Acquired Lands": [104]},
            "EPAct Competitive Geothermal Leases":
                {"Public Domain & Acquired Lands": [104]},
            "Oil and Gas Pre-Reform Act Over-the-Counter Leases":
                {"Public Domain": [106], "Acquired Lands": [106, 107]},
            "Pre-Reform Act Simultaneous Leases":
                {"Acquired Lands": [108, 109]},
            "Summary: Pre-Reform Act Simultaneous Leases":
                {"Public Domain & Acquired Lands": [109]},
            "Geothermal Leases": {"Public Domain & Acquired Lands": [112]},
            "Private Leases": {"Acquired Lands": [114]},
            "Exchange Leases": {"Public Domain": [114]},
            "Renewal Leases": {"Public Domain": [114]},
            "Class III Reinstatement Leases": {"Public Domain": [115]},
            "Oil and Gas Special Act – Rights-of-Way of 1930":
                {"Public Domain": [115]},
            "Oil and Gas Special Act – Federal Farm Mortgage Corporation "
            "Act of 1934": {"Acquired Lands": [115]},
            "Oil and Gas Special Act – Texas Relinquishment Act of 1919":
                {"Acquired Lands": [115]},
            "Federal Coal Leases":
                {"Competitive Nonregional Lease-by-Application Leases": [122],
                 "Competitive Pre-Federal Coal Leasing Amendment Act "
                 "(FCLAA) Leases": [122],
                 "Competitive Regional Emergency/Bypass Leases": [122],
                 "Competitive Regional Leases": [123],
                 "Exchange Leases": [123],
                 "Preference Right Leases": [123]},

            "Coal Licenses": {"Exploration Licenses": [124],
                              "Licenses to Mine": [124]},
            "Logical Mining Units": {"None": [124]},
            "Combined Hydrocarbon Leases": {"None": [126]},
            "Phosphate Leases":
                {"Phosphate Competitive Leases": [126],
                 "Phosphate Fringe Acreage Noncompetitive Leases": [126],
                 "Phosphate Preference Right Leases": [126]},
            "Phosphate Use Permits": {"None": [127]},
            "Sodium Leases":
                {"Sodium Competitive Leases": [127],
                 "Sodium Fringe Acreage Noncompetitive Leases": [127],
                 "Sodium Preference Right Leases": [127]},
            "Sodium Use Permit": {"None": [127]},
            "Potassium Leases":
                {"Potassium Competitive Leases": [128],
                 "Potassium Fringe Acreage Noncompetitive Leases": [128],
                 "Potassium Preference Right Leases": [128]},
            "Gilsonite Leases":
                {"Gilsonite Competitive Leases": [128],
                 "Gilsonite Fringe Acreage Noncompetitive Lease": [129],
                 "Gilsonite Preference Right Leases": [129]},
            "Oil Shale Leases": {"Oil Shale R, D&D Leases": [129]},
            "Hardrock – Acquired Lands Leases":
                {"Hardrock Preference Right Leases": [130]},
            "Asphalt Competitive Leases": {"None": [130]}
        }
        competitive_page_numbers = [100, 101, 102]
        no_header_page_numbers = [123, 129]
    elif year == "2011":
        sub_headers = {
            "Oil and Gas Pre-Reform Act Leases":
                {"Public Domain": [111], "Acquired Lands": [111, 112]},
            "Pre-Reform Act Future Interest Leases":
                {"Public Domain and Acquired Lands": [113, 122]},
            "Reform Act Leases": {"Public Domain": [113, 123],
                                  "Acquired Lands": [123, 124]},
            "Reform Act Leases—continued": {"Acquired Lands": [114]},
            "Competitive General Services Administration (GSA) "
            "Oil and Gas Leases": {"Public Domain": [116]},
            "Competitive Protective Leases":
                {"Public Domain and Acquired Lands": [116]},
            "Competitive National Petroleum Reserve—Alaska Leases":
                {"Public Domain": [116]},
            "Competitive Naval Oil Shale Reserve Leases":
                {"Public Domain": [116]},
            "Pre-EPAct Competitive Geothermal Leases":
                {"Public Domain and Acquired Lands": [117]},
            "EPAct Competitive Geothermal Leases":
                {"Public Domain and Acquired Lands": [117]},
            "Oil and Gas Pre-Reform Act Over-the-Counter Leases":
                {"Public Domain": [119], "Acquired Lands": [119]},
            "Pre-Reform Act Simultaneous Leases—continued":
                {"Acquired Lands": [120, 121]},
            "Summary:  Pre-Reform Act Simultaneous Leases":
                {"Public Domain and Acquired Lands": [122]},
            "Reform Act Future Interest Leases": {"Acquired Lands": [125]},
            "Geothermal Leases": {"Public Domain and Acquired Lands": [125]},
            "Private Leases": {"Acquired Lands": [126]},
            "Exchange Leases": {"Public Domain": [126]},
            "Renewal Leases": {"Public Domain": [126, 127]},
            "Class III Reinstatement Leases": {"Public Domain": [127]},
            "Oil and Gas Special Act – Rights-of-Way of 1930":
                {"Public Domain": [127, 128]},
            "Oil and Gas Special Act – Federal Farm Mortgage "
            "Corporation Act of 1934": {"Acquired Lands": [128]},
            "Oil and Gas Special Act – Texas Relinquishment Act of 1919":
                {"Acquired Lands": [128]},
            "Federal Coal Leases": {
                "Competitive Nonregional Lease-by-Application Leases": [135],
                "Competitive Pre-Federal Coal Leasing Amendment "
                "Act (FCLAA) Leases": [135],
                "Competitive Regional Emergency/Bypass Leases": [135],
                "Competitive Regional Leases": [136],
                "Exchange Leases": [136],
                "Preference Right Leases": [136]},
            "Coal Licenses": {
                "Exploration Licenses": [137],
                "Licenses To Mine": [137]},
            "Logical Mining Units": {"None": [137]},
            "Combined Hydrocarbon Leases": {"None": [139]},
            "Phosphate Leases": {
                "Phosphate Competitive Leases": [139],
                "Phosphate Fringe Acreage Noncompetitive Leases": [139],
                "Phosphate Preference Right Leases": [139]},
            "Phosphate Use Permits": {"None": [139]},
            "Sodium Leases": {
                "Sodium Competitive Leases": [140],
                "Sodium Fringe Acreage Noncompetitive Leases": [140],
                "Sodium Preference Right Leases": [140]},
            "Sodium Use Permit": {"None": [140]},
            "Potassium Leases": {
                "Potassium Competitive Leases": [141],
                "Potassium Fringe Acreage Noncompetitive Leases": [141],
                "Potassium Preference Right Leases": [141]},
            "Gilsonite Leases": {
                "Gilsonite Competitive Leases": [142],
                "Gilsonite Fringe Acreage Noncompetitive Leases": [142],
                "Gilsonite Preference Right Leases": [142]},
            "Oil Shale RD&D Leases": {"None": [142]},
            "Hardrock – Acquired Lands Leases":
                {"Hardrock Preference Right Leases": [143]}
        }
        competitive_page_numbers = [113, 114]
        no_header_page_numbers = [136]
    elif year == "2012":
        sub_headers = {
            "Oil and Gas Pre-Reform Act Leases":
                {"Public Domain": [108], "Acquired Lands": [108, 109]},
            "Pre-Reform Act Future Interest Leases":
                {"Public Domain and Acquired Lands": [110, 119]},
            "Reform Act Leases": {"Public Domain": [110, 120],
                                  "Acquired Lands": [110]},
            "Reform Act Leases—continued": {"Acquired Lands": [111]},
            "Competitive General Services Administration (GSA) "
            "Oil and Gas Leases": {"Public Domain": [113]},
            "Competitive Protective Leases":
                {"Public Domain and Acquired Lands": [113]},
            "Competitive National Petroleum Reserve—Alaska Leases":
                {"Public Domain": [113]},
            "Competitive Naval Oil Shale Reserve Leases":
                {"Public Domain": [113]},
            "Pre-EPAct Competitive Geothermal Leases":
                {"Public Domain and Acquired Lands": [114]},
            "EPAct Competitive Geothermal Leases":
                {"Public Domain and Acquired Lands": [114]},
            "Oil and Gas Pre-Reform Act Over-the-Counter Leases":
                {"Public Domain": [116], "Acquired Lands": [116]},
            "Pre-Reform Act Simultaneous Leases": {"Public Domain": [117]},
            "Pre-Reform Act Simultaneous Leases—continued":
                {"Public Domain": [118], "Acquired Lands": [118]},
            "Summary: Pre-Reform Act Simultaneous Leases":
                {"Public Domain and Acquired Lands": [119]},
            "Reform Act Future Interest Leases": {"Acquired Lands": [122]},
            "Geothermal Leases": {"Public Domain and Acquired Lands": [122]},
            "Private Leases": {"Acquired Lands": [124]},
            "Exchange Leases": {"Public Domain": [124]},
            "Renewal Leases": {"Public Domain": [124, 125]},
            "Class III Reinstatement Leases": {"Public Domain": [125]},
            "Oil and Gas Special Act – Rights-of-Way of 1930":
                {"Public Domain": [125, 126]},
            "Oil and Gas Special Act – Federal Farm Mortgage Corporation "
            "Act of 1934": {"Acquired Lands": [126]},
            "Oil and Gas Special Act – Texas Relinquishment Act of 1919":
                {"Acquired Lands": [126]},
            "Federal Coal Leases": {
                "Competitive Nonregional Lease-by-Application Leases": [133],
                "Competitive Pre-Federal Coal Leasing Amendment Act "
                "(FCLAA) Leases": [133],
                "Competitive Regional Emergency/Bypass Leases": [133],
                "Competitive Regional Leases": [134],
                "Exchange Leases": [134],
                "Preference Right Leases": [134]},
            "Coal Licenses": {
                "Exploration Licenses": [135],
                "Licenses To Mine": [135]},
            "Logical Mining Units": {"None": [135]},
            "Combined Hydrocarbon Leases": {"None": [137]},
            "Phosphate Leases": {
                "Phosphate Competitive Leases": [137],
                "Phosphate Fringe Acreage Noncompetitive Leases": [137],
                "Phosphate Preference Right Leases": [137]},
            "Phosphate Use Permits": {"None": [137]},
            "Sodium Leases": {
                "Sodium Competitive Leases": [138],
                "Sodium Fringe Acreage Noncompetitive Leases": [138],
                "Sodium Preference Right Leases": [138]},
            "Sodium Use Permit": {"None": [138]},
            "Potassium Leases": {
                "Potassium Competitive Leases": [139],
                "Potassium Fringe Acreage Noncompetitive Leases": [139],
                "Potassium Preference Right Leases": [139]},
            "Gilsonite Leases": {
                "Gilsonite Competitive Leases": [140],
                "Gilsonite Fringe Acreage Noncompetitive Leases": [140],
                "Gilsonite Preference Right Leases": [140]},
            "Oil Shale RD&D Leases": {"None": [140]},
            "Hardrock – Acquired Lands Leases":
                {"Hardrock Preference Right Leases": [141]}
        }
        competitive_page_numbers = [110, 111]
        no_header_page_numbers = [134]
    else:
        # provide reasoning for failure of parsing data
        log.error('Missing code specifying sub-headers, '
                  'add code to blm_pls_call()')

    for header in sub_headers:
        for sub_header in sub_headers[header]:
            pg = sub_headers[header][sub_header]
            pdf_pages = []
            for page_number in pg:
                found_header = False

                pdf_page = read_pdf(io.BytesIO(resp.content),
                                    pages=page_number, stream=True,
                                    guess=False)[0]
                # drop nan rows
                pdf_page = (pdf_page
                            .dropna(axis=0, how='all')
                            .reset_index(drop=True))

                page_shape = pdf_page.shape[1]
                if page_shape == 1:
                    pdf_page.columns = ["one"]
                else:
                    pdf_page.columns = ["one", "two"]

                pdf_page.dropna(subset=["one"], inplace=True)
                # add col of page number
                pdf_page['page_no'] = page_number
                pdf_pages.append(pdf_page)

            for page in pdf_pages:
                for index, row in page.iterrows():
                    if " /" in row["one"]:
                        split_header = row["one"].split(" /")
                        split_row = split_header[0].strip()
                    else:
                        split_row = row["one"]
                    # if page_number in no_header_page_numbers:
                    if row['page_no'] in no_header_page_numbers:
                        # if pages in no_header_page_numbers:
                        found_header = True
                    if split_row == header:
                        found_header = True
                        last_row_header = header
                    if split_row == sub_header and last_row_header == header:
                        copy = True
                    elif sub_header == "None" and last_row_header == header:
                        copy = True

                    if copy and split_row != sub_header and \
                            split_row != header and found_header:
                        if "FISCAL" in row["one"] or row["one"].isdigit():
                            skip = True

                        if not skip:
                            if sub_header == "None":
                                sub_header = ""
                            lists = split(row, header, sub_header, next_line)
                            if header in duplicate_headers:
                                # if page_number in competitive_page_numbers:
                                if row['page_no'] in competitive_page_numbers:
                                    flow_name.append(f"Competitive {lists[1]}")
                                else:
                                    flow_name.append(f"Noncompetitive "
                                                     f"{lists[1]}")
                            else:
                                flow_name.append(lists[1])
                            location_str.append(lists[0])
                            flow_value.append(lists[2])
                            if next_line:
                                copy = False
                                next_line = False
                                header = "Nothing"
                            if "Total" in row["one"]:
                                row_one_str = ""
                                if any(i.isdigit() for i in row["one"]):
                                    #   row split based on space
                                    row_one_split = row["one"].split(" ")
                                    for r in row_one_split:
                                        if not any(d.isdigit() for d in r):
                                            row_one_str = row_one_str + " " + r
                                else:
                                    row_one_str = row["one"]

                                if page_shape == 1 and \
                                        row["one"] == "Total":
                                    next_line = True
                                elif row_one_str.strip() == "Total" or \
                                        "Leases" in row["one"] or "None" in \
                                        row["one"]:
                                    number_of_sub_headers = \
                                        number_of_sub_headers + 1
                                    copy = False
                                    found_header = False
                                else:
                                    next_line = True
                        if sub_header + "—continued" in row["one"]:
                            skip = False

    df["LocationStr"] = location_str
    df["ActivityConsumedBy"] = flow_name
    df["FlowAmount"] = flow_value

    # drop where flowamount is null, at times due to duplicated info
    df = df.query("FlowAmount != ''").reset_index(drop=True)

    return df


def blm_pls_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    Location = []
    fips = get_all_state_FIPS_2()
    df = pd.concat(df_list, sort=False)
    df = df.drop(df[df.FlowAmount == ""].index)
    for index, row in df.iterrows():
        if row['LocationStr'] == "Total":
            Location.append("00000")
        else:
            for i, fips_row in fips.iterrows():
                if fips_row["State"] == row['LocationStr']:
                    Location.append(fips_row["FIPS_2"] + "000")
    df = df.drop(columns=["LocationStr"])

    # standardize activity names
    df = standardize_blm_pls_activity_names(df)

    # replace withdrawn code
    df.loc[df['FlowAmount'] == "Q", 'FlowAmount'] = WITHDRAWN_KEYWORD
    df.loc[df['FlowAmount'] == "N", 'FlowAmount'] = WITHDRAWN_KEYWORD
    df['FlowName'] = df['ActivityConsumedBy'].copy()
    df['Location'] = Location
    df["Class"] = 'Land'
    df['Compartment'] = "ground"
    df["LocationSystem"] = "FIPS_2010"
    df["SourceName"] = 'BLM_PLS'
    df['Year'] = year
    df['Unit'] = "Acres"
    df['FlowType'] = 'ELEMENTARY_FLOW'
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df


def standardize_blm_pls_activity_names(df):
    """
    Over the years, BLM PLS activities have minor syntax differences.
    Standardize the names over the years
    :param df: df, BLM PLS
    :return: df, BLM PLS with standardized activity names
    """

    standardize_column = 'ActivityConsumedBy'

    df[standardize_column] = df[standardize_column].apply(
        lambda x: re.sub(' & ', ' and ', x))
    df[standardize_column] = df[standardize_column].apply(
        lambda x: re.sub(' to ', ' To ', x))
    df[standardize_column] = df[standardize_column].apply(
        lambda x: re.sub('\bLease\b', 'Leases', x))
    df[standardize_column] = df[standardize_column].apply(
        lambda x: re.sub(':  ', ': ', x))

    # standardize dashes so do not corrupt in csvs
    df[standardize_column] = df[standardize_column].apply(
        lambda x: re.sub('–', '-', x))
    df[standardize_column] = df[standardize_column].apply(
        lambda x: re.sub('—', '-', x))
    return df
