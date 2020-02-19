# data_pull.py (usgs_water_consume)
# !/usr/bin/env python3
# coding=utf-8
# ingwersen.wesley@epa.gov

import os
import io
import requests
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile

"""
Classes and methods for pulling data from a USGS web service.

Available functions:
-
"""


def generate_urls():
    """This method generates the urls that will be called
    This includes all states and DC and over 2 years 2010 and 2015"""
    state_abrevation = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]
    year = [2010, 2015]
    url_list = []
    for y in year:
        for s in state_abrevation:
            url = " https://waterdata.usgs.gov/" + s + "/nwis/water_use?format=rdb&rdb_compression=value&wu_area=County&wu_year=" + str(y) + "&wu_county=ALL&wu_category=ALL"
            url_list.append(url)
    return url_list


def process_data(unit_list, index_list, source_name_list, data, general_list):
     """This method adds in the data be used for the Flow by activity."""
     year_index = general_list.index("year")
     state_cd_index = general_list.index("state_cd")
     county_cd_index = general_list.index("county_cd")
     for d in data:
        data_list = d.split("\t")
        year = data_list[year_index]
        fips = str(data_list[state_cd_index]) + str(data_list[county_cd_index])
        for i in range(len(source_name_list)):
            source_name = source_name_list[i]
            flow_name = "null"
            flow_amount = data_list[index_list[i]]
            unit = unit_list[i]
            activity = "null"
            reliablity_score = "null"
            data_collection = "null"
            compartment = "null"
            flow_type = "null"
            print(source_name, flow_name, flow_amount, unit, activity, reliablity_score, data_collection, compartment, fips, flow_type, year)


def split_source_and_flow_names(line):
     """This method takes the source and flow header data and splits it so it can be used for the Flow by activity."""
     source_name = ""
     flow_name = ""
     if ")" in line:
        para_split = line.split(")")
        source_name = para_split[0].strip() + ")"
        flow_name = para_split[1].strip()
     else:
        space_split = line.split(" ")
        source_name = ""
        flow_name = ""
        captial_flag = True
        for s in space_split:
            if s[0].isupper() == True:
                if captial_flag == True:
                    source_name = source_name.strip() + " " + s
                else:
                    flow_name = flow_name.strip() + " " + s
            else:
                flow_name = flow_name.strip() + " " + s
        # source_name = source_name
        # flow_name_list. append(flow_name.strip())
        return source_name, flow_name

# def split_header(headers, data):
#     """This method takes the header data and splits it so it can be used for the Flow by activity."""
#     headers_list = headers.split("\t")
#     comma_count = []
#     source_name_list = []
#     flow_name_list = []
#     unit_list = []
#     index_list = []
#     general_list = []
#     index = 0
#     for h in headers_list:
#         if "_" in h:
#           general_list.append(h)

#         else:
#             comma_split = h.split(",")
#             comma_count.append(len(comma_split))
#             if len(comma_split) == 1:
#                 if "facilities" in h:
#                     source_flow_name = split_source_and_flow_names(h.strip())
#                     source_name_list.append(source_flow_name[0])
#                     flow_name_list.append(source_flow_name[1])
#                     unit_list.append("decimal integer")
#                     index_list.append(index)

#                 elif "in " in h:
#                      in_split = h.split("in")
#                      source_flow_name = split_source_and_flow_names(in_split[0].strip())
#                      source_name_list.append(source_flow_name[0])
#                      flow_name_list.append(source_flow_name[1])
#                      unit_list.append(in_split[1].strip())
#                      index_list.append(index)
#                 else:
#                     general_list.append(h)

            # if len(comma_split) == 2:

            #     if "in" in h:
            #          space_split = comma_split[0].split(" ")
            #          source_name = ""
            #          flow_name = ""
            #          captial_flag = True
            #          for s in space_split:
            #               if s[0].isupper() == True:
            #                   if captial_flag == True:
            #                       source_name = source_name.strip() +" " + s
            #                   else:
            #                       flow_name = flow_name.strip() +" " + s
            #               else:
            #                   flow_name = flow_name.strip() +" " + s
            #          source_name_list.append(source_name.strip())
            #          flow_name_list. append(flow_name.strip())

            #          in_split = h.split("in ")
            #          unit_list.append(in_split[1].strip())
            #          index_list.append(index)

            # elif len(comma_split) == 3:
            #       in_split = h.split("in ")
            #       source_name_list.append(in_split[0].strip())
            #       unit_list.append(in_split[1].strip())
            #       index_list.append(index)

            # elif len(comma_split) == 4:
            #      in_split = h.split("in ")
            #      source_name_list.append(in_split[0].strip())
            #      unit_list.append(in_split[1].strip())
            #      index_list.append(index)
        # index += 1

    # process_data(unit_list, index_list, source_name_list, data, general_list)

    # for i in range(len(source_name_list)):
    #     #print( unit_list[i] +"\t" + str(index_list[i]) +"\t"  +names_list[i])
    #     print(source_name_list[i])

    # print("*************************************************************")
    # for i in range(len(flow_name_list)):
    #     print(flow_name_list[i])


def parse_header(headers):
    """This method will create a header_list and an index_list and a general list.
    The header list will contain the headers that deal with water headers.
    Water headers are currently defined as a header with m/g as a unit.
    The index list will contain the indexes that corospond to the selected headers. This will be used for parsing the data.
    The general list will contain the headers that deal with the headers that will be needed for each other parts of the flow by activtivity list such as year"""
    headers_list = headers.split("\t")
    general_list = []
    comma_count = []
    headers_list = []
    for h in headers_list:
        # get all headers that have a units.
        if "Mgal"in h:
            headers_list.append[h]
          i = 0
        else:
            if comma_count == 1:
                print(h)

        # if "_" in h:
        #   general_list.append(h)
        # else:
        #     comma_split = h.split(",")
        #     comma_count.append(len(comma_split))
        #     if len(comma_split) == 1:
        #         # if "in " in h:
        #         #     in_split = h.split("in")
        #         print(h)


                # if "facilities" not in h:
                #     source_flow_name = split_source_and_flow_names(h.strip())
                #     source_name_list.append(source_flow_name[0])
                #     flow_name_list.append(source_flow_name[1])
                #     unit_list.append("decimal integer")
                #     index_list.append(index)

                # elif "in " in h:
                #         in_split = h.split("in")
                #         source_flow_name = split_source_and_flow_names(in_split[0].strip())
                #         source_name_list.append(source_flow_name[0])
                #         flow_name_list.append(source_flow_name[1])
                #         unit_list.append(in_split[1].strip())
                #         index_list.append(index)
                # else:
                #     general_list.append(h)



def get_header_general_and_data(text):
    """This method takes the file data line by line and seperates it into 3 catigories
        Metadata - these are the comments at the begining of the file.
        Header data - the headers for the data.
        Data - the actuall data for each of the headers.
        This method also eliminats the table metadata. The table metadata declares how long each string in the table can be."""
    flag = True
    header = ""
    column_size = ""
    data = []
    metadata = []
    with io.StringIO(text) as fp:
        for line in fp:
            if line[0] != '#':
                 if flag == True:
                    header = line
                    flag = False
                 else:
                     if "16s" in line:
                         column_size = line
                     else:
                         data.append(line)
            else:
                metadata.append(line)
    parse_header(header)
    #split_header(header, data)

def call_urls(urls_list):
    """This method calls all the urls that have been generated.
    It then calls the processing method to begin processing the returned data"""
    # for url in urls_list:
    #     r = requests.get(url)
        # print(r.text)
    r = requests.get(urls_list[1], stream = True)
    get_header_general_and_data(r.text)


generated_urls_list = generate_urls()
call_urls(generated_urls_list)
