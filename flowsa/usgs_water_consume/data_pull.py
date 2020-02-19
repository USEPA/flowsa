# data_pull.py (usgs_water_consume)
# !/usr/bin/env python3
# coding=utf-8
import os
import io
import requests
import yaml
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
from data_reshape import parse_header
"""
Classes and methods for pulling data from a USGS web service.

Available functions:
generate_urls
get_header_general_and_data
call_urls
-
"""
def generate_urls(state_abre, year, usgs_url_p1, usgs_url_p2, usgs_url_p3):
    """This method generates the urls that will be called
    This includes all states and DC and over 2 years 2010 and 2015"""
    url_list = []
    for y in year:
        for s in state_abre:
            url = usgs_url_p1 + s + usgs_url_p2 + str(y) + usgs_url_p3
            url_list.append(url)
    return url_list


def get_header_general_and_data(text):
    """This method takes the file data line by line and seperates it into 3 catigories
        Metadata - these are the comments at the begining of the file. 
        Header data - the headers for the data.
        Data - the actuall data for each of the headers. 
        This method also eliminates the table metadata. The table metadata declares how long each string in the table can be."""
    flag = True
    header = "" 
    column_size = ""
    data =[]
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
    return (header, data)
    
def print_file(data_frames_list):
    result = pd.concat(data_frames_list)
    result.to_parquet('FlowByActivity.parquet')


def call_urls(urls_list, activity_array, surface_array, water_type_array, technosphere_flow_array, waste_flow_array ):
    """This method calls all the urls that have been generated.
    It then calls the processing method to begin processing the returned data"""
    request_list = []
    data_frames_list = []
    for url in urls_list:
        r = requests.get(urls_list[1])
        usgs_split = get_header_general_and_data(r.text)
        parse_header(usgs_split[0], usgs_split[1], activity_array, surface_array, water_type_array, technosphere_flow_array, waste_flow_array)
       # data_frames_list.append(df)

    print_file(data_frames_list)
        # print(r.text)
   # r = requests.get(urls_list[1], stream = True)

with open(r'C://Git//projects//epa//flowsa_testbed//usgs_water_consume//datasource_config.yaml') as file:
    documents = yaml.full_load(file)

    for item, doc in documents.items():
        if item == "state_abrevation":
            state_abre = doc
        elif item == "year":
            year = doc
        elif item == "usgs_url_part_1":
            usgs_url_p1 = doc
        elif item == "usgs_url_part_2":
            usgs_url_p2 = doc
        elif item == "usgs_url_part_3":
            usgs_url_p3 = doc
        elif item == "activity_array":
            activity_array = doc
        elif item == "surface_array":
            surface_array = doc
        elif item == "water_type_array":
            water_type_array = doc
        elif item == "technosphere_flow_array":
            technosphere_flow_array = doc
        elif item == "waste_flow_array":
            waste_flow_array = doc

generated_urls_list = generate_urls(state_abre, year, usgs_url_p1, usgs_url_p2, usgs_url_p3)
usgs_split = call_urls(generated_urls_list, activity_array, surface_array, water_type_array, technosphere_flow_array, waste_flow_array)

