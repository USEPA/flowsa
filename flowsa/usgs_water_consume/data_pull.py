# data_pull.py (usgs_water_consume)
# !/usr/bin/env python3
# coding=utf-8
import io
import requests
import yaml
import pandas as pd
from flowsa.usgs_water_consume.data_reshape import parse_header
from flowsa.common import modulepath, outputpath
"""
Classes and methods for pulling data from a USGS web service.

Available functions:
generate_urls - generates the URL for the USGS water use data. 
get_header_general_and_data - takes usgs data file and parces the Header, Meta Data and Data out of the file. 
print_file - Joins all the data frames 1 from each file and concatinates the data frames into 1 data frame. This method  then prints the parquet file
call_urls - calls the url and gets the usgs data file. 
                This method calls the get_header_general_and_data method and the parse_header method in the data_reshape class. 
-
"""
def generate_urls(state_abrevation, year_value, base_url, nwis, url_params1, url_params2):
    """This method generates the urls that will be called
    This includes all states and DC and over 2 years 2010 and 2015"""
    url_list = []
    for y in year_value:
        for s in state_abrevation:
            url = base_url + s + nwis  + url_params1 + str(y) + url_params2
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
    """Concatenates all the data frames from the data frames list. 
        The data frame is then printed into a parquet file."""
    result = pd.concat(data_frames_list)
    result.to_parquet(outputpath+'usgs_water_consume.parquet')


def call_urls(urls_list, activity_array, compartment_array, water_type_array, technosphere_flow_array, waste_flow_array):
    """This method calls all the urls that have been generated.
    It then calls the processing method to begin processing the returned data"""
    data_frames_list = []
    for url in urls_list:
        r = requests.get(url)
        usgs_split = get_header_general_and_data(r.text)
        df = parse_header(usgs_split[0], usgs_split[1], activity_array, compartment_array, water_type_array, technosphere_flow_array, waste_flow_array)
        data_frames_list.append(df)

    print_file(data_frames_list)

usgs_water_path = modulepath + 'usgs_water_consume/'
file = open(usgs_water_path+'datasource_config.yaml', 'r')
documents = yaml.full_load(file)
base_url = ""
state_abrevation = []
nwis = ""
url_params1 = ""
url_params2 = ""
year_value = []
for key, value in documents.items():
    if key == "usgs_url":
        usgs_url_value = value 
        base_url = usgs_url_value["base_url"]
        state_abrevation = usgs_url_value["state_abrevation"]
        nwis = usgs_url_value["nwis"]
        params =  usgs_url_value["params"]
        formats = params["format"]
        format_value = params["format_value"]
        compression = params["compression"]
        compression_value = params["compression_value"]
        area = params["area"]
        area_value = params["area_value"]
        year = params["year"]
        year_value = params["year_value"]
        county = params["county"]
        county_value = params["county_value"]
        category = params["category"]
        category_value = params["category_value"]
        url_params1 = "{0}{1}{2}{3}{4}{5}{6}".format(formats, format_value, compression, compression_value, area, area_value, year)
        url_params2 = "{0}{1}{2}{3}".format(county, county_value, category, category_value)

    elif key == "activity_array":
        activity_array = value
    elif key == "compartment_array":
        compartment_array = value
    elif key == "water_type_array":
        water_type_array = value
    elif key == "technosphere_flow_array":
        technosphere_flow_array = value
    elif key == "waste_flow_array":
        waste_flow_array = value

generated_urls_list = generate_urls(state_abrevation, year_value, base_url, nwis, url_params1, url_params2)
usgs_split = call_urls(generated_urls_list, activity_array, compartment_array, water_type_array, technosphere_flow_array, waste_flow_array)